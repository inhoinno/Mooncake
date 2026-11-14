#!/usr/bin/env python3
"""Host-local legacy Store serving experiment; never contacts a peer by SSH."""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import importlib.metadata
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import secrets
import signal
import socket
import stat
import subprocess
import sys
import sysconfig
import time
import urllib.request

from callosum_lab import process_stamp
from callosum_store_probe import crc64, dax_geometry, events, prepare_resources

ROOT = Path(__file__).resolve().parents[2]
SOURCE = Path(__file__).resolve().parents[1]
WORKER = Path("distributed/kv_transfer/kv_connector/v1/mooncake/store/worker.py")
SCHEDULER = Path("v1/core/sched/scheduler.py")
NATIVE_SOURCES = ("mooncake-store/src/callosum_store.cpp", "mooncake-store/src/client_service.cpp",
                  "mooncake-store/src/master_service.cpp", "mooncake-store/src/real_client.cpp",
                  "mooncake-store/src/transfer_task.cpp", "mooncake-integration/store/store_py.cpp")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, indent=2) + "\n"


def save(path, value):
    """Exclusive evidence creation; reruns retain an identical resolved input."""
    path = Path(path)
    text = encoded(value)
    if path.exists():
        require(path.read_text() == text, f"preserve existing different file: {path}")
        return
    with path.open("x") as stream:
        os.chmod(path, 0o600)
        stream.write(text)


def private_token(path):
    path = Path(path).resolve(strict=True)
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_mode & 0o077 == 0,
            "token must be a private regular file")
    require(len(path.read_text().strip()) >= 32, "token must have at least 32 characters")
    return path


def validate_spec(spec):
    require(spec.get("schema") == 1 and re.fullmatch(r"[A-Za-z0-9_-]{8,64}", spec.get("campaign") or ""),
            "spec needs schema=1 and a fresh campaign ID (8..64 safe characters)")
    nodes = spec.get("nodes", {})
    require(len(nodes) == 2 and {n.get("role") for n in nodes.values()} == {"source", "requester"},
            "exactly two explicit source/requester nodes are required")
    for name, node in nodes.items():
        for field in ("hostname", "ip", "pool_id", "physical_pool_id", "dax_path", "owner",
                      "hca", "cuda_devices", "authorization"):
            require(isinstance(node.get(field), str) and node[field], f"{name}.{field} is required")
        for field in ("capacity", "owned_size", "alignment", "api_port", "te_port"):
            require(type(node.get(field)) is int and node[field] > 0, f"{name}.{field} must be positive")
        for field in ("mapping_offset", "owned_offset"):
            require(type(node.get(field)) is int and node[field] >= 0, f"{name}.{field} must be nonnegative")
        require(node["owned_offset"] + node["owned_size"] <= node["capacity"], "owned arena exceeds mapping")
        require(node["alignment"] & (node["alignment"] - 1) == 0, "alignment must be a power of two")
        require(all(node[f] % node["alignment"] == 0 for f in
                    ("capacity", "mapping_offset", "owned_offset", "owned_size")), "unaligned pool geometry")
        require(node["dax_path"].startswith("/dev/dax"), "serving requires an actual devdax path")
        ipaddress.IPv4Address(node["ip"])
        ipaddress.IPv4Address(node.get("control_ip", node["ip"]))
        require(0 < node["api_port"] < 65536 and 0 < node["te_port"] < 65536, "invalid node port")
    a, b = nodes.values()
    require(a["hostname"] != b["hostname"] and a["ip"] != b["ip"] and
            a["pool_id"] != b["pool_id"] and a["physical_pool_id"] != b["physical_pool_id"],
            "workers and physical pools must be independent")
    model = spec.get("model", {})
    require(Path(model.get("path", "")).is_absolute() and model.get("revision") and
            model.get("tokenizer_revision"), "model path and pinned model/tokenizer revisions are required")
    require(model.get("dtype") in ("float16", "bfloat16") and model.get("kv_dtype") == "auto",
            "select one supported model/KV dtype contract")
    require(type(model.get("block_size")) is int and model["block_size"] > 0 and
            type(model.get("prefix_blocks")) is int and model["prefix_blocks"] >= 2 and
            type(model.get("token_id")) is int and model["token_id"] >= 0,
            "invalid deterministic block/token profile")
    require(model.get("max_model_len", 0) > model["block_size"] * model["prefix_blocks"] + 1,
            "context must hold the complete prompt and continuation")
    require(spec.get("master_node") in nodes, "master_node must identify an experiment worker")
    require(nodes[spec["master_node"]]["role"] == "source", "run the owned master on the source host")
    for field in ("master_port", "metrics_port"):
        require(type(spec.get(field)) is int and 0 < spec[field] < 65536, f"invalid {field}")
    for name, node in nodes.items():
        ports = [node["api_port"], node["te_port"]]
        if name == spec["master_node"]:
            ports += [spec["master_port"], spec["metrics_port"]]
        require(len(ports) == len(set(ports)), f"colliding service ports on {name}")
    for name, value in spec.get("rdma_environment", {}).items():
        require(name in ("MC_GID_INDEX", "MC_MTU", "MC_NUM_QP_PER_EP") and
                isinstance(value, str) and value.isdecimal(), "invalid resolved RDMA environment")
    return spec


def initialize(spec_path, node_id, token_path, run):
    spec = validate_spec(json.loads(Path(spec_path).read_text()))
    require(node_id in spec["nodes"], "unknown node ID")
    token = private_token(token_path)
    node = spec["nodes"][node_id]
    require(socket.gethostname() == node["hostname"], "this host does not match the selected node ID")
    run = Path(run).resolve()
    require(run.is_absolute(), "run directory must be absolute")
    # Collect all mandatory inputs before creating outputs.
    authorization_path = Path(node["authorization"]).resolve(strict=True)
    authorization = json.loads(authorization_path.read_text())
    for field in ("hostname", "pool_id", "physical_pool_id", "dax_path", "owner",
                  "mapping_offset", "capacity", "owned_offset", "owned_size", "alignment"):
        require(authorization.get(field) == node[field], f"authorization does not match {field}")
    require(authorization.get("exclusive") is True and authorization.get("physical_evidence") and
            authorization.get("challenge_evidence"),
            "exclusive ownership, physical topology and reserved-byte visibility evidence are required")
    for field in ("physical_evidence", "challenge_evidence"):
        evidence = Path(authorization[field])
        require(evidence.is_absolute() and evidence.is_file() and evidence.stat().st_size > 0,
                f"missing operator evidence: {field}")
    master = spec["nodes"][spec["master_node"]]
    native = {k: node[k] for k in ("pool_id", "capacity", "mapping_offset", "alignment", "owned_offset", "owned_size")}
    native.update(campaign=spec["campaign"], path=node["dax_path"], backend_kind="devdax",
                  token_file=str(token), journal=str(run / "arena.journal"), wait_ms=30000,
                  mux_capacity=2, mux_queue_limit=32)
    connector = dict(metadata_server="P2PHANDSHAKE",
                     master_server_address=f"{master.get('control_ip', master['ip'])}:{spec['master_port']}", protocol="rdma",
                     device_name=node["hca"], mode="standalone-store", global_segment_size=0,
                     local_buffer_size=64 * 1024 * 1024, enable_offload=False)
    setup = dict(schema=1, spec=spec, node_id=node_id, run=str(run), python=sys.executable,
                 native=str(run / "native.json"), connector=str(run / "connector.json"),
                 authorization_sha256=sha(authorization_path), token_sha256=sha(token),
                 input_sha256={"native": hashlib.sha256(encoded(native).encode()).hexdigest(),
                               "connector": hashlib.sha256(encoded(connector).encode()).hexdigest()},
                 physical_sha256=sha(authorization["physical_evidence"]),
                 challenge_sha256=sha(authorization["challenge_evidence"]),
                 source=str(SOURCE), root=str(ROOT))
    validate_environment(setup)
    run.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name, value in (("native.json", native), ("connector.json", connector), ("setup.json", setup)):
        save(run / name, value)
    return setup


def load(run):
    setup = json.loads((Path(run) / "setup.json").read_text())
    require(Path(run).resolve() == Path(setup["run"]) and setup["source"] == str(SOURCE),
            "setup receipt belongs to another run directory or checkout")
    require(setup["python"] == sys.executable, "use the interpreter recorded by init")
    node = setup["spec"]["nodes"][setup["node_id"]]
    require(socket.gethostname() == node["hostname"], "setup receipt belongs to another host")
    require(sha(node["authorization"]) == setup["authorization_sha256"], "authorization changed after init")
    for name in ("native", "connector"):
        require(sha(setup[name]) == setup["input_sha256"][name], f"resolved {name} config changed after init")
    native = json.loads(Path(setup["native"]).read_text())
    require(sha(private_token(native["token_file"])) == setup["token_sha256"], "token changed after init")
    require(native["campaign"] == setup["spec"]["campaign"], "native config campaign changed")
    return setup


def environment(setup, role):
    node = setup["spec"]["nodes"][setup["node_id"]]
    fixed = {"MC_STORE_RDMA_GPU_DIRECT": "0", "MOONCAKE_STORE_CHECKSUM": "1",
             "MC_STORE_LOCAL_HOT_CACHE_SIZE": "0", "MC_STORE_MEMCPY": "1",
             "MC_NUM_QP_PER_EP": "16", "GLOG_logtostderr": "1", "PYTHONHASHSEED": "0",
             "VLLM_TTFT_BREAKDOWN": "1", "VLLM_LOGGING_LEVEL": "INFO",
             "CUDA_VISIBLE_DEVICES": node["cuda_devices"],
             "MOONCAKE_REQUESTER_LOCAL_HOSTNAME": f"{node['ip']}:{node['te_port']}"}
    fixed.update(setup["spec"].get("rdma_environment", {}))
    if role == "worker":
        fixed.update(MC_CALLOSUM_STORE_CONFIG=setup["native"], MOONCAKE_CONFIG_PATH=setup["connector"])
    if role == "master":
        fixed["MC_CALLOSUM_RETAIN_COPY_TASKS"] = "1"
    forbidden = ("MC_CXL_", "MC_STORE_USE_HUGEPAGE", "MOONCAKE_PREFERRED_SEGMENT",
                 "MC_USE_TENT", "MC_USE_TEV1", "MC_LEGACY_RPC_PORT_BINDING", "MC_MS_AUTO_DISC",
                 "MOONCAKE_GPU_RDMA_", "MOONCAKE_RDMA_")
    for name, value in os.environ.items():
        if name.startswith(forbidden):
            raise RuntimeError(f"inherited {name} conflicts with the resolved serving mode")
        if name in fixed:
            require(value == fixed[name], f"inherited {name} conflicts with resolved configuration")
        elif name in ("MC_CALLOSUM_STORE_CONFIG", "MOONCAKE_CONFIG_PATH", "MC_CALLOSUM_RETAIN_COPY_TASKS",
                      "MC_GID_INDEX", "MC_MTU"):
            raise RuntimeError(f"{name} is not valid for the selected role")
    env = os.environ.copy()
    env.update(fixed)
    libraries = str(Path(sys.prefix) / "lib")
    env["LD_LIBRARY_PATH"] = libraries + (":" + env["LD_LIBRARY_PATH"] if env.get("LD_LIBRARY_PATH") else "")
    return env, fixed


def validate_environment(setup):
    environment(setup, "worker")


def endpoint(node):
    return f"{node['ip']}:{node['te_port']}"


def api_url(node):
    return f"http://{node.get('control_ip', node['ip'])}:{node['api_port']}"


def preflight_pair(setup, peer_path):
    local = json.loads((Path(setup["run"]) / "preflight.json").read_text())
    peer = json.loads(Path(peer_path).read_text())
    expected = (set(setup["spec"]["nodes"]) - {setup["node_id"]}).pop()
    require(peer.get("node_id") == expected and
            peer.get("hostname") == setup["spec"]["nodes"][expected]["hostname"],
            "peer preflight belongs to a different node")
    for field in ("spec_sha256", "token_sha256", "model_manifest", "connector_sha256",
                  "scheduler_sha256", "vllm_version", "native_abi", "native_mux_abi", "source_fingerprint"):
        require(peer.get(field) is not None and local[field] == peer[field],
                f"peer preflight differs in {field}")
    return local, peer


def vllm_root():
    module = importlib.util.find_spec("vllm")
    require(module is not None and module.submodule_search_locations, "vLLM is not installed")
    return Path(next(iter(module.submodule_search_locations)))


def instrument(root, run):
    """Add evidence only to the audited legacy connector/scheduler seams."""
    changes = []
    export_anchor = "                failed = [i for i, v in enumerate(res) if v < 0]\n"
    export = '''                # Callosum serving evidence v1 (ordinary save, not eviction).
                if ttft_trace_enabled():
                    # Keep diagnostic lookup failure out of the save result.
                    try:
                        placement = self.store.batch_get_replica_desc(keys)
                    except Exception:
                        placement = {}
                    for key, size, start, end, group, result in zip(
                        keys, sizes, starts, ends, group_indices, res, strict=True
                    ):
                        replicas = [
                            r.get_memory_descriptor().buffer_descriptor
                            for r in placement.get(key, ()) if r.is_memory_replica()
                        ]
                        emit_ttft_event(
                            "callosum_connector_export", request_id=req_id,
                            object_key=key, num_bytes=sum(size), start_token=start,
                            end_token=end, group_index=group, tp_rank=self.tp_rank,
                            tensor_bytes=size,
                            replica_endpoints=[r.transport_endpoint for r in replicas],
                            status="ok" if result >= 0 else "error",
                        )
'''
    compute_anchor = "            request.num_computed_tokens += num_scheduled_token\n"
    compute = '''            # Callosum serving evidence v1: actual scheduled compute ranges.
            emit_ttft_event(
                "callosum_compute", request_id=req_id,
                computed_tokens_before=request.num_computed_tokens,
                scheduled_tokens=num_scheduled_token,
                prompt_tokens=request.num_prompt_tokens,
            )
'''
    chunk_anchor = "                            result_code=result,\n"
    chunk = "                            tensor_bytes=sizes, tp_rank=self.tp_rank,\n"
    for relative, edits in (
        (WORKER, [('"callosum_connector_export"', export_anchor, export),
                  ("tensor_bytes=sizes, tp_rank=self.tp_rank", chunk_anchor, chunk)]),
        (SCHEDULER, [('"callosum_compute"', compute_anchor, compute)]),
    ):
        path = Path(root) / relative
        original = path.read_text()
        require("emit_ttft_event" in original, f"installed source lacks required trace API: {relative}")
        updated = original
        for marker, anchor, addition in edits:
            if marker in updated:
                require(addition in updated, "existing Callosum trace instrumentation differs")
                continue
            require(updated.count(anchor) == 1, f"installed source does not match audited seam: {relative}")
            updated = updated.replace(anchor, addition + anchor)
        ast.parse(updated)
        if updated != original:
            changes.append((path, original, updated))
    run = Path(run)
    for path, original, updated in changes:
        backup = run / (path.name + ".before")
        require(not backup.exists(), f"preserve existing instrumentation backup: {backup}")
    for path, original, updated in changes:
        (run / (path.name + ".before")).write_text(original)
        path.write_text(updated)
    return {str(Path(root) / p): sha(Path(root) / p) for p in (WORKER, SCHEDULER)}


def command(setup, role):
    spec = setup["spec"]
    node = spec["nodes"][setup["node_id"]]
    if role == "master":
        require(setup["node_id"] == spec["master_node"], "master role belongs to another node")
        binary = Path(setup["run"]) / "build-store-cuda/mooncake-store/src/mooncake_master"
        require(binary.is_file(), "build the Store master before startup")
        return [str(binary), f"--rpc_port={spec['master_port']}",
                f"--rpc_address={node.get('control_ip', node['ip'])}",
                f"--metrics_port={spec['metrics_port']}", "--enable_metric_reporting=false",
                "--memory_allocator=offset"]
    model = spec["model"]
    result = [sys.executable, "-m", "vllm.entrypoints.openai.api_server", "--model", model["path"],
              "--tokenizer", model["path"], "--served-model-name", "callosum-test",
              "--dtype", model["dtype"], "--kv-cache-dtype", model["kv_dtype"],
              "--block-size", str(model["block_size"]), "--max-model-len", str(model["max_model_len"]),
              "--tensor-parallel-size", "1", "--pipeline-parallel-size", "1",
              "--no-enable-prefix-caching", "--prefix-caching-hash-algo", "sha256",
              "--enable-request-id-headers", "--host", node.get("control_ip", node["ip"]),
              "--port", str(node["api_port"])]
    if role == "worker":
        result += ["--kv-transfer-config", json.dumps(dict(kv_connector="MooncakeStoreConnector",
                       kv_role="kv_producer" if node["role"] == "source" else "kv_consumer",
                       kv_connector_extra_config={"load_async": True}))]
    return result


def http(url, body=None, request_id=None, timeout=120):
    headers = {"Content-Type": "application/json"}
    if request_id:
        headers["X-Request-ID"] = request_id
    request = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def journals(run):
    path = Path(run) / "arena.journal"
    if not path.exists():
        return {}
    content = path.read_text()
    require(not content or content.endswith("\n"), "truncated arena journal; retain all resources")
    lines = content.splitlines()
    require(lines and lines[0].startswith("IDENTITY "), "invalid arena journal identity")
    states = {}
    for line in lines[1:]:
        identity, state = line.split()
        require(state in {"PENDING", "DRAINED", "NO_POST", "VALIDATED"}, "invalid arena journal state")
        states[identity] = state
    return states


def start(setup, role, peer_path=None, prepare=False):
    run = Path(setup["run"])
    receipt = run / f"{role}.process.json"
    require(not receipt.exists(), f"inspect retained process receipt before replacement: {receipt}")
    if role == "cold":
        require((run / "worker.process.retired.json").is_file(), "retire the owned requester worker before cold control")
    require(peer_path is not None, "start requires both host preflights via --peer-preflight")
    local, _ = preflight_pair(setup, peer_path)
    require(sha(local["binding"]) == local["binding_sha256"] and
            sha(local["engine"]) == local["engine_sha256"] and
            sha(local["connector"]) == local["connector_sha256"] and
            sha(local["scheduler"]) == local["scheduler_sha256"],
            "installed artifacts changed since preflight")
    env, effective = environment(setup, role)
    argv = command(setup, role)
    if role == "master":
        build_receipt = json.loads((run / "build.json").read_text())
        require(str(Path(argv[0]).resolve()) == build_receipt["master"] and
                sha(argv[0]) == build_receipt["master_sha256"], "master artifact changed after build")
    node = setup["spec"]["nodes"][setup["node_id"]]
    bind_ip = node.get("control_ip", node["ip"])
    port = setup["spec"]["master_port"] if role == "master" else node["api_port"]
    with socket.socket() as probe:
        probe.settimeout(1)
        require(probe.connect_ex((bind_ip, port)) != 0, "selected service port is already occupied")
    if role == "worker":
        with socket.socket() as probe:
            probe.settimeout(1)
            require(probe.connect_ex((node["ip"], node["te_port"])) != 0,
                    "selected worker RDMA handshake port is already occupied")
    log = run / f"{role}.log"
    require(not log.exists(), "preserve existing service log; select a fresh campaign after controlled retirement")
    if role == "worker":
        require(not (run / "arena.journal").exists(), "retain the previous native arena journal; do not remount")
        master = setup["spec"]["nodes"][setup["spec"]["master_node"]]
        with socket.create_connection((master.get("control_ip", master["ip"]), setup["spec"]["master_port"]), timeout=2):
            pass
        require(not (run / "resources.json").exists() or
                json.loads((run / "resources.json").read_text()).get("status") != "PASS",
                "existing successful resource receipt requires inspection before remount")
        prepare_resources(json.loads(Path(setup["native"]).read_text()), 64 * 1024 * 1024,
                          run, authorize=prepare)
    with log.open("xb") as output:
        child = subprocess.Popen(argv, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    stamp = process_stamp(child.pid)
    require(stamp is not None, f"child start identity unavailable; inspect {log}")
    save(receipt, dict(pid=child.pid, stamp=stamp, argv=argv,
                       role=role, campaign=setup["spec"]["campaign"], environment=effective))
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        require(child.poll() is None, f"{role} exited; inspect {log}; no other service was stopped")
        try:
            if role == "master":
                with socket.create_connection((bind_ip, port), timeout=1):
                    return str(receipt)
            elif http(f"http://{bind_ip}:{port}/v1/models", timeout=2).get("data"):
                if role == "worker":
                    mounted = [r for r in events(log) if r.get("event") == "pool_mounted"]
                    require(len(mounted) == 1 and mounted[0].get("segment") == endpoint(node) and
                            mounted[0].get("pool") == node["pool_id"] and mounted[0].get("backend") == "devdax",
                            "live Store arena does not match the approved DAX endpoint")
                return str(receipt)
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError(f"readiness timed out; retain {receipt} and {log}; no process was killed")


def stop(setup, role, peer_retired=None):
    run = Path(setup["run"])
    path = run / f"{role}.process.json"
    receipt = json.loads(path.read_text())
    require(receipt["campaign"] == setup["spec"]["campaign"], "process belongs to another campaign")
    require(all(state != "PENDING" for state in journals(run).values()),
            "pending DMA authorization prevents retirement; preserve journals and source pins")
    require(role == "cold" or not (run / "request.inflight.json").exists(),
            "request outcome is not recorded; inspect the retained service and journal")
    if role == "master":
        require((run / "worker.process.retired.json").is_file() and peer_retired is not None,
                "retire both owned workers before the master")
        peer = json.loads(Path(peer_retired).read_text())
        requester = next(n for n in setup["spec"]["nodes"].values() if n["role"] == "requester")
        require(peer.get("campaign") == setup["spec"]["campaign"] and peer.get("role") == "worker" and
                peer.get("node_id") in setup["spec"]["nodes"] and
                setup["spec"]["nodes"][peer["node_id"]] == requester and peer.get("retired") is True,
                "invalid requester retirement receipt")
    pid = receipt["pid"]
    require(process_stamp(pid) == receipt["stamp"], "owned process identity changed; no process was stopped")
    argv = subprocess.check_output(["ps", "-p", str(pid), "-o", "args="], text=True).strip()
    require(all(part in argv for part in receipt["argv"][:3]), "owned process argv changed")
    # pidfd binds the signal to the inspected process incarnation on Linux.
    require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"), "Linux pidfd is required for safe retirement")
    descriptor = os.pidfd_open(pid)
    try:
        require(process_stamp(pid) == receipt["stamp"], "process identity changed during retirement")
        signal.pidfd_send_signal(descriptor, signal.SIGTERM)
    finally:
        os.close(descriptor)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if process_stamp(pid) != receipt["stamp"]:
            retired = path.with_suffix(".retired.json")
            save(retired, dict(receipt, node_id=setup["node_id"], retired=True))
            path.unlink()
            return str(retired)
        time.sleep(0.2)
    raise RuntimeError("owned process has not exited; do not force kill or recycle its arena")


def preflight(setup):
    require(platform.system() == "Linux", "Linux/RDMA/CUDA preflight is unavailable on this development Mac")
    require(sys.prefix != sys.base_prefix, "use a target-compatible venv, not system Python")
    node = setup["spec"]["nodes"][setup["node_id"]]
    run = Path(setup["run"])
    build_receipt = json.loads((run / "build.json").read_text())
    env, effective = environment(setup, "worker")
    _, device_size, alignment = dax_geometry(Path(node["dax_path"]))
    require(node["mapping_offset"] + node["capacity"] <= device_size and
            node["alignment"] % alignment == 0, "approved mapping exceeds live DAX geometry")
    require(Path("/sys/class/infiniband", node["hca"]).exists(), "configured RDMA device is unavailable")
    probe = subprocess.run([sys.executable, "-c", '''
import json, torch
from mooncake import engine, store
print(json.dumps(dict(binding=store.__file__, engine=engine.__file__,
    native_abi=getattr(store, "CALLOSUM_STORE_ABI", 0),
    native_mux_abi=getattr(store, "CALLOSUM_STORE_MUX_ABI", 0),
    cuda=torch.cuda.is_available(), gpu_count=torch.cuda.device_count())))
'''], env=env, check=True, capture_output=True, text=True)
    native = json.loads(probe.stdout.strip().splitlines()[-1])
    require(native["native_abi"] == native["native_mux_abi"] == 1 and
            native["cuda"] and native["gpu_count"] == 1,
            "require rebuilt Store/mux bindings and one available CUDA GPU per worker")
    for name in ("binding", "engine"):
        require(str(Path(native[name]).resolve()) == build_receipt[name] and
                sha(native[name]) == build_receipt[name + "_sha256"],
                f"imported {name} differs from the build receipt")
    root = vllm_root()
    for relative, marker in ((WORKER, '"callosum_connector_export"'), (SCHEDULER, '"callosum_compute"')):
        require(marker in (root / relative).read_text(), "install the bounded trace instrumentation before startup")
    model_path = Path(setup["spec"]["model"]["path"]).resolve(strict=True)
    config = json.loads((model_path / "config.json").read_text())
    require(config.get("model_type") in ("llama", "qwen2", "qwen3") and not config.get("sliding_window") and
            set(config.get("layer_types") or ["full_attention"]) == {"full_attention"},
            "initial serving gate requires an available full-attention Llama/Qwen model")
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(str(model_path), local_files_only=True)
    require(setup["spec"]["model"]["token_id"] < len(tokenizer), "prompt token ID is outside the installed vocabulary")
    files = sorted(p for p in model_path.iterdir() if p.is_file() and
                   (p.suffix == ".safetensors" or p.suffix == ".json" or p.name.startswith("tokenizer")))
    require(any(p.suffix == ".safetensors" for p in files), "local model weights are missing")
    manifest = {p.name: sha(p) for p in files}
    report = dict(node_id=setup["node_id"], hostname=socket.gethostname(), environment=effective,
                  spec_sha256=hashlib.sha256(encoded(setup["spec"]).encode()).hexdigest(),
                  model_manifest=manifest, binding=str(Path(native["binding"]).resolve()),
                  binding_sha256=sha(native["binding"]), engine=str(Path(native["engine"]).resolve()),
                  engine_sha256=sha(native["engine"]), token_sha256=setup["token_sha256"],
                  num_layers=config["num_hidden_layers"],
                  connector=str(root / WORKER), connector_sha256=sha(root / WORKER),
                  scheduler=str(root / SCHEDULER), scheduler_sha256=sha(root / SCHEDULER),
                  vllm_version=importlib.metadata.version("vllm"), native_abi=native["native_abi"],
                  native_mux_abi=native["native_mux_abi"], source_fingerprint=build_receipt["source_fingerprint"],
                  authorization_sha256=setup["authorization_sha256"], hardware="UNVERIFIED")
    if not (run / "preflight.json").exists():
        subprocess.run(["bash", str(ROOT / "scripts/capture_preflight.sh"), "--output",
                        str(run / "host-preflight"), "--checkout", str(SOURCE),
                        "--python", sys.executable, "--dax", node["dax_path"]], env=env, check=True)
    save(run / "preflight.json", report)
    return report


def request(setup, phase, source_run=None):
    node = setup["spec"]["nodes"][setup["node_id"]]
    require((phase == "produce" and node["role"] == "source") or
            (phase in ("reuse", "local", "cold") and node["role"] == "requester"), "request phase does not match node role")
    role = "cold" if phase == "cold" else "worker"
    require((Path(setup["run"]) / f"{role}.process.json").exists(), "start the owned ready service first")
    model = setup["spec"]["model"]
    prompt = [model["token_id"]] * (model["block_size"] * model["prefix_blocks"] + 1)
    identity = setup["spec"]["campaign"] + "-" + phase
    run = Path(setup["run"])
    path = run / f"{phase}.response.json"
    if path.exists():
        return json.loads(path.read_text())
    if phase == "reuse":
        require(source_run is not None, "reuse requires B's export evidence via --source-evidence")
        source_run = Path(source_run)
        _, peer = preflight_pair(setup, source_run / "preflight.json")
        identity = setup["spec"]["campaign"] + "-produce"
        receipt = json.loads((source_run / "produce.response.json").read_text())
        require(receipt["request_id"] == identity, "source response belongs to another campaign")
        prefix = model["block_size"] * model["prefix_blocks"]
        exported = objects(trace_rows(source_run / "worker.log", identity),
                           "callosum_connector_export", prefix, model["block_size"])
        source = setup["spec"]["nodes"][peer["node_id"]]
        require(all(row["replica_endpoints"] == [endpoint(source)] for row in exported),
                "B must have exported every claimed full block to its own pool")
    if phase == "local":
        require((run / "reuse.response.json").is_file(), "run the remote-reuse request first")
    inflight = run / "request.inflight.json"
    require(not inflight.exists(), "a request already started; inspect the logs before any retry")
    with inflight.open("x") as stream:
        stream.write(encoded(dict(phase=phase, request_id=identity)))
    started = time.monotonic_ns()
    response = http(api_url(node) + "/v1/completions",
                    dict(model="callosum-test", prompt=prompt, max_tokens=1, temperature=0,
                         stream=False, seed=0, cache_salt=setup["spec"]["campaign"]), identity,
                    timeout=300)
    require(response.get("usage", {}).get("prompt_tokens") == len(prompt) and response.get("choices"),
            "inference response is incomplete or prompt accounting differs")
    require(response.get("id") == "cmpl-" + identity and response["usage"].get("completion_tokens") == 1,
            "completion ID or generated token count differs from the requested experiment")
    report = dict(phase=phase, request_id=identity, prompt_tokens=len(prompt), response=response,
                  elapsed_ns=time.monotonic_ns() - started, status="INFERENCE_ONLY")
    save(path, report)
    inflight.rename(run / f"{phase}.request.json")
    return report


def trace_rows(path, identity):
    prefix = "cmpl-" + identity + "-0"
    rows = []
    for line in Path(path).read_text().splitlines():
        if "[TTFT_TRACE] " not in line:
            continue
        event = json.loads(line.split("[TTFT_TRACE] ", 1)[1])
        rid = event.get("request_id", "")
        if rid == prefix or re.fullmatch(re.escape(prefix) + r"-[0-9a-f]{8}", rid):
            rows.append(event)
    require(rows and len({row["request_id"] for row in rows}) == 1,
            f"missing or ambiguous engine request ID for {identity}")
    return rows


def one(rows, name, **fields):
    selected = [r for r in rows if r.get("event") == name and
                all(r.get(key) == value for key, value in fields.items())]
    require(len(selected) == 1, f"expected one {name} event for {fields}")
    return selected[0]


def objects(rows, name, prefix_tokens, block_size):
    selected = sorted((r for r in rows if r.get("event") == name), key=lambda r: r["start_token"])
    require(len(selected) == prefix_tokens // block_size, f"wrong full-block count for {name}")
    require(len({r["object_key"] for r in selected}) == len(selected), f"duplicate connector key for {name}")
    for start, row in zip(range(0, prefix_tokens, block_size), selected, strict=True):
        require(row["start_token"] == start and row["end_token"] == start + block_size and
                row["status"] == "ok" and row["tp_rank"] == row["group_index"] == 0 and
                row["tensor_bytes"] and all(type(n) is int and n > 0 for n in row["tensor_bytes"]) and
                sum(row["tensor_bytes"]) == row["bytes"], f"bad layout, status or token coverage for {name}")
    return selected


def verify_compute(rows, start, end):
    cursor = start
    for r in sorted((r for r in rows if r.get("event") == "callosum_compute"), key=lambda r: r["timestamp_ns"]):
        require(r["computed_tokens_before"] == cursor and r["scheduled_tokens"] > 0,
                "actual compute overlaps the claimed reused prefix or has a gap")
        cursor += r["scheduled_tokens"]
    require(cursor == end and end > start, "missing actual prefill compute evidence")


def load_window(rows, prefix, prompt_tokens):
    wait = one(rows, "external_kv_wait_start")
    require(wait.get("external_cached_tokens") == prefix and wait.get("local_cached_tokens") == 0,
            "scheduler did not reserve the expected external-only prefix")
    finished = one(rows, "external_kv_request_complete")
    require(finished.get("status") == "ok" and finished.get("token_count") == prefix,
            "connector reported a load failure or incomplete prefix")
    ready = one(rows, "external_kv_completion_observed")
    computed = [r for r in rows if r.get("event") == "callosum_compute"]
    require(computed and wait["timestamp_ns"] <= finished["timestamp_ns"] <= ready["timestamp_ns"] <=
            min(r["timestamp_ns"] for r in computed), "GPU load/readiness did not precede compute")
    verify_compute(rows, prefix, prompt_tokens)
    return ready["timestamp_ns"] - wait["timestamp_ns"]


def verify_transfer(tx, rx, key_crc, size, source, requester):
    match = dict(key_crc=key_crc)
    auth = one(tx, "receiver_authorized", **match)
    attempt = auth["attempt"]
    gens = {k: auth[k] for k in ("source_generation", "target_generation")}
    require(auth["source_pool"] == source["pool_id"] and auth["destination_pool"] == requester["pool_id"] and
            auth["receiver"] == endpoint(requester) and auth["receiver_epoch"] and int(auth["bytes"]) == size,
            "wrong receiver authorization or object length")
    for label, node in (("source", source), ("destination", requester)):
        offset = int(auth[label + "_offset"])
        require(node["owned_offset"] <= offset <= node["owned_offset"] + node["owned_size"] - size,
                "payload range exceeds approved arena")
    queued, admitted, gathered = (one(tx, f"mux_{stage}", attempt=attempt)
                                  for stage in ("queued", "admitted", "gathered"))
    require(all(int(r["bytes"]) == size for r in (queued, admitted, gathered)), "mux byte count mismatch")
    writes = [r for r in tx if r.get("event") == "write_drained" and r.get("attempt") == attempt]
    end = 0
    for row in writes:
        require(row["protocol"] == "rdma" and row["opcode"] == "WRITE" and
                row["sender"] == endpoint(source) and 1 <= int(row["batch"]) <= 32 and
                all(row[k] == v for k, v in gens.items()), "wrong native opcode, endpoint, batch or generation")
        count = int(row["batch_bytes"])
        require(count > 0 and int(row["completed_end"]) == end + count, "gap in WRITE byte coverage")
        end += count
    require(writes and end == size, "incomplete native WRITE coverage")
    validated = one(rx, "receiver_validated", attempt=attempt)
    require(validated["content_ok"] == "1" and int(validated["bytes"]) == size and
            validated["epoch"] == auth["receiver_epoch"], "receiver content or epoch check failed")
    copied = one(tx, "copy_verified", attempt=attempt, **match)
    published = one(tx, "replica_published", **match, **gens)
    require(int(copied["bytes"]) == size and tx.index(auth) < tx.index(queued) < tx.index(admitted) <
            tx.index(writes[0]) <= tx.index(writes[-1]) < tx.index(gathered) < tx.index(copied) < tx.index(published),
            "publication did not follow native drain, gather and checksum")
    missed = one(rx, "initial_local_miss", **match)
    requested = one(rx, "remote_materialization", **match)
    dispatch = one(tx, "store_task_dispatch", **match)
    require(missed["pool"] == requester["pool_id"] and requested["task"] == dispatch["task"] and
            tx.index(dispatch) < tx.index(auth), "requester miss did not trigger the source Store task")
    remote = one(rx, "replica_usable", phase="remote", **match)
    local = one(rx, "replica_usable", phase="local", **match)
    gpu = [(i, r) for i, r in enumerate(rx) if r.get("event") == "gpu_load_verified" and r.get("key_crc") == key_crc]
    require(len(gpu) == 2 and all(int(r["bytes"]) == size and r["path"] == "local_cxl_copy" and
            r["replica_generation"] == gens["target_generation"] for _, r in gpu) and
            rx.index(missed) < rx.index(requested) < rx.index(validated) < rx.index(remote) < gpu[0][0] <
            rx.index(local) < gpu[1][0], "missing ordered GPU copy/checksum or local reuse evidence")
    return dict(attempt=attempt, task=requested["task"], bytes=size, native_batches=len(writes), **gens)


def verify(setup, source_run):
    run = Path(setup["run"])
    source_run = Path(source_run)
    requester = setup["spec"]["nodes"][setup["node_id"]]
    require(requester["role"] == "requester", "verify on requester A or with its local receipt")
    source = next(n for n in setup["spec"]["nodes"].values() if n["role"] == "source")
    preflight_pair(setup, source_run / "preflight.json")
    require((source_run / "host-preflight/summary.json").is_file() and
            (run / "host-preflight/summary.json").is_file(), "retain both host diagnostic bundles")
    spec = setup["spec"]
    model = spec["model"]
    prefix = model["block_size"] * model["prefix_blocks"]
    prompt = prefix + 1
    evidence = {}
    for phase in ("produce", "reuse", "local", "cold"):
        directory = source_run if phase == "produce" else run
        identity = spec["campaign"] + "-" + phase
        response = json.loads((directory / f"{phase}.response.json").read_text())
        require(response["phase"] == phase and response["request_id"] == identity and
                response["response"]["id"] == "cmpl-" + identity and
                response["response"]["usage"]["prompt_tokens"] == prompt and
                response["response"]["usage"]["completion_tokens"] == 1 and response["response"]["choices"],
                f"invalid inference response for {phase}")
        evidence[phase] = (response, trace_rows(directory / ("cold.log" if phase == "cold" else "worker.log"), identity))
    exported = objects(evidence["produce"][1], "callosum_connector_export", prefix, model["block_size"])
    ready = {}
    for phase in ("reuse", "local"):
        rows = evidence[phase][1]
        loaded = objects(rows, "external_kv_chunk_complete", prefix, model["block_size"])
        for saved, received in zip(exported, loaded, strict=True):
            require(all(saved[k] == received[k] for k in ("object_key", "tensor_bytes", "bytes")) and
                    received["result_code"] == received["bytes"], "legacy GPU load does not match B's exported block")
        ready[phase] = load_window(rows, prefix, prompt)
    verify_compute(evidence["cold"][1], 0, prompt)
    require(not any(r["event"].startswith("external_kv_") for r in evidence["cold"][1]),
            "cold control had external reuse enabled")
    verify_compute(evidence["produce"][1], 0, prompt)
    tx, rx = events(source_run / "worker.log"), events(run / "worker.log")
    transfers = []
    for row in exported:
        require(row["replica_endpoints"] == [endpoint(source)], "initial complete placement was not source-only")
        transfers.append(verify_transfer(tx, rx, str(crc64(row["object_key"].encode())), row["bytes"], source, requester))
    require(len({r["attempt"] for r in transfers}) == len(transfers), "transfer attempt reused across objects")
    report = dict(status="SERVING_REUSE_PROVEN", campaign=spec["campaign"], export_origin="ordinary_prefill_save",
                  source=endpoint(source), requester=endpoint(requester), scope="CXL_FIRST_LEGACY_STORE",
                  reused_blocks=len(transfers), reused_prefix_tokens=prefix, reused_compute_tokens=1,
                  local_compute_tokens=1, cold_compute_tokens=prompt, transfer_bytes=sum(r["bytes"] for r in transfers),
                  requester_gpu_ready_ns=ready, requests={phase: row[0]["elapsed_ns"] for phase, row in evidence.items()},
                  output_text_matches_cold=evidence["reuse"][0]["response"]["choices"][0]["text"] ==
                    evidence["cold"][0]["response"]["choices"][0]["text"], transfers=transfers)
    save(run / "serving-verification.json", report)
    return report


def build(setup):
    require(platform.system() == "Linux" and sys.prefix != sys.base_prefix,
            "production Store builds require Linux and the selected target-compatible venv")
    env, _ = environment(setup, "build")
    run = Path(setup["run"])
    require(not any(run.glob("*.process.json")), "stop owned services before rebuilding")
    directory = run / "build-store-cuda"
    source_fingerprint = {name: sha(SOURCE / name) for name in NATIVE_SOURCES}
    git = lambda directory, *args: subprocess.check_output(["git", "-C", str(directory), *args], text=True).strip()
    revisions = dict(parent=git(ROOT, "rev-parse", "HEAD"), mooncake=git(SOURCE, "rev-parse", "HEAD"),
                     dirty=git(SOURCE, "status", "--porcelain"))
    save(run / "build.source.json", dict(files=source_fingerprint, revisions=revisions))
    if (run / "build.json").exists():
        recorded = json.loads((run / "build.json").read_text())
        for name in ("binding", "engine", "master"):
            require(sha(recorded[name]) == recorded[name + "_sha256"], f"retained {name} changed after build")
        return recorded
    argv = ["cmake", "-S", str(SOURCE), "-B", str(directory), f"-DPython3_EXECUTABLE={sys.executable}",
            f"-DCMAKE_INSTALL_PREFIX={sys.prefix}", "-DUSE_CUDA=ON", "-DUSE_CXL=OFF", "-DUSE_TCP=OFF",
            "-DWITH_TE=ON", "-DWITH_STORE=ON", "-DWITH_STORE_RUST=OFF", "-DWITH_EP=OFF",
            "-DBUILD_EXAMPLES=OFF", "-DBUILD_BENCHMARK=OFF", "-DBUILD_UNIT_TESTS=ON",
            "-DUSE_TENT=OFF", "-DENABLE_MULTI_PROTOCOL=OFF"]
    commands = [argv, ["cmake", "--build", str(directory), "--parallel", "2", "--target", "store", "engine", "mooncake_master", "mooncake_client", "master_service_test"],
                [str(directory / "mooncake-store/tests/master_service_test"), "--gtest_filter=MasterServiceTest.CallosumLookupAuthorizePublishRequery"],
                ["cmake", "--install", str(directory)]]
    save(run / "build.commands.json", commands)
    for argv in commands:
        subprocess.run(argv, env=env, check=True)
    package = Path(sysconfig.get_path("platlib")) / "mooncake"
    extension = sysconfig.get_config_var("EXT_SUFFIX")
    receipt = dict(build=str(directory), interpreter=sys.executable, source_fingerprint=source_fingerprint,
                   revisions=revisions, cmake_cache_sha256=sha(directory / "CMakeCache.txt"))
    for name, path in (("binding", package / ("store" + extension)),
                       ("engine", package / ("engine" + extension)),
                       ("master", directory / "mooncake-store/src/mooncake_master")):
        receipt[name] = str(path.resolve())
        receipt[name + "_sha256"] = sha(path)
    save(run / "build.json", receipt)
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("token", "init", "build", "instrument", "preflight", "start", "request", "verify", "stop"))
    parser.add_argument("--run", type=Path)
    parser.add_argument("--spec", type=Path)
    parser.add_argument("--node")
    parser.add_argument("--token", type=Path)
    parser.add_argument("--role", choices=("master", "worker", "cold"))
    parser.add_argument("--phase", choices=("produce", "reuse", "local", "cold"))
    parser.add_argument("--peer-preflight", type=Path)
    parser.add_argument("--peer-retired", type=Path)
    parser.add_argument("--source-evidence", type=Path)
    parser.add_argument("--prepare-resources", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.stage == "token":
            require(args.token is not None, "token stage requires --token")
            fd = os.open(args.token, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(secrets.token_hex(32) + "\n")
            result = dict(token_file=str(args.token.resolve()))
        else:
            require(args.run is not None, "stage requires --run")
            if args.stage == "init":
                require(args.spec is not None and args.node and args.token is not None,
                        "init requires --spec, --node and --token")
                result = initialize(args.spec, args.node, args.token, args.run)
            else:
                setup = load(args.run)
                if args.stage == "build": result = build(setup)
                elif args.stage == "instrument": result = instrument(vllm_root(), args.run)
                elif args.stage == "preflight": result = preflight(setup)
                elif args.stage in ("start", "stop"):
                    require(args.role, "start/stop requires --role")
                    if args.stage == "start":
                        result = start(setup, args.role, args.peer_preflight, args.prepare_resources)
                    else:
                        result = stop(setup, args.role, args.peer_retired)
                elif args.stage == "verify":
                    require(args.source_evidence, "verify requires --source-evidence")
                    result = verify(setup, args.source_evidence)
                else:
                    require(args.phase, "request requires --phase")
                    result = request(setup, args.phase, args.source_evidence)
        status = "SERVING_REUSE_PROVEN" if args.stage == "verify" else "PREPARATION_OR_INFERENCE_ONLY"
        print(encoded(dict(status=status, result=result)), end="")
        return 0
    except Exception as error:
        status = "REUSE_NOT_PROVEN" if args.stage == "verify" else "BLOCKED"
        print(encoded(dict(status=status, stage=args.stage, cause=str(error),
                           recovery="inspect the stage receipts and logs; preserve live services, journals and prior evidence")), end="")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
