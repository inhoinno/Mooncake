#!/usr/bin/env python3
"""Host-local legacy Store serving experiment; never contacts a peer by SSH."""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import secrets
import signal
import socket
import stat
import subprocess
import sys
import time
import urllib.request

from callosum_lab import process_stamp

ROOT = Path(__file__).resolve().parents[2]
SOURCE = Path(__file__).resolve().parents[1]
WORKER = Path("distributed/kv_transfer/kv_connector/v1/mooncake/store/worker.py")
SCHEDULER = Path("v1/core/sched/scheduler.py")


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
    require(spec.get("schema") == 1 and spec.get("campaign"), "spec needs schema=1 and a campaign ID")
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
    for field in ("master_port", "metadata_port", "metrics_port"):
        require(type(spec.get(field)) is int and 0 < spec[field] < 65536, f"invalid {field}")
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
    require(authorization.get("exclusive") is True and authorization.get("physical_evidence"),
            "exclusive ownership and physical topology evidence are required")
    run.mkdir(mode=0o700, parents=True, exist_ok=True)
    master = spec["nodes"][spec["master_node"]]
    native = {k: node[k] for k in ("pool_id", "capacity", "mapping_offset", "alignment", "owned_offset", "owned_size")}
    native.update(campaign=spec["campaign"], path=node["dax_path"], backend_kind="devdax",
                  token_file=str(token), journal=str(run / "arena.journal"), wait_ms=30000)
    connector = dict(metadata_server=f"http://{master['ip']}:{spec['metadata_port']}/metadata",
                     master_server_address=f"{master['ip']}:{spec['master_port']}", protocol="rdma",
                     device_name=node["hca"], mode="standalone-store", global_segment_size=0,
                     local_buffer_size=64 * 1024 * 1024, enable_offload=False)
    setup = dict(schema=1, spec=spec, node_id=node_id, run=str(run), python=sys.executable,
                 native=str(run / "native.json"), connector=str(run / "connector.json"),
                 authorization_sha256=sha(authorization_path), token_sha256=sha(token),
                 source=str(SOURCE), root=str(ROOT))
    for name, value in (("native.json", native), ("connector.json", connector), ("setup.json", setup)):
        save(run / name, value)
    return setup


def load(run):
    setup = json.loads((Path(run) / "setup.json").read_text())
    require(setup["python"] == sys.executable, "use the interpreter recorded by init")
    node = setup["spec"]["nodes"][setup["node_id"]]
    require(socket.gethostname() == node["hostname"], "setup receipt belongs to another host")
    require(sha(node["authorization"]) == setup["authorization_sha256"], "authorization changed after init")
    native = json.loads(Path(setup["native"]).read_text())
    require(sha(private_token(native["token_file"])) == setup["token_sha256"], "token changed after init")
    require(native["campaign"] == setup["spec"]["campaign"], "native config campaign changed")
    return setup


def environment(setup, role):
    node = setup["spec"]["nodes"][setup["node_id"]]
    fixed = {"MC_STORE_RDMA_GPU_DIRECT": "0", "MOONCAKE_STORE_CHECKSUM": "1",
             "MC_STORE_LOCAL_HOT_CACHE_SIZE": "0", "MC_NUM_QP_PER_EP": "16",
             "VLLM_TTFT_BREAKDOWN": "1", "CUDA_VISIBLE_DEVICES": node["cuda_devices"],
             "MOONCAKE_REQUESTER_LOCAL_HOSTNAME": f"{node['ip']}:{node['te_port']}"}
    if role == "worker":
        fixed.update(MC_CALLOSUM_STORE_CONFIG=setup["native"], MOONCAKE_CONFIG_PATH=setup["connector"])
    if role == "master":
        fixed["MC_CALLOSUM_RETAIN_COPY_TASKS"] = "1"
    forbidden = ("MC_CXL_", "MC_STORE_USE_HUGEPAGE", "MOONCAKE_PREFERRED_SEGMENT")
    for name, value in os.environ.items():
        if name.startswith(forbidden):
            raise RuntimeError(f"inherited {name} conflicts with the resolved serving mode")
        if name in fixed:
            require(value == fixed[name], f"inherited {name} conflicts with resolved configuration")
        elif name in ("MC_CALLOSUM_STORE_CONFIG", "MOONCAKE_CONFIG_PATH", "MC_CALLOSUM_RETAIN_COPY_TASKS"):
            raise RuntimeError(f"{name} is not valid for the selected role")
    env = os.environ.copy()
    env.update(fixed)
    libraries = str(Path(sys.prefix) / "lib")
    env["LD_LIBRARY_PATH"] = libraries + (":" + env["LD_LIBRARY_PATH"] if env.get("LD_LIBRARY_PATH") else "")
    return env, fixed


def vllm_root():
    module = importlib.util.find_spec("vllm")
    require(module is not None and module.submodule_search_locations, "vLLM is not installed")
    return Path(next(iter(module.submodule_search_locations)))


def instrument(root, run):
    """Add evidence only to the audited legacy connector/scheduler seams."""
    changes = []
    export_anchor = "                failed = [i for i, v in enumerate(res) if v < 0]\n"
    export = '''                # Callosum serving evidence v1 (ordinary save, not eviction).
                for key, size, start, end, group, result in zip(
                    keys, sizes, starts, ends, group_indices, res, strict=True
                ):
                    emit_ttft_event(
                        "callosum_connector_export", request_id=req_id,
                        object_key=key, num_bytes=sum(size), start_token=start,
                        end_token=end, group_index=group,
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
    for relative, marker, anchor, addition in (
        (WORKER, '"callosum_connector_export"', export_anchor, export),
        (SCHEDULER, '"callosum_compute"', compute_anchor, compute),
    ):
        path = Path(root) / relative
        original = path.read_text()
        require("emit_ttft_event" in original, f"installed source lacks required trace API: {relative}")
        if marker in original:
            require(addition in original, "existing Callosum trace instrumentation differs")
            continue
        require(original.count(anchor) == 1, f"installed source does not match audited seam: {relative}")
        updated = original.replace(anchor, addition + anchor)
        ast.parse(updated)
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
        return [str(binary), f"--port={spec['master_port']}", f"--metrics_port={spec['metrics_port']}",
                "--memory_allocator=offset", "--enable_http_metadata_server=true",
                f"--http_metadata_server_port={spec['metadata_port']}"]
    model = spec["model"]
    result = [sys.executable, "-m", "vllm.entrypoints.openai.api_server", "--model", model["path"],
              "--tokenizer", model["path"], "--served-model-name", "callosum-test",
              "--dtype", model["dtype"], "--kv-cache-dtype", model["kv_dtype"],
              "--block-size", str(model["block_size"]), "--max-model-len", str(model["max_model_len"]),
              "--tensor-parallel-size", "1", "--pipeline-parallel-size", "1",
              "--no-enable-prefix-caching", "--prefix-caching-hash-algo", "sha256",
              "--enable-request-id-headers", "--host", node["ip"], "--port", str(node["api_port"])]
    if role == "worker":
        result += ["--kv-transfer-config", json.dumps(dict(kv_connector="MooncakeStoreConnector",
                       kv_role="kv_both", kv_connector_extra_config={"load_async": True}))]
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
    states = {}
    for line in content.splitlines()[1:]:
        identity, state = line.split()
        states[identity] = state
    return states


def start(setup, role):
    run = Path(setup["run"])
    receipt = run / f"{role}.process.json"
    require(not receipt.exists(), f"inspect retained process receipt before replacement: {receipt}")
    if role == "cold":
        require(not (run / "worker.process.json").exists(), "stop the owned requester worker before the cold control")
    env, effective = environment(setup, role)
    argv = command(setup, role)
    node = setup["spec"]["nodes"][setup["node_id"]]
    port = setup["spec"]["master_port"] if role == "master" else node["api_port"]
    with socket.socket() as probe:
        require(probe.connect_ex((node["ip"], port)) != 0, "selected service port is already occupied")
    if role != "master":
        require((run / "preflight.json").exists(), "preflight must pass before starting inference")
    log = run / f"{role}.log"
    require(not log.exists(), "preserve existing service log; select a fresh campaign after controlled retirement")
    with log.open("xb") as output:
        child = subprocess.Popen(argv, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    save(receipt, dict(pid=child.pid, stamp=process_stamp(child.pid), argv=argv,
                       role=role, campaign=setup["spec"]["campaign"], environment=effective))
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        require(child.poll() is None, f"{role} exited; inspect {log}; no other service was stopped")
        try:
            if role == "master":
                with socket.create_connection((node["ip"], port), timeout=1):
                    return str(receipt)
            elif http(f"http://{node['ip']}:{port}/v1/models", timeout=2).get("data"):
                return str(receipt)
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError(f"readiness timed out; retain {receipt} and {log}; no process was killed")


def stop(setup, role):
    run = Path(setup["run"])
    path = run / f"{role}.process.json"
    receipt = json.loads(path.read_text())
    require(receipt["campaign"] == setup["spec"]["campaign"], "process belongs to another campaign")
    require(all(state != "PENDING" for state in journals(run).values()),
            "pending DMA authorization prevents retirement; preserve journals and source pins")
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
            require(not retired.exists(), "retirement evidence collision")
            path.rename(retired)
            return str(retired)
        time.sleep(0.2)
    raise RuntimeError("owned process has not exited; do not force kill or recycle its arena")


def preflight(setup):
    require(platform.system() == "Linux", "Linux/RDMA/CUDA preflight is unavailable on this development Mac")
    require(sys.prefix != sys.base_prefix, "use a target-compatible venv, not system Python")
    node = setup["spec"]["nodes"][setup["node_id"]]
    env, effective = environment(setup, "worker")
    info = Path(node["dax_path"]).stat()
    require(stat.S_ISCHR(info.st_mode), "DAX path must be a character device")
    fd = os.open(node["dax_path"], os.O_RDWR); os.close(fd)  # Inventory only: no mapping/write.
    require(Path("/sys/class/infiniband", node["hca"]).exists(), "configured RDMA device is unavailable")
    import mooncake.store as store
    require(getattr(store, "CALLOSUM_STORE_ABI", 0) == 1, "imported Store binding lacks Callosum code")
    root = vllm_root()
    for relative, marker in ((WORKER, '"callosum_connector_export"'), (SCHEDULER, '"callosum_compute"')):
        require(marker in (root / relative).read_text(), "install the bounded trace instrumentation before startup")
    model_path = Path(setup["spec"]["model"]["path"]).resolve(strict=True)
    config = json.loads((model_path / "config.json").read_text())
    require(config.get("model_type") in ("llama", "qwen2", "qwen3") and not config.get("sliding_window"),
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
                  model_manifest=manifest, binding=str(store.__file__), binding_sha256=sha(store.__file__),
                  connector=str(root / WORKER), connector_sha256=sha(root / WORKER),
                  scheduler_sha256=sha(root / SCHEDULER), native_abi=store.CALLOSUM_STORE_ABI,
                  authorization_sha256=setup["authorization_sha256"], hardware="UNVERIFIED")
    save(Path(setup["run"]) / "preflight.json", report)
    subprocess.run(["bash", str(ROOT / "scripts/capture_preflight.sh"), "--output",
                    str(Path(setup["run"]) / "host-preflight"), "--checkout", str(SOURCE),
                    "--python", sys.executable, "--dax", node["dax_path"]], env=env, check=True)
    return report


def request(setup, phase):
    node = setup["spec"]["nodes"][setup["node_id"]]
    require((phase == "produce" and node["role"] == "source") or
            (phase in ("reuse", "cold") and node["role"] == "requester"), "request phase does not match node role")
    role = "cold" if phase == "cold" else "worker"
    require((Path(setup["run"]) / f"{role}.process.json").exists(), "start the owned ready service first")
    model = setup["spec"]["model"]
    prompt = [model["token_id"]] * (model["block_size"] * model["prefix_blocks"] + 1)
    identity = setup["spec"]["campaign"] + "-" + phase
    path = Path(setup["run"]) / f"{phase}.response.json"
    require(not path.exists(), "request already has evidence; do not warm the prefix again")
    started = time.monotonic_ns()
    response = http(f"http://{node['ip']}:{node['api_port']}/v1/completions",
                    dict(model="callosum-test", prompt=prompt, max_tokens=1, temperature=0,
                         stream=False, seed=0), identity)
    require(response.get("usage", {}).get("prompt_tokens") == len(prompt) and response.get("choices"),
            "inference response is incomplete or prompt accounting differs")
    report = dict(phase=phase, request_id=identity, prompt_tokens=len(prompt), response=response,
                  elapsed_ns=time.monotonic_ns() - started, status="INFERENCE_ONLY")
    save(path, report)
    return report


def build(setup):
    require(platform.system() == "Linux" and sys.prefix != sys.base_prefix,
            "production Store builds require Linux and the selected target-compatible venv")
    env, _ = environment(setup, "build")
    directory = Path(setup["run"]) / "build-store-cuda"
    argv = ["cmake", "-S", str(SOURCE), "-B", str(directory), f"-DPython3_EXECUTABLE={sys.executable}",
            f"-DCMAKE_INSTALL_PREFIX={sys.prefix}", "-DUSE_CUDA=ON", "-DUSE_CXL=OFF", "-DUSE_TCP=OFF",
            "-DWITH_TE=ON", "-DWITH_STORE=ON", "-DWITH_STORE_RUST=OFF", "-DWITH_EP=OFF",
            "-DBUILD_EXAMPLES=OFF", "-DBUILD_BENCHMARK=OFF", "-DBUILD_UNIT_TESTS=ON"]
    commands = [argv, ["cmake", "--build", str(directory), "--parallel", "2", "--target", "store", "engine", "mooncake_master", "mooncake_client"],
                ["cmake", "--install", str(directory)]]
    save(Path(setup["run"]) / "build.commands.json", commands)
    for argv in commands:
        subprocess.run(argv, env=env, check=True)
    return dict(build=str(directory), interpreter=sys.executable)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("token", "init", "build", "instrument", "preflight", "start", "request", "stop"))
    parser.add_argument("--run", type=Path)
    parser.add_argument("--spec", type=Path)
    parser.add_argument("--node")
    parser.add_argument("--token", type=Path)
    parser.add_argument("--role", choices=("master", "worker", "cold"))
    parser.add_argument("--phase", choices=("produce", "reuse", "cold"))
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
                    result = (start if args.stage == "start" else stop)(setup, args.role)
                else:
                    require(args.phase, "request requires --phase")
                    result = request(setup, args.phase)
        print(encoded(dict(status="PREPARATION_OR_INFERENCE_ONLY", result=result)), end="")
        return 0
    except Exception as error:
        print(encoded(dict(status="BLOCKED", cause=str(error),
                           recovery="retain existing config, journals, services and evidence; repair the named prerequisite")), end="")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
