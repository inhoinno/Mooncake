#!/usr/bin/env python3
"""TODO#Extra baseline: GPU <- RDMA fetching using the Mooncake lib.

One consumer fetches a variable-size KV block that is distributed across N
DRAM-backed Mooncake clients (the master shards the object's 16 MiB slices over
their global segments), and times:

  1. fetch to local DRAM      -- store.get(key)                (RDMA -> host)
  2. fetch to GPU (staged)    -- batch_get_into(key -> cuda)   (RDMA -> host -> GPU)
  3. fetch to GPU (GPUDirect) -- same, MC_STORE_RDMA_GPU_DIRECT=1 (plumbing hook)

Roles:
  server    -- mount a DRAM global segment (protocol=rdma) and stay alive so its
               memory is registered/served over RDMA until signaled.
  prep      -- put one object of --block-bytes into the pool (slices shard across
               the live servers).
  consumer  -- run the three measurements above and emit a JSON summary.

Topology matches the diagram: N Mooncake clients (legacy) over one RDMA fabric,
one Mooncake master. Single-node (loopback RDMA, N processes) by default; the
same roles run across real nodes by pointing --master-server / --metadata-server
at the shared endpoints.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import importlib
import json
import mmap
import os
import signal
import sys
import time
from typing import Any

MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


class PerfError(RuntimeError):
    pass


def _registered_payload(store: Any, label: str, size: int) -> tuple[Any, int]:
    """Create a page-aligned mmap and register it as an RDMA source buffer."""
    seed = hashlib.sha256(label.encode()).digest()
    tile = (seed * (MiB // len(seed) + 1))[:MiB]
    buf = mmap.mmap(-1, size)
    off = 0
    while off < size:
        n = min(MiB, size - off)
        buf[off:off + n] = tile[:n]
        off += n
    ptr = ctypes.addressof(ctypes.c_char.from_buffer(buf))
    rc = int(store.register_buffer(ptr, size))
    if rc != 0:
        buf.close()
        raise PerfError(f"register source buffer failed rc={rc}")
    return buf, ptr


def _open_store(args: argparse.Namespace, global_segment: int, local_buffer: int):
    module = importlib.import_module(args.store_module)
    store = module.MooncakeDistributedStore()
    rc = int(
        store.setup(
            args.local_hostname,
            args.metadata_server,
            global_segment,
            local_buffer,
            "rdma",
            args.device_name,
            args.master_server,
        )
    )
    if rc != 0:
        store.close()
        raise PerfError(f"store.setup failed rc={rc}")
    if int(store.health_check()) != 0:
        store.close()
        raise PerfError("store.health_check failed")
    return module, store


def run_server(args: argparse.Namespace) -> dict[str, Any]:
    # Contribute DRAM to the pool and stay alive until SIGTERM/SIGINT.
    _module, store = _open_store(args, args.segment_bytes, 64 * MiB)
    print(json.dumps({"role": "server", "status": "READY",
                      "segment_bytes": args.segment_bytes,
                      "local_hostname": args.local_hostname}), flush=True)
    stop = {"v": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("v", True))
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("v", True))
    try:
        while not stop["v"]:
            time.sleep(0.2)
    finally:
        store.close()
    return {"role": "server", "status": "PASS"}


def _assert_placement(placement: dict[str, Any], args: argparse.Namespace) -> None:
    # Fan-out: the object must span at least the requested number of source
    # segments (proves "Distributed /N").
    if len(placement["source_endpoints"]) < args.min_source_segments:
        raise PerfError(
            f"object spans {len(placement['source_endpoints'])} source segments; "
            f"expected at least {args.min_source_segments}: {placement}"
        )
    # Transport: every source segment must be RDMA. A 'tcp' here means a source
    # fell back (e.g. its RDMA segment did not register) and the fetch would not
    # use RDMA -- fail loudly instead of silently measuring TCP.
    bad = [p for p in placement["source_protocols"] if p != "rdma"]
    if bad:
        raise PerfError(
            f"object placed on non-RDMA segment(s) {placement['source_protocols']}; "
            f"expected all 'rdma'. A source likely fell back to tcp: {placement}"
        )


def run_prep(args: argparse.Namespace) -> dict[str, Any]:
    module, store = _open_store(args, args.prep_segment_bytes, 64 * MiB)
    try:
        # Default: write fresh against the CURRENT sources. A pre-existing object
        # from an earlier run (different sources / pre-MTU-fix tcp fallback)
        # otherwise poisons the measurement via the idempotent short-circuit.
        if int(store.is_exist(args.key)) == 1:
            if args.reuse:
                placement = _placement_for_key(store, args.key)
                _assert_placement(placement, args)
                return {"role": "prep", "status": "PASS", "note": "already present",
                        "block_bytes": args.block_bytes, **placement}
            rc = int(store.remove(args.key, True))  # force: skip lease checks
            if rc != 0:
                raise PerfError(f"failed to remove stale object {args.key} rc={rc}")
        config = module.ReplicateConfig()
        config.replica_num = 1
        config.nof_replica_num = 0
        # put_from() avoids constructing a second multi-GiB Python bytes object.
        # The master splits this object into <=kMaxSliceSize slices and places
        # those slices across the live RDMA source segments.
        payload, ptr = _registered_payload(store, args.key, args.block_bytes)
        try:
            rc = int(store.put_from(args.key, ptr, args.block_bytes, config))
        finally:
            store.unregister_buffer(ptr)
            payload.close()
        if rc != 0:
            raise PerfError(f"put {args.key} failed rc={rc}")
        placement = _placement_for_key(store, args.key)
        _assert_placement(placement, args)
        return {"role": "prep", "status": "PASS", "block_bytes": args.block_bytes,
                **placement}
    finally:
        store.close()


def _placement_for_key(store: Any, key: str) -> dict[str, Any]:
    endpoints: set[str] = set()
    protocols: set[str] = set()
    slices = 0
    bytes_described = 0
    for replica in store.get_replica_desc(key):
        if not replica.is_memory_replica():
            continue
        desc = replica.get_memory_descriptor().buffer_descriptor
        endpoints.add(str(desc.transport_endpoint))
        protocols.add(str(desc.protocol))
        slices += 1
        bytes_described += int(desc.size)
    return {
        "replica_slices": slices,
        "source_endpoints": sorted(endpoints),
        "source_segment_count": len(endpoints),
        "source_protocols": sorted(protocols),
        "descriptor_bytes": bytes_described,
    }


def _rate(bytes_n: int, dt: float, iterations: int) -> dict[str, Any]:
    return {
        "iterations": iterations,
        "total_bytes": bytes_n,
        "sec": round(dt, 6),
        "latency_sec_avg": round(dt / iterations, 6) if iterations else 0.0,
        "GBps": round(bytes_n / dt / 1e9, 3) if dt > 0 else 0.0,
        "GiBps": round(bytes_n / dt / GiB, 3) if dt > 0 else 0.0,
    }


def run_consumer(args: argparse.Namespace) -> dict[str, Any]:
    gdr = os.environ.get("MC_STORE_RDMA_GPU_DIRECT") == "1"
    # Registered get_into needs no internal staging pool. Staged GPU reads do;
    # GDR deliberately has no fallback after selection.
    local_buffer = (
        args.consumer_buffer_bytes
        if args.mode in ("both", "gpu") and not gdr
        else 64 * MiB
    )
    module, store = _open_store(args, 0, local_buffer)
    out: dict[str, Any] = {
        "role": "consumer", "status": "PASS", "gpu_direct_requested": gdr,
        "block_bytes": args.block_bytes, "key": args.key,
        **_placement_for_key(store, args.key),
    }
    try:
        if int(store.is_exist(args.key)) != 1:
            raise PerfError(f"{args.key} missing; run role=prep first")

        # --- (1) fetch to local DRAM ---------------------------------------
        if args.mode in ("both", "dram"):
            host = mmap.mmap(-1, args.block_bytes)
            host_ptr = ctypes.addressof(ctypes.c_char.from_buffer(host))
            rc = int(store.register_buffer(host_ptr, args.block_bytes))
            if rc != 0:
                host.close()
                raise PerfError(f"register destination buffer failed rc={rc}")
            for _ in range(args.warmup):
                got = int(store.get_into(args.key, host_ptr, args.block_bytes))
                if got != args.block_bytes:
                    raise PerfError(f"DRAM warmup returned {got}")
            try:
                t = time.monotonic()
                for _ in range(args.iterations):
                    got = int(store.get_into(args.key, host_ptr, args.block_bytes))
                    if got != args.block_bytes:
                        raise PerfError(f"DRAM get_into returned {got}")
                dt = time.monotonic() - t
                out["to_local_dram"] = _rate(
                    args.block_bytes * args.iterations, dt, args.iterations)
            finally:
                store.unregister_buffer(host_ptr)
                host.close()

        # --- (2)/(3) fetch to GPU (staged or GPUDirect) --------------------
        if args.mode == "dram":
            return out
        torch = importlib.import_module("torch")
        if not torch.cuda.is_available():
            raise PerfError("torch.cuda.is_available() is false")
        torch.cuda.set_device(args.gpu_id)
        dst = torch.empty(args.block_bytes, dtype=torch.uint8, device="cuda")
        ptr, size = int(dst.data_ptr()), args.block_bytes
        for _ in range(args.warmup):
            store.batch_get_into([args.key], [ptr], [size])
            torch.cuda.synchronize()
        t = time.monotonic()
        for _ in range(args.iterations):
            res = list(store.batch_get_into([args.key], [ptr], [size]))
            torch.cuda.synchronize()
            if int(res[0]) != size:
                raise PerfError(f"GPU get_into returned {res[0]}, expected {size}")
        dt = time.monotonic() - t
        path = "rdma_gpu_direct" if gdr else "rdma_host_staged"
        out["gpu_path_selected"] = path
        out["to_gpu" + ("_gpudirect" if gdr else "_staged")] = _rate(
            size * args.iterations, dt, args.iterations)
        return out
    finally:
        store.close()


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--role", choices=["server", "prep", "consumer"], required=True)
    p.add_argument("--store-module", default="mooncake.store")
    p.add_argument("--local-hostname", required=True)
    p.add_argument("--master-server", required=True)
    p.add_argument("--metadata-server", default="http://127.0.0.1:8080/metadata")
    p.add_argument("--device-name", default="", help="RDMA device (blank = auto)")
    p.add_argument("--key", default="dram-rdma-blk")
    p.add_argument("--block-bytes", type=int, default=GiB, help="1..16 GiB KV block")
    # server / prep sizing
    p.add_argument("--segment-bytes", type=int, default=8 * GiB,
                   help="per-server DRAM contributed to the pool")
    p.add_argument("--prep-segment-bytes", type=int, default=0)
    p.add_argument("--reuse", action="store_true",
                   help="reuse an existing object instead of removing + re-putting "
                        "it fresh against the current sources (default: fresh)")
    p.add_argument("--min-source-segments", type=int, default=1,
                   help="prep fails unless placement spans this many RDMA endpoints")
    # consumer sizing / behavior
    p.add_argument("--consumer-buffer-bytes", type=int, default=20 * GiB,
                   help="registered local buffer; must hold one block for get()")
    p.add_argument("--gpu-id", type=int, default=0)
    p.add_argument("--skip-dram", action="store_true")
    p.add_argument("--mode", choices=["both", "dram", "gpu"], default="both")
    p.add_argument("--iterations", type=int, default=1)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--summary-json", default="")
    args = p.parse_args(argv)
    if args.skip_dram:
        args.mode = "gpu"
    if args.iterations <= 0:
        p.error("--iterations must be positive")

    try:
        if args.role == "server":
            summary = run_server(args)
        elif args.role == "prep":
            summary = run_prep(args)
        else:
            summary = run_consumer(args)
    except Exception as err:
        summary = {"role": args.role, "status": "FAIL", "error": repr(err)}
        print(json.dumps(summary), flush=True)
        if args.summary_json:
            json.dump(summary, open(args.summary_json, "w"))
        return 1

    print(json.dumps(summary), flush=True)
    if args.summary_json:
        json.dump(summary, open(args.summary_json, "w"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
