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
import hashlib
import importlib
import json
import os
import signal
import sys
import time
from typing import Any

MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


class PerfError(RuntimeError):
    pass


def _payload_np(np: Any, label: str, size: int) -> Any:
    # Deterministic, cheap to build at multi-GiB sizes (tile a seed block).
    seed = hashlib.sha256(label.encode()).digest()
    tile = (seed * (MiB // len(seed) + 1))[:MiB]
    buf = np.empty(size, dtype=np.uint8)
    view = memoryview(buf)
    off = 0
    while off < size:
        n = min(MiB, size - off)
        view[off:off + n] = tile[:n]
        off += n
    return buf


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


def run_prep(args: argparse.Namespace) -> dict[str, Any]:
    np = importlib.import_module("numpy")
    module, store = _open_store(args, args.prep_segment_bytes, 2 * args.block_bytes)
    try:
        if int(store.is_exist(args.key)) == 1:
            return {"role": "prep", "status": "PASS", "note": "already present",
                    "block_bytes": args.block_bytes}
        config = module.ReplicateConfig()
        config.replica_num = 1
        config.nof_replica_num = 0
        # put() copies the payload through the client's own registered buffer, so
        # the source need not be pre-registered (unlike put_from). The master
        # shards the object's 16 MiB slices across the live server segments.
        payload = _payload_np(np, args.key, args.block_bytes).tobytes()
        rc = int(store.put(args.key, payload, config))
        del payload
        if rc != 0:
            raise PerfError(f"put {args.key} failed rc={rc}")
        return {"role": "prep", "status": "PASS", "block_bytes": args.block_bytes}
    finally:
        store.close()


def _segments_for_key(store: Any, key: str) -> int:
    try:
        return len(store.get_replica_desc(key))
    except Exception:
        return -1


def run_consumer(args: argparse.Namespace) -> dict[str, Any]:
    gdr = os.environ.get("MC_STORE_RDMA_GPU_DIRECT") == "1"
    module, store = _open_store(args, 0, args.consumer_buffer_bytes)
    out: dict[str, Any] = {
        "role": "consumer", "status": "PASS", "gpu_direct": gdr,
        "block_bytes": args.block_bytes, "key": args.key,
        "replica_shards": _segments_for_key(store, args.key),
    }
    try:
        if int(store.is_exist(args.key)) != 1:
            raise PerfError(f"{args.key} missing; run role=prep first")

        def rate(bytes_n: int, dt: float) -> dict[str, float]:
            return {"sec": round(dt, 4),
                    "GBps": round(bytes_n / dt / 1e9, 3) if dt > 0 else 0.0,
                    "GiBps": round(bytes_n / dt / GiB, 3) if dt > 0 else 0.0}

        # --- (1) fetch to local DRAM ---------------------------------------
        if not args.skip_dram:
            for _ in range(args.warmup):
                _ = store.get(args.key)
            t = time.monotonic()
            val = store.get(args.key)
            dt = time.monotonic() - t
            got = len(val) if val is not None else 0
            if got != args.block_bytes:
                raise PerfError(f"DRAM get returned {got}, expected {args.block_bytes}")
            out["to_local_dram"] = rate(got, dt)
            del val

        # --- (2)/(3) fetch to GPU (staged or GPUDirect) --------------------
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
        res = store.batch_get_into([args.key], [ptr], [size])
        torch.cuda.synchronize()
        dt = time.monotonic() - t
        if int(res[0]) != size:
            raise PerfError(f"GPU get_into returned {res[0]}, expected {size}")
        out["to_gpu" + ("_gpudirect" if gdr else "_staged")] = rate(size, dt)
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
    # consumer sizing / behavior
    p.add_argument("--consumer-buffer-bytes", type=int, default=20 * GiB,
                   help="registered local buffer; must hold one block for get()")
    p.add_argument("--gpu-id", type=int, default=0)
    p.add_argument("--skip-dram", action="store_true")
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--summary-json", default="")
    args = p.parse_args(argv)

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
