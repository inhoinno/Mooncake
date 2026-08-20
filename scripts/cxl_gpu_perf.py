#!/usr/bin/env python3
"""Store-level CXL->GPU throughput harness (one master, N client processes).

Two roles:
  prep    -- put NUM_OBJECTS deterministic blocks into the shared CXL pool via
             the single Mooncake master, then exit. Run once before the clients.
  client  -- setup a Store client, allocate BATCH_SIZE reusable GPU tensors, and
             loop batch_get_into(CXL objects -> GPU tensors) for RUNTIME seconds.
             Reports per-client GB/s and a JSON summary.

This measures the real end-to-end CXL->GPU read path (master metadata lookup +
CxlTransport offset resolution + cudaMemcpyAsync into device memory), which is
what the multipath consumer does. It is the "realistic" number; the raw ceiling
is bench/cxl_gpu_bw.cu.

Constraints honored:
  * block bytes default 16 MiB - 4 KiB (16773120): the Store single-slice cap is
    kMaxSliceSize = Slab::kSize - 16, so a full 16 MiB object is rejected.
  * protocol=cxl, global_segment_size=0, faketract/devdax provider -- the exact
    setup proven by the TODO1.5 two-node run on this hardware.

The scaling sweep (1,2,4,8 clients on one GPU) is driven by
run_cxl_gpu_scaling.sh.
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
import random
from typing import Any

# 16 MiB - 4 KiB: largest block under the Store single-slice cap (Slab::kSize-16).
DEFAULT_BLOCK_BYTES = 16 * 1024 * 1024 - 4096


class PerfError(RuntimeError):
    pass


def _payload(label: str, size: int) -> bytes:
    seed = hashlib.sha256(label.encode("utf-8")).digest()
    return (seed * ((size + len(seed) - 1) // len(seed)))[:size]


def _object_key(prefix: str, index: int) -> str:
    return f"{prefix}-obj-{index:06d}"


def _client_object_order(offset: int, count: int, seed: int) -> list[int]:
    if offset < 0 or count <= 0:
        raise PerfError("object-offset must be non-negative and object-count positive")
    order = list(range(offset, offset + count))
    random.Random(seed).shuffle(order)
    return order


def _apply_cxl_env(args: argparse.Namespace) -> None:
    """Publish the MC_CXL_* knobs the Transfer Engine reads at setup()."""
    os.environ["MC_CXL_PROVIDER"] = args.provider
    os.environ["MC_CXL_BACKEND_KIND"] = args.backend_kind
    os.environ["MC_CXL_DEV_PATH"] = args.device_name
    os.environ["MC_CXL_DEV_SIZE"] = str(args.dev_size)
    os.environ["MC_CXL_POOL_ID"] = args.pool_id
    os.environ["MC_CXL_MAP_OFFSET"] = str(args.map_offset)
    os.environ.setdefault("MOONCAKE_STORE_CHECKSUM", "0")  # perf: skip checksum
    if args.provider == "mooncake":
        # Native provider requires a disjoint owned partition per mount.
        os.environ["MC_CXL_OWNED_OFFSET"] = str(args.owned_offset)
        os.environ["MC_CXL_OWNED_SIZE"] = str(args.owned_size)


def _open_store(args: argparse.Namespace) -> tuple[Any, Any]:
    module = importlib.import_module(args.store_module)
    store = module.MooncakeDistributedStore()
    rc = int(
        store.setup(
            args.local_hostname,
            args.metadata_server,
            0,  # global_segment_size must be 0 for protocol=cxl
            args.local_buffer_size,
            "cxl",
            args.device_name,
            args.master_server,
        )
    )
    if rc != 0:
        store.close()
        raise PerfError(f"store.setup failed rc={rc}")
    health = int(store.health_check())
    if health != 0:
        store.close()
        raise PerfError(f"store.health_check failed rc={health}")
    return module, store


def _write_objects(store: Any, module: Any, args: argparse.Namespace) -> int:
    config = module.ReplicateConfig()
    config.replica_num = 1
    config.nof_replica_num = 0
    payload = _payload(f"{args.key_prefix}-blk", args.block_bytes)
    written = 0
    for i in range(args.num_objects):
        key = _object_key(args.key_prefix, i)
        if int(store.is_exist(key)) == 1:  # idempotent
            written += args.block_bytes
            continue
        rc = int(store.put(key, payload, config))
        if rc != 0:
            raise PerfError(f"put {key} failed rc={rc}")
        written += args.block_bytes
    return written


def run_prep(args: argparse.Namespace) -> dict[str, Any]:
    module, store = _open_store(args)
    try:
        written = _write_objects(store, module, args)
        return {"role": "prep", "status": "PASS",
                "num_objects": args.num_objects, "block_bytes": args.block_bytes,
                "bytes_written": written}
    finally:
        store.close()


def run_server(args: argparse.Namespace) -> dict[str, Any]:
    # Write the objects, then STAY ALIVE so the allocating segment descriptor
    # remains resolvable. A CXL replica references the writer's segment endpoint;
    # if the writer exits (and its descriptor is deleted from the metadata store)
    # readers get 404 -> TRANSFER_FAIL when resolving the object.
    module, store = _open_store(args)
    written = _write_objects(store, module, args)
    print(json.dumps({"role": "server", "status": "READY",
                      "num_objects": args.num_objects,
                      "block_bytes": args.block_bytes,
                      "bytes_written": written}), flush=True)
    stop = {"v": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("v", True))
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("v", True))
    try:
        while not stop["v"]:
            time.sleep(0.2)
    finally:
        store.close()
    return {"role": "server", "status": "PASS"}


def run_client(args: argparse.Namespace) -> dict[str, Any]:
    torch = importlib.import_module("torch")
    if not torch.cuda.is_available():
        raise PerfError("torch.cuda.is_available() is false")
    torch.cuda.set_device(args.gpu_id)

    module, store = _open_store(args)
    try:
        # Confirm the objects exist (prep must have run) and are CXL-resident.
        probe = _object_key(args.key_prefix, 0)
        if int(store.is_exist(probe)) != 1:
            raise PerfError(f"object {probe} missing; run role=prep first")

        # Reusable GPU destination tensors: allocate once, measure steady state.
        tensors = [
            torch.empty(args.block_bytes, dtype=torch.uint8, device="cuda")
            for _ in range(args.batch_size)
        ]
        ptrs = [int(t.data_ptr()) for t in tensors]
        sizes = [args.block_bytes] * args.batch_size

        object_count = args.object_count or args.num_objects
        if args.object_offset + object_count > args.num_objects:
            raise PerfError(
                f"client range [{args.object_offset}, "
                f"{args.object_offset + object_count}) exceeds {args.num_objects} objects"
            )
        order = _client_object_order(
            args.object_offset, object_count, args.access_seed)

        def one_batch(base: int) -> None:
            keys = [
                _object_key(args.key_prefix, order[(base + j) % object_count])
                for j in range(args.batch_size)
            ]
            # The pybind call is eager: RealClient::batch_get_into_internal
            # completes before this list is returned. Synchronize as an extra
            # CUDA-side assertion before accounting bytes.
            res = list(store.batch_get_into(keys, ptrs, sizes))
            torch.cuda.synchronize()
            for r in res:
                if int(r) != args.block_bytes:
                    raise PerfError(
                        f"batch_get_into returned {r}, expected {args.block_bytes}"
                    )

        # Warmup (not timed): populate caches / CUDA context / first-touch.
        base = 0
        for _ in range(args.warmup):
            one_batch(base)
            base += args.batch_size

        # All clients in a scaling round wait for one epoch barrier. This makes
        # aggregate throughput total_bytes / common_wall_interval rather than a
        # physically misleading sum of partially overlapping per-client rates.
        if args.start_at_epoch > 0:
            delay = args.start_at_epoch - time.time()
            if delay > 0:
                time.sleep(delay)

        # Timed loop.
        bytes_read = 0
        batches = 0
        start_epoch = time.time()
        start = time.monotonic()
        deadline = start + args.runtime
        while time.monotonic() < deadline:
            one_batch(base)
            base += args.batch_size
            bytes_read += args.block_bytes * args.batch_size
            batches += 1
        elapsed = time.monotonic() - start
        end_epoch = time.time()

        gbps = (bytes_read / elapsed) / 1e9 if elapsed > 0 else 0.0
        gibps = (bytes_read / elapsed) / (1024**3) if elapsed > 0 else 0.0
        return {
            "role": "client",
            "status": "PASS",
            "client_id": args.client_id,
            "gpu_id": args.gpu_id,
            "block_bytes": args.block_bytes,
            "batch_size": args.batch_size,
            "batches": batches,
            "bytes_read": bytes_read,
            "object_offset": args.object_offset,
            "object_count": object_count,
            "access_seed": args.access_seed,
            "start_epoch": start_epoch,
            "end_epoch": end_epoch,
            "elapsed_sec": round(elapsed, 4),
            "throughput_GBps": round(gbps, 3),
            "throughput_GiBps": round(gibps, 3),
        }
    finally:
        store.close()


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--role", choices=["prep", "server", "client"], required=True)
    p.add_argument("--store-module", default="mooncake.store")
    # topology / endpoints
    p.add_argument("--local-hostname", required=True, help="unique host:port")
    p.add_argument("--master-server", required=True, help="single master host:port")
    p.add_argument("--metadata-server", default="P2PHANDSHAKE")
    p.add_argument("--local-buffer-size", type=int, default=512 * 1024 * 1024)
    # CXL pool
    p.add_argument("--device-name", required=True, help="/dev/daxX.Y")
    p.add_argument("--dev-size", type=int, required=True, help="full pool bytes")
    p.add_argument("--pool-id", default="cxl-gpu-perf-pool")
    p.add_argument("--map-offset", type=int, default=0)
    p.add_argument("--provider", default="faketract", choices=["faketract", "mooncake"])
    p.add_argument("--backend-kind", default="devdax", choices=["devdax", "file"])
    p.add_argument("--owned-offset", type=int, default=0, help="mooncake provider only")
    p.add_argument("--owned-size", type=int, default=0, help="mooncake provider only")
    # workload
    p.add_argument("--block-bytes", type=int, default=DEFAULT_BLOCK_BYTES)
    p.add_argument("--num-objects", type=int, default=64)
    p.add_argument("--object-offset", type=int, default=0,
                   help="first object in this client's disjoint working set")
    p.add_argument("--object-count", type=int, default=0,
                   help="objects in this client working set (0 = all)")
    p.add_argument("--access-seed", type=int, default=1,
                   help="deterministic permutation; avoids lockstep key reuse")
    p.add_argument("--start-at-epoch", type=float, default=0.0,
                   help="shared wall-clock barrier for one scaling round")
    p.add_argument("--batch-size", type=int, default=8)
    p.add_argument("--runtime", type=float, default=10.0)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--gpu-id", type=int, default=0)
    p.add_argument("--client-id", default="client0")
    p.add_argument("--key-prefix", default="cxlperf")
    p.add_argument("--summary-json", default="")
    args = p.parse_args(argv)

    if args.block_bytes > DEFAULT_BLOCK_BYTES and args.provider != "file":
        print(
            f"[WARN] block-bytes {args.block_bytes} exceeds the Store slice cap "
            f"{DEFAULT_BLOCK_BYTES}; PutStart will reject it",
            file=sys.stderr,
        )

    _apply_cxl_env(args)
    try:
        if args.role == "prep":
            summary = run_prep(args)
        elif args.role == "server":
            summary = run_server(args)
        else:
            summary = run_client(args)
    except Exception as err:  # structured failure, nonzero exit
        summary = {"role": args.role, "status": "FAIL", "error": repr(err)}
        print(json.dumps(summary), flush=True)
        if args.summary_json:
            with open(args.summary_json, "w") as fh:
                json.dump(summary, fh)
        return 1

    print(json.dumps(summary), flush=True)
    if args.summary_json:
        with open(args.summary_json, "w") as fh:
            json.dump(summary, fh)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
