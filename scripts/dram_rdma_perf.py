#!/usr/bin/env python3
"""TODO#Extra baseline: GPU <- RDMA fetching using the Mooncake lib.

One consumer fetches one or more variable-size KV objects from N DRAM-backed
Mooncake clients.  In multi-key mode, prep deterministically places one whole
object on each requested source segment; Mooncake does not stripe one memory
replica across multiple source segments.  The consumer times:

  1. fetch to local DRAM      -- store.get(key)                (RDMA -> host)
  2. fetch to GPU (staged)    -- batch_get_into(key -> cuda)   (RDMA -> host -> GPU)
  3. fetch to GPU (GPUDirect) -- same, MC_STORE_RDMA_GPU_DIRECT=1 (plumbing hook)

Roles:
  server    -- mount a DRAM global segment (protocol=rdma) and stay alive so its
               memory is registered/served over RDMA until signaled.
  prep      -- issue one put_from() per key and verify COMPLETE publication on
               the requested source endpoint.
  consumer  -- run the three measurements above and emit a JSON summary.
  cleanup   -- remove all keys in the current dataset after results are saved.

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


def _target_endpoint(args: argparse.Namespace, index: int) -> str | None:
    if not args.source_endpoints:
        return None
    return args.source_endpoints[index % len(args.source_endpoints)]


def _wait_for_published_object(
    store: Any, key: str, timeout_sec: float
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if int(store.is_exist(key)) == 1:
            placement = _placement_for_key(store, key)
            if placement["replica_slices"] > 0:
                return placement
        time.sleep(0.05)
    raise PerfError(
        f"put returned success but object {key!r} was not published COMPLETE "
        f"within {timeout_sec}s"
    )


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


def _object_keys(args: argparse.Namespace) -> list[str]:
    if args.object_count == 1:
        return [args.key]
    return [f"{args.key}-{index:04d}" for index in range(args.object_count)]


def _assert_placement(placement: dict[str, Any], args: argparse.Namespace) -> None:
    # Multi-source coverage is measured across distinct objects. Mooncake does
    # not stripe one memory replica across several source segments.
    if len(placement["source_endpoints"]) < args.min_source_segments:
        raise PerfError(
            f"objects span {len(placement['source_endpoints'])} source segments; "
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
        keys = _object_keys(args)
        placements: dict[str, dict[str, Any]] = {}
        for index, key in enumerate(keys):
            target = _target_endpoint(args, index)
            config = module.ReplicateConfig()
            config.replica_num = 1
            config.nof_replica_num = 0
            if target is not None:
                config.preferred_segments = [target]
            # Default: remove stale objects so this run is placed against the
            # currently mounted RDMA source set.
            if int(store.is_exist(key)) == 1 and not args.reuse:
                rc = int(store.remove(key, True))
                if rc != 0:
                    raise PerfError(f"failed to remove stale object {key} rc={rc}")
            if int(store.is_exist(key)) != 1:
                payload, ptr = _registered_payload(store, key, args.block_bytes)
                try:
                    # Deliberately one PUT per key. The baseline compares many
                    # sequential single GETs with one true multi-key batch GET.
                    rc = int(store.put_from(key, ptr, args.block_bytes, config))
                finally:
                    store.unregister_buffer(ptr)
                    payload.close()
                if rc != 0:
                    raise PerfError(f"put {key} failed rc={rc}")
            placement = _wait_for_published_object(
                store, key, args.publish_timeout_sec)
            if target is not None and placement["source_endpoints"] != [target]:
                store.remove(key, True)
                raise PerfError(
                    f"key {key!r} was not placed on requested source {target!r}: "
                    f"{placement}"
                )
            placements[key] = placement
            print(json.dumps({"event": "object_published", "key": key,
                              "target_endpoint": target, **placement}), flush=True)

        placement = _placement_for_keys(placements)
        _assert_placement(placement, args)
        return {"role": "prep", "status": "PASS", "keys": keys,
                "object_count": len(keys), "block_bytes": args.block_bytes,
                "total_bytes": len(keys) * args.block_bytes,
                "per_key_placement": placements, **placement}
    finally:
        store.close()


def run_cleanup(args: argparse.Namespace) -> dict[str, Any]:
    _module, store = _open_store(args, 0, 64 * MiB)
    removed = 0
    try:
        for key in _object_keys(args):
            if int(store.is_exist(key)) != 1:
                continue
            rc = int(store.remove(key, True))
            if rc != 0:
                raise PerfError(f"failed to remove {key} rc={rc}")
            removed += 1
        return {"role": "cleanup", "status": "PASS", "removed": removed}
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


def _placement_for_keys(
    placements: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    endpoints = sorted({endpoint for placement in placements.values()
                        for endpoint in placement["source_endpoints"]})
    protocols = sorted({protocol for placement in placements.values()
                        for protocol in placement["source_protocols"]})
    return {
        "replica_slices": sum(p["replica_slices"] for p in placements.values()),
        "source_endpoints": endpoints,
        "source_segment_count": len(endpoints),
        "source_protocols": protocols,
        "descriptor_bytes": sum(p["descriptor_bytes"] for p in placements.values()),
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


def _batch_groups(
    keys: list[str], ptrs: list[int], sizes: list[int], group_size: int
) -> list[tuple[list[str], list[int], list[int]]]:
    """Return stable per-source batches; zero means one global batch."""
    width = group_size or len(keys)
    return [
        (keys[start:start + width], ptrs[start:start + width],
         sizes[start:start + width])
        for start in range(0, len(keys), width)
    ]


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
    keys = _object_keys(args)
    try:
        missing = [key for key in keys if int(store.is_exist(key)) != 1]
        if missing:
            raise PerfError(f"missing keys {missing}; run role=prep first")
        placements = {key: _placement_for_key(store, key) for key in keys}
        aggregate_placement = _placement_for_keys(placements)
        _assert_placement(aggregate_placement, args)
        if args.source_endpoints:
            expected = sorted(set(args.source_endpoints[:len(keys)]))
            if aggregate_placement["source_endpoints"] != expected:
                raise PerfError(
                    "consumer placement differs from deterministic prep: "
                    f"expected={expected}, actual="
                    f"{aggregate_placement['source_endpoints']}"
                )
        out: dict[str, Any] = {
            "role": "consumer", "status": "PASS",
            "gpu_direct_requested": gdr, "block_bytes": args.block_bytes,
            "keys": keys, "object_count": len(keys),
            "gpu_pattern": args.gpu_pattern,
            "per_key_placement": placements, **aggregate_placement,
        }

        # --- (1) fetch to local DRAM ---------------------------------------
        if args.mode in ("both", "dram"):
            host = mmap.mmap(-1, args.block_bytes)
            host_ptr = ctypes.addressof(ctypes.c_char.from_buffer(host))
            rc = int(store.register_buffer(host_ptr, args.block_bytes))
            if rc != 0:
                host.close()
                raise PerfError(f"register destination buffer failed rc={rc}")
            try:
                for key in keys:
                    got = int(store.get_into(key, host_ptr, args.block_bytes))
                    if got != args.block_bytes:
                        raise PerfError(f"DRAM warmup returned {got} for {key}")
                t = time.monotonic()
                for _ in range(args.iterations):
                    for key in keys:
                        got = int(store.get_into(key, host_ptr, args.block_bytes))
                        if got != args.block_bytes:
                            raise PerfError(f"DRAM get_into returned {got} for {key}")
                dt = time.monotonic() - t
                operations = args.iterations * len(keys)
                out["to_local_dram_sequential_single"] = _rate(
                    args.block_bytes * operations, dt, operations)
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
        destinations = [torch.empty(args.block_bytes, dtype=torch.uint8,
                                    device="cuda") for _ in keys]
        ptrs = [int(dst.data_ptr()) for dst in destinations]
        sizes = [args.block_bytes] * len(keys)
        batch_groups = _batch_groups(keys, ptrs, sizes, args.batch_group_size)
        for _ in range(args.warmup):
            if args.gpu_pattern == "single":
                for key, ptr, size in zip(keys, ptrs, sizes):
                    got = int(store.get_into(key, ptr, size))
                    if got != size:
                        raise PerfError(f"GPU warmup returned {got} for {key}")
            else:
                for group_keys, group_ptrs, group_sizes in batch_groups:
                    results = list(store.batch_get_into(
                        group_keys, group_ptrs, group_sizes))
                    if any(int(got) != size for got, size
                           in zip(results, group_sizes)):
                        raise PerfError(
                            f"GPU batch warmup returned {results} for "
                            f"keys={group_keys}")
            torch.cuda.synchronize()
        t = time.monotonic()
        transfer_to_staging_ns = 0
        staging_to_gpu_ns = 0
        for _ in range(args.iterations):
            if args.gpu_pattern == "single":
                if not hasattr(store, "get_into_profiled"):
                    raise PerfError("store.so is missing get_into_profiled; rebuild")
                for key, ptr, size in zip(keys, ptrs, sizes):
                    profile = store.get_into_profiled(key, ptr, size)
                    if int(profile["result"]) != size:
                        raise PerfError(f"GPU get_into failed for {key}: {profile}")
                    transfer_to_staging_ns += int(profile["transfer_to_staging_ns"])
                    staging_to_gpu_ns += int(profile["staging_to_gpu_ns"])
            else:
                if not hasattr(store, "batch_get_into_profiled"):
                    raise PerfError("store.so is missing batch_get_into_profiled; rebuild")
                for group_keys, group_ptrs, group_sizes in batch_groups:
                    profile = store.batch_get_into_profiled(
                        group_keys, group_ptrs, group_sizes)
                    results = list(profile["results"])
                    if any(int(got) != size for got, size
                           in zip(results, group_sizes)):
                        raise PerfError(
                            f"GPU batch_get_into failed for keys={group_keys}: "
                            f"{results}")
                    transfer_to_staging_ns += int(
                        profile["transfer_to_staging_ns"])
                    staging_to_gpu_ns += int(profile["staging_to_gpu_ns"])
            torch.cuda.synchronize()
        dt = time.monotonic() - t
        path = "rdma_gpu_direct" if gdr else "rdma_host_staged"
        out["gpu_path_selected"] = path
        total_operations = args.iterations * len(keys)
        result_name = f"to_gpu_{args.gpu_pattern}" + (
            "_gpudirect" if gdr else "_staged")
        out[result_name] = _rate(
            args.block_bytes * total_operations, dt, args.iterations)
        calls_per_iteration = (
            len(keys) if args.gpu_pattern == "single" else len(batch_groups))
        out[result_name]["api_calls"] = args.iterations * calls_per_iteration
        out[result_name]["api_calls_per_iteration"] = calls_per_iteration
        out[result_name]["objects_per_call"] = (
            1 if args.gpu_pattern == "single" else
            max(len(group[0]) for group in batch_groups))
        out[result_name]["batch_group_count"] = (
            0 if args.gpu_pattern == "single" else len(batch_groups))
        if not gdr:
            transfer_sec = transfer_to_staging_ns / 1e9
            scatter_sec = staging_to_gpu_ns / 1e9
            measured_sec = transfer_sec + scatter_sec
            measured_bytes = args.block_bytes * total_operations
            out["staged_breakdown"] = {
                "rdma_to_host": _rate(
                    measured_bytes, transfer_sec, args.iterations),
                "host_to_gpu": _rate(
                    measured_bytes, scatter_sec, args.iterations),
                "measured_phases_sec": round(measured_sec, 6),
                "unattributed_sec": round(max(0.0, dt - measured_sec), 6),
                "unattributed_note":
                    "Python/pybind overhead, metadata preparation, and final "
                    "torch.cuda.synchronize outside the two internal clocks",
            }
        return out
    finally:
        store.close()


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--role", choices=["server", "prep", "consumer", "cleanup"],
                   required=True)
    p.add_argument("--store-module", default="mooncake.store")
    p.add_argument("--local-hostname", required=True)
    p.add_argument("--master-server", required=True)
    p.add_argument("--metadata-server", default="http://127.0.0.1:8080/metadata")
    p.add_argument("--device-name", default="", help="RDMA device (blank = auto)")
    p.add_argument("--key", default="dram-rdma-blk")
    p.add_argument("--block-bytes", type=int, default=GiB, help="1..16 GiB KV block")
    p.add_argument("--object-count", type=int, default=1,
                   help="number of distinct keys/objects")
    p.add_argument("--source-endpoint", dest="source_endpoints", action="append",
                   default=[], help="deterministic target segment; repeat per key")
    p.add_argument("--publish-timeout-sec", type=float, default=30.0)
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
    p.add_argument("--gpu-pattern", choices=["single", "batch"], default="batch")
    p.add_argument("--batch-group-size", type=int, default=0,
                   help="objects per batch call; zero batches all keys together")
    p.add_argument("--iterations", type=int, default=1)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--summary-json", default="")
    args = p.parse_args(argv)
    if args.skip_dram:
        args.mode = "gpu"
    if args.iterations <= 0:
        p.error("--iterations must be positive")
    if args.object_count <= 0:
        p.error("--object-count must be positive")
    if args.publish_timeout_sec <= 0:
        p.error("--publish-timeout-sec must be positive")
    if args.batch_group_size < 0:
        p.error("--batch-group-size cannot be negative")
    if args.source_endpoints and len(args.source_endpoints) < args.object_count:
        p.error("provide at least one --source-endpoint per object")

    try:
        if args.role == "server":
            summary = run_server(args)
        elif args.role == "prep":
            summary = run_prep(args)
        elif args.role == "cleanup":
            summary = run_cleanup(args)
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
