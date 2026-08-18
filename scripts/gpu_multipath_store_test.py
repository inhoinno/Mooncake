#!/usr/bin/env python3
"""Hardware-gated Mooncake Store CXL/RDMA-to-GPU acceptance test.

The functional case places one object on a named CXL segment and one on a
named RDMA segment, then fetches both into CUDA tensors with one
``batch_get_into`` call. Set ``MC_STORE_TRACE_GPU_TRANSFERS=1`` on the client
to obtain the corresponding safe path records from Mooncake logs.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import pathlib
import sys
import time
from typing import Any, Sequence


CASES = ("preflight", "functional", "failure", "status")


class TestFailure(RuntimeError):
    pass


def _payload(label: str, size: int) -> bytes:
    seed = hashlib.sha256(label.encode("utf-8")).digest()
    return (seed * ((size + len(seed) - 1) // len(seed)))[:size]


def _descriptor_protocols(store: Any, key: str) -> list[str]:
    protocols: list[str] = []
    for replica in store.get_replica_desc(key):
        if replica.is_memory_replica():
            protocols.append(replica.get_memory_descriptor().buffer_descriptor.protocol)
    return protocols


def _make_config(module: Any, segment: str) -> Any:
    config = module.ReplicateConfig()
    config.replica_num = 1
    config.nof_replica_num = 0
    config.preferred_segments = [segment]
    return config


def _validate_preflight(args: argparse.Namespace, config: dict[str, Any]) -> None:
    if not args.cxl_segment or not args.rdma_segment:
        raise TestFailure("both --cxl-segment and --rdma-segment are required")
    if args.cxl_segment == args.rdma_segment:
        raise TestFailure("CXL and RDMA segment names must be different")
    if args.object_bytes <= 0:
        raise TestFailure("--object-bytes must be positive")
    if config.get("protocol") not in ("rdma", "efa", "cxi"):
        raise TestFailure(
            "consumer config protocol must be rdma, efa, or cxi so its "
            "registered host pool can stage network reads"
        )
    if not os.environ.get("MC_CXL_DEV_PATH"):
        raise TestFailure(
            "MC_CXL_DEV_PATH is required so the consumer Transfer Engine "
            "installs CxlTransport alongside the network transport"
        )
    local_buffer_size = config.get("local_buffer_size")
    if not isinstance(local_buffer_size, int) or local_buffer_size < 2 * args.object_bytes:
        raise TestFailure(
            "local_buffer_size must be an integer at least twice --object-bytes"
        )

    module = importlib.import_module(args.store_module)
    for symbol in ("MooncakeDistributedStore", "ReplicateConfig"):
        if not hasattr(module, symbol):
            raise TestFailure(f"{args.store_module} is missing {symbol}")
    torch = importlib.import_module("torch")
    if not torch.cuda.is_available():
        raise TestFailure("torch.cuda.is_available() is false")


def _open_store(args: argparse.Namespace, config: dict[str, Any]) -> tuple[Any, Any]:
    module = importlib.import_module(args.store_module)
    store = module.MooncakeDistributedStore()
    result = int(store.setup(config))
    if result != 0:
        store.close()
        raise TestFailure(f"MooncakeDistributedStore.setup failed: {result}")
    health = int(store.health_check())
    if health != 0:
        store.close()
        raise TestFailure(f"Mooncake Store health check failed: {health}")
    return module, store


def _place_objects(
    args: argparse.Namespace, module: Any, store: Any
) -> tuple[list[str], list[bytes], list[list[str]]]:
    token = hashlib.sha256(
        f"{os.getpid()}-{time.time_ns()}".encode("utf-8")
    ).hexdigest()[:16]
    keys = [f"gpu-multipath-{token}-cxl", f"gpu-multipath-{token}-rdma"]
    values = [
        _payload("cxl-" + token, args.object_bytes),
        _payload("rdma-" + token, args.object_bytes),
    ]
    segments = [args.cxl_segment, args.rdma_segment]
    for key, value, segment in zip(keys, values, segments):
        result = int(store.put(key, value, _make_config(module, segment)))
        if result != 0:
            raise TestFailure(f"put to preferred segment failed: {result}")

    protocols = [_descriptor_protocols(store, key) for key in keys]
    if "cxl" not in protocols[0]:
        raise TestFailure(f"CXL object protocols are {protocols[0]!r}")
    if not any(protocol in ("rdma", "efa", "cxi") for protocol in protocols[1]):
        raise TestFailure(f"network object protocols are {protocols[1]!r}")
    return keys, values, protocols


def _fetch_to_gpu(
    args: argparse.Namespace, store: Any, keys: list[str], values: list[bytes]
) -> tuple[list[int], float]:
    torch = importlib.import_module("torch")
    outputs = [
        torch.empty(args.object_bytes, dtype=torch.uint8, device="cuda")
        for _ in keys
    ]
    started = time.monotonic()
    results = [
        int(value)
        for value in store.batch_get_into(
            keys, [tensor.data_ptr() for tensor in outputs], [args.object_bytes] * 2
        )
    ]
    torch.cuda.synchronize()
    elapsed_ms = (time.monotonic() - started) * 1000.0
    if results != [args.object_bytes, args.object_bytes]:
        raise TestFailure(f"batch_get_into results are {results!r}")
    for expected, observed in zip(values, outputs):
        actual = observed.cpu().numpy().tobytes()
        if hashlib.sha256(actual).digest() != hashlib.sha256(expected).digest():
            raise TestFailure("GPU payload checksum mismatch")
    return results, elapsed_ms


def _run(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    _validate_preflight(args, config)
    if args.case == "preflight":
        return {"case": args.case, "status": "PASS", "tier": "T1_HARDWARE"}

    module, store = _open_store(args, config)
    keys: list[str] = []
    try:
        keys, values, protocols = _place_objects(args, module, store)
        if args.case == "status":
            return {
                "case": args.case,
                "status": "PASS",
                "tier": "T1_HARDWARE",
                "protocols": protocols,
            }
        if args.case == "failure":
            torch = importlib.import_module("torch")
            first = torch.full(
                (args.object_bytes,), 0xA5, dtype=torch.uint8, device="cuda"
            )
            second = torch.empty(args.object_bytes, dtype=torch.uint8, device="cuda")
            results = [
                int(value)
                for value in store.batch_get_into(
                    keys,
                    [first.data_ptr(), second.data_ptr()],
                    [args.object_bytes - 1, args.object_bytes],
                )
            ]
            torch.cuda.synchronize()
            if results[0] >= 0 or results[1] != args.object_bytes:
                raise TestFailure(f"failure isolation results are {results!r}")
            if not bool(torch.all(first == 0xA5).item()):
                raise TestFailure("undersized destination was modified")
            return {
                "case": args.case,
                "status": "PASS",
                "tier": "T1_HARDWARE",
                "results": results,
            }

        results, elapsed_ms = _fetch_to_gpu(args, store, keys, values)
        return {
            "case": args.case,
            "status": "PASS",
            "tier": "T1_HARDWARE",
            "bytes": args.object_bytes * 2,
            "elapsed_ms": round(elapsed_ms, 3),
            "protocols": protocols,
            "results": results,
        }
    finally:
        for key in keys:
            try:
                store.remove(key, True)
            except Exception:
                pass
        store.close()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=CASES, required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--cxl-segment", required=True)
    parser.add_argument("--rdma-segment", required=True)
    parser.add_argument("--object-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--store-module", default="mooncake.store")
    parser.add_argument("--summary-json")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        config = json.loads(pathlib.Path(args.config).read_text(encoding="utf-8"))
        if not isinstance(config, dict):
            raise TestFailure("--config must contain one JSON object")
        summary = _run(args, config)
        exit_code = 0
    except Exception as error:
        summary = {
            "case": args.case,
            "status": "FAIL",
            "failure_type": type(error).__name__,
            "message": str(error)[:1000],
        }
        exit_code = 1
    encoded = json.dumps(summary, sort_keys=True)
    print(encoded, flush=True)
    if args.summary_json:
        pathlib.Path(args.summary_json).write_text(encoded + "\n", encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
