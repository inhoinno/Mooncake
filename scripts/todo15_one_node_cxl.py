#!/usr/bin/env python3
"""One-node native Mooncake Store CXL PUT/GET correctness gate.

This harness talks only to the public ``mooncake.store`` Python binding.  The
launcher starts the real Mooncake Master and selects the built-in ``mooncake``
CXL provider, so a PASS covers Master allocation/publication and local
CxlTransport offset resolution.  It never prints payload bytes or raw mapping
addresses.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import importlib
import json
import os
import pathlib
import re
import tempfile
import time
from typing import Any, Optional, Sequence


# MasterClient::PutStart sends the total object length to MasterService, whose
# CacheLib guard rejects values above kMaxSliceSize (Slab::kSize - 16).  Keep a
# page of headroom instead of pretending that an exact 16 MiB object passes.
REQUIRED_OBJECT_SIZES = (
    4 * 1024,
    64 * 1024,
    1024 * 1024,
    16 * 1024 * 1024 - 4096,
)
SLAB_SIZE = 16 * 1024 * 1024
MIN_POOL_CAPACITY = 4 * SLAB_SIZE


class ConfigError(ValueError):
    """The native CXL test cannot safely start with the supplied settings."""


class ProtocolError(RuntimeError):
    """A Store operation or data-integrity invariant failed."""


@dataclasses.dataclass(frozen=True)
class TestConfig:
    run_id: str
    pool_id: str
    device_name: str
    capacity: int
    local_hostname: str
    master_server: str
    metadata_server: str = "P2PHANDSHAKE"
    global_segment_size: int = 0
    local_buffer_size: int = 256 * 1024 * 1024
    mapping_offset: int = 0
    owned_offset: int = 0
    owned_capacity: int = 0
    object_sizes: tuple[int, ...] = REQUIRED_OBJECT_SIZES
    summary_json: Optional[str] = None

    @property
    def run_token(self) -> str:
        return hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()[:16]

    def validate(self, require_lab_sizes: bool = True) -> None:
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,96}", self.run_id):
            raise ConfigError(
                "run_id must contain only letters, digits, '.', '_' or '-' "
                "and be at most 96 characters"
            )
        for field_name in ("pool_id", "device_name"):
            value = getattr(self, field_name)
            if not value or any(ord(char) < 32 for char in value):
                raise ConfigError(f"{field_name} must be non-empty and printable")
        _validate_endpoint("local_hostname", self.local_hostname)
        _validate_endpoint("master_server", self.master_server)
        if self.local_hostname == self.master_server:
            raise ConfigError("local_hostname must differ from the Master endpoint")
        if self.metadata_server != "P2PHANDSHAKE":
            raise ConfigError("metadata_server must be P2PHANDSHAKE")
        if self.global_segment_size != 0:
            raise ConfigError("global_segment_size must be 0 for protocol=cxl")
        if self.local_buffer_size <= 0:
            raise ConfigError("local_buffer_size must be positive")
        if self.capacity <= 0 or self.capacity % SLAB_SIZE != 0:
            raise ConfigError("capacity must be positive and 16 MiB aligned")
        if self.capacity < MIN_POOL_CAPACITY:
            raise ConfigError(
                "capacity must be at least 64 MiB for the four allocation classes"
            )
        if self.mapping_offset != 0:
            raise ConfigError("the one-node baseline requires mapping_offset=0")
        if self.owned_offset != 0:
            raise ConfigError("the one-node baseline requires owned_offset=0")
        if self.owned_capacity != self.capacity:
            raise ConfigError(
                "the one-node baseline requires owned_capacity == mapped capacity"
            )
        if self.owned_capacity % SLAB_SIZE != 0:
            raise ConfigError("owned_capacity must be 16 MiB aligned")
        if not self.object_sizes or any(size <= 0 for size in self.object_sizes):
            raise ConfigError("all object sizes must be positive")
        if len(set(self.object_sizes)) != len(self.object_sizes):
            raise ConfigError("object sizes must be unique")
        if require_lab_sizes and self.object_sizes != REQUIRED_OBJECT_SIZES:
            raise ConfigError(
                f"lab sizes must be exactly {REQUIRED_OBJECT_SIZES}, "
                f"got {self.object_sizes}"
            )


def _validate_endpoint(name: str, endpoint: str) -> None:
    if not endpoint or ":" not in endpoint:
        raise ConfigError(f"{name} must be host:port")
    host, port_text = endpoint.rsplit(":", 1)
    if not host or not port_text.isdecimal():
        raise ConfigError(f"{name} must be host:port")
    port = int(port_text)
    if port <= 0 or port > 65535:
        raise ConfigError(f"{name} port must be in [1, 65535]")


def validate_lab_environment(config: TestConfig) -> None:
    expected = {
        "MC_CXL_PROVIDER": "mooncake",
        "MC_CXL_BACKEND_KIND": "devdax",
        "MC_CXL_POOL_ID": config.pool_id,
        "MC_CXL_DEV_PATH": config.device_name,
        "MC_CXL_DEV_SIZE": str(config.capacity),
        "MC_CXL_MAP_OFFSET": str(config.mapping_offset),
        "MC_CXL_OWNED_OFFSET": str(config.owned_offset),
        "MC_CXL_OWNED_SIZE": str(config.owned_capacity),
        "MC_CXL_TEST_DESTRUCTIVE": "1",
        "MOONCAKE_STORE_CHECKSUM": "1",
    }
    for name, expected_value in expected.items():
        actual = os.environ.get(name)
        if actual != expected_value:
            raise ConfigError(
                f"{name} must equal {expected_value!r}, got {actual!r}"
            )
    if not config.device_name.startswith("/dev/dax"):
        raise ConfigError("the T2 native gate requires an explicit /dev/dax device")


def object_key(config: TestConfig, api: str, size: int) -> str:
    return f"todo15-one-{config.run_token}-{api}-{size}"


def deterministic_payload(config: TestConfig, api: str, size: int) -> bytes:
    seed = hashlib.sha256(
        f"{config.run_token}:{api}:{size}".encode("utf-8")
    ).digest()
    return (seed * ((size + len(seed) - 1) // len(seed)))[:size]


def checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _require_zero(operation: str, result: Any) -> None:
    if isinstance(result, Sequence) and not isinstance(
        result, (str, bytes, bytearray)
    ):
        codes = [int(code) for code in result]
        if not codes or any(code != 0 for code in codes):
            raise ProtocolError(f"{operation} failed with return codes {codes}")
        return
    try:
        code = int(result)
    except (TypeError, ValueError) as error:
        raise ProtocolError(
            f"{operation} returned invalid status {result!r}"
        ) from error
    if code != 0:
        raise ProtocolError(f"{operation} failed with return code {code}")


def _verify_payload(operation: str, actual: Any, expected: bytes) -> None:
    if actual is None:
        raise ProtocolError(f"{operation} returned a cache miss")
    actual_bytes = bytes(actual)
    if len(actual_bytes) != len(expected):
        raise ProtocolError(
            f"{operation} length mismatch: expected={len(expected)} "
            f"actual={len(actual_bytes)}"
        )
    if checksum(actual_bytes) != checksum(expected) or actual_bytes != expected:
        raise ProtocolError(
            f"{operation} checksum mismatch: expected_sha256={checksum(expected)} "
            f"actual_sha256={checksum(actual_bytes)}"
        )


def _emit(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def write_summary(path: Optional[str], summary: dict[str, Any]) -> None:
    if not path:
        return
    destination = pathlib.Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        delete=False,
    ) as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = pathlib.Path(handle.name)
    os.replace(temporary, destination)


def run_matrix(store: Any, config: TestConfig, replicate_config: Any) -> dict[str, Any]:
    keys = [
        object_key(config, api, size)
        for api in ("single", "batch")
        for size in config.object_sizes
    ]
    for key in keys:
        exists = int(store.is_exist(key))
        if exists < 0:
            raise ProtocolError(f"preflight is_exist failed for {key}: {exists}")
        if exists == 1:
            raise ConfigError(
                f"stale test key exists: {key}; choose a new TODO15_ONE_RUN_ID"
            )

    started = time.monotonic()
    inserted: list[str] = []
    puts = 0
    gets = 0
    checksums_verified = 0
    bytes_written = 0
    bytes_read = 0
    removed = 0
    try:
        for size in config.object_sizes:
            key = object_key(config, "single", size)
            payload = deterministic_payload(config, "single", size)
            _require_zero(f"put {key}", store.put(key, payload, replicate_config))
            inserted.append(key)
            puts += 1
            bytes_written += size
            _verify_payload(f"get {key}", store.get(key), payload)
            gets += 1
            bytes_read += size
            checksums_verified += 1
            _emit(
                "single_put_get",
                key=key,
                bytes=size,
                sha256=checksum(payload),
                status="PASS",
            )

        batch_keys = [object_key(config, "batch", size) for size in config.object_sizes]
        batch_values = [
            deterministic_payload(config, "batch", size)
            for size in config.object_sizes
        ]
        _require_zero(
            "put_batch",
            store.put_batch(batch_keys, batch_values, replicate_config),
        )
        inserted.extend(batch_keys)
        puts += len(batch_keys)
        bytes_written += sum(config.object_sizes)
        actual_values = list(store.get_batch(batch_keys))
        if len(actual_values) != len(batch_values):
            raise ProtocolError(
                f"get_batch result count mismatch: expected={len(batch_values)} "
                f"actual={len(actual_values)}"
            )
        for key, size, actual, expected in zip(
            batch_keys, config.object_sizes, actual_values, batch_values
        ):
            _verify_payload(f"get_batch {key}", actual, expected)
            gets += 1
            bytes_read += size
            checksums_verified += 1
            _emit(
                "batch_put_get",
                key=key,
                bytes=size,
                sha256=checksum(expected),
                status="PASS",
            )
    finally:
        for key in reversed(inserted):
            try:
                result = store.remove(key, True)
                _require_zero(f"remove {key}", result)
                if int(store.is_exist(key)) != 0:
                    raise ProtocolError(f"removed key remains visible: {key}")
                removed += 1
            except Exception as error:
                _emit(
                    "cleanup_failure",
                    key=key,
                    failure_type=type(error).__name__,
                    message=str(error)[:500],
                    status="FAIL",
                )
                raise

    return {
        "schema_version": 1,
        "milestone": "TODO1.5-one-node-native-gate",
        "tier": "T2_NATIVE_CXL",
        "status": "PASS",
        "provider": "mooncake",
        "backend_kind": "devdax",
        "pool_id": config.pool_id,
        "run_token": config.run_token,
        "object_sizes_bytes": list(config.object_sizes),
        "large_object_limit": "16MiB-4KiB (current kMaxSliceSize constraint)",
        "apis": ["put/get", "put_batch/get_batch"],
        "objects_put": puts,
        "objects_get": gets,
        "checksums_verified": checksums_verified,
        "bytes_written": bytes_written,
        "bytes_read": bytes_read,
        "objects_removed": removed,
        "exact_byte_equality": True,
        "checksum_algorithm": "sha256",
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
    }


def _load_store_module(module_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except Exception as error:
        raise ConfigError(
            f"cannot import {module_name!r}: {error}; build/stage the TODO1 wheel"
        ) from error
    for symbol in ("MooncakeDistributedStore", "ReplicateConfig"):
        if not hasattr(module, symbol):
            raise ConfigError(f"{module_name!r} does not expose {symbol}")
    return module


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--pool-id", required=True)
    parser.add_argument("--device-name", required=True)
    parser.add_argument("--capacity", required=True, type=int)
    parser.add_argument("--local-hostname", required=True)
    parser.add_argument("--master-server", required=True)
    parser.add_argument("--metadata-server", default="P2PHANDSHAKE")
    parser.add_argument("--global-segment-size", type=int, default=0)
    parser.add_argument("--local-buffer-size", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--mapping-offset", type=int, default=0)
    parser.add_argument("--owned-offset", type=int, default=0)
    parser.add_argument("--owned-capacity", required=True, type=int)
    parser.add_argument("--summary-json")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    config = TestConfig(
        run_id=args.run_id,
        pool_id=args.pool_id,
        device_name=args.device_name,
        capacity=args.capacity,
        local_hostname=args.local_hostname,
        master_server=args.master_server,
        metadata_server=args.metadata_server,
        global_segment_size=args.global_segment_size,
        local_buffer_size=args.local_buffer_size,
        mapping_offset=args.mapping_offset,
        owned_offset=args.owned_offset,
        owned_capacity=args.owned_capacity,
        summary_json=args.summary_json,
    )
    store = None
    try:
        config.validate(require_lab_sizes=True)
        validate_lab_environment(config)
        module_name = os.environ.get("MOONCAKE_STORE_MODULE", "mooncake.store")
        module = _load_store_module(module_name)
        store = module.MooncakeDistributedStore()
        _require_zero(
            "MooncakeDistributedStore.setup",
            store.setup(
                config.local_hostname,
                config.metadata_server,
                config.global_segment_size,
                config.local_buffer_size,
                "cxl",
                config.device_name,
                config.master_server,
            ),
        )
        health = int(store.health_check())
        if health != 0:
            raise ProtocolError(f"Mooncake Store health_check failed: {health}")
        _emit("store_ready", protocol="cxl", status="PASS")

        replicate_config = module.ReplicateConfig()
        replicate_config.replica_num = 1
        replicate_config.nof_replica_num = 0
        summary = run_matrix(store, config, replicate_config)
        _require_zero("MooncakeDistributedStore.close", store.close())
        store = None
        write_summary(config.summary_json, summary)
        _emit("test_complete", **summary)
        return 0
    except Exception as error:
        if store is not None:
            try:
                store.close()
            except Exception:
                pass
        summary = {
            "schema_version": 1,
            "milestone": "TODO1.5-one-node-native-gate",
            "tier": "T2_NATIVE_CXL",
            "status": "FAIL",
            "pool_id": config.pool_id,
            "run_token": config.run_token,
            "failure_type": type(error).__name__,
            "message": str(error)[:1000],
        }
        write_summary(config.summary_json, summary)
        _emit("test_complete", **summary)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
