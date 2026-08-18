#!/usr/bin/env python3
"""Two-node Mooncake Store correctness gate for one shared CXL pool.

The harness deliberately uses only the public Python Store API.  Mooncake
objects are also used as phase markers, so a PASS requires the real Master
metadata path as well as both clients' CXL mappings.  It does not emulate or
replace TraCT metadata.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import importlib
import io
import json
import os
import pathlib
import re
import sys
import tempfile
import time
from typing import Any, Iterable, Optional, Sequence


REQUIRED_OBJECT_SIZES = (4 * 1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024)
VALID_ROLES = ("node0", "node1")
DEFAULT_COMPONENT = "todo15_shared_cxl"
DEFAULT_TIER = "T2_SHARED_CXL"
# Backward-compatible names used by the TODO1.5 protocol tests.
COMPONENT = DEFAULT_COMPONENT
TIER = DEFAULT_TIER


class ConfigError(ValueError):
    """The test cannot start safely with the supplied topology."""


class ProtocolError(RuntimeError):
    """A Store operation or data-path invariant failed."""


class MarkerTimeout(TimeoutError):
    """The peer did not publish the expected Store marker in time."""


@dataclasses.dataclass(frozen=True)
class TestConfig:
    role: str
    run_id: str
    node_id: str
    pool_id: str
    device_name: str
    capacity: int
    local_hostname: str
    master_server: str
    component: str = DEFAULT_COMPONENT
    tier: str = DEFAULT_TIER
    metadata_server: str = "P2PHANDSHAKE"
    global_segment_size: int = 0
    local_buffer_size: int = 256 * 1024 * 1024
    mapping_offset: int = 0
    timeout_sec: float = 180.0
    poll_ms: int = 100
    object_sizes: tuple[int, ...] = REQUIRED_OBJECT_SIZES
    cleanup: bool = False
    summary_json: Optional[str] = None

    @property
    def run_token(self) -> str:
        return hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()[:16]

    def validate(self, require_lab_sizes: bool = True) -> None:
        if self.role not in VALID_ROLES:
            raise ConfigError(f"role must be one of {VALID_ROLES}, got {self.role!r}")
        for field_name in ("run_id", "node_id", "pool_id", "device_name"):
            value = getattr(self, field_name)
            if not value or any(ord(char) < 32 for char in value):
                raise ConfigError(f"{field_name} must be non-empty and printable")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,96}", self.run_id):
            raise ConfigError(
                "run_id must contain only letters, digits, '.', '_' or '-' "
                "and be at most 96 characters"
            )
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", self.component):
            raise ConfigError("component must be a portable 1-64 character token")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", self.tier):
            raise ConfigError("tier must be a portable 1-64 character token")
        _validate_endpoint("local_hostname", self.local_hostname)
        _validate_endpoint("master_server", self.master_server)
        if self.metadata_server != "P2PHANDSHAKE":
            raise ConfigError(
                "TODO1.5 requires metadata_server=P2PHANDSHAKE so peer CXL "
                "descriptors are resolved through the existing Mooncake path"
            )
        if self.global_segment_size != 0:
            raise ConfigError(
                "global_segment_size must be 0 for protocol=cxl; the backend "
                "supplies the mapped capacity"
            )
        if self.local_buffer_size <= 0:
            raise ConfigError("local_buffer_size must be positive")
        if self.capacity <= 0:
            raise ConfigError("capacity must be positive")
        if self.mapping_offset < 0:
            raise ConfigError("mapping_offset must be non-negative")
        if self.timeout_sec <= 0:
            raise ConfigError("timeout_sec must be positive")
        if self.poll_ms <= 0:
            raise ConfigError("poll_ms must be positive")
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
    if not host or not port_text.isdigit() or not 1 <= int(port_text) <= 65535:
        raise ConfigError(f"{name} must contain a non-empty host and valid port")


def validate_lab_environment(config: TestConfig) -> None:
    """Fail closed before touching a real shared CXL extent."""

    expected = {
        "MC_CXL_BACKEND_KIND": "devdax",
        "MC_CXL_POOL_ID": config.pool_id,
        "MC_CXL_DEV_PATH": config.device_name,
        "MC_CXL_MAP_OFFSET": str(config.mapping_offset),
    }
    for name, wanted in expected.items():
        actual = os.environ.get(name)
        if actual != wanted:
            raise ConfigError(f"{name} must be {wanted!r}, got {actual!r}")
    capacity = os.environ.get("MC_CXL_DEV_SIZE", "")
    if capacity != str(config.capacity):
        raise ConfigError(
            f"MC_CXL_DEV_SIZE must be {config.capacity!r}, got {capacity!r}"
        )
    if os.environ.get("MC_CXL_TEST_DESTRUCTIVE") != "1":
        raise ConfigError(
            "MC_CXL_TEST_DESTRUCTIVE=1 is required because this test writes "
            "and later removes objects in the selected DAX extent"
        )
    if os.environ.get("MOONCAKE_STORE_CHECKSUM") != "1":
        raise ConfigError("MOONCAKE_STORE_CHECKSUM=1 is required")


@dataclasses.dataclass(frozen=True)
class ObjectSpec:
    key: str
    direction: str
    api: str
    index: int
    size: int


def object_specs(config: TestConfig, direction: str, api: str) -> list[ObjectSpec]:
    if direction not in ("n0-n1", "n1-n0"):
        raise ValueError(f"invalid direction: {direction}")
    if api not in ("single", "batch"):
        raise ValueError(f"invalid api: {api}")
    return [
        ObjectSpec(
            key=(
                f"{config.component}-{config.run_token}-{direction}-{api}-{index}"
            ),
            direction=direction,
            api=api,
            index=index,
            size=size,
        )
        for index, size in enumerate(config.object_sizes)
    ]


def marker_key(config: TestConfig, marker: str) -> str:
    return f"{config.component}-{config.run_token}-marker-{marker}"


def marker_payload(config: TestConfig, marker: str) -> bytes:
    return json.dumps(
        {
            "component": config.component,
            "marker": marker,
            "pool_id": config.pool_id,
            "run_token": config.run_token,
            "mapping_offset": config.mapping_offset,
            "capacity": config.capacity,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def deterministic_payload(config: TestConfig, spec: ObjectSpec) -> bytes:
    seed_text = (
        f"{config.component}|{config.run_token}|{config.pool_id}|{spec.direction}|"
        f"{spec.api}|{spec.index}|{spec.size}"
    )
    seed = hashlib.sha256(seed_text.encode("utf-8")).digest()
    return (seed * ((spec.size + len(seed) - 1) // len(seed)))[: spec.size]


def checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class StatusRecorder:
    """Emit safe JSONL diagnostics and build one machine-readable summary."""

    def __init__(self, config: TestConfig, stream: Optional[io.TextIOBase] = None):
        self.config = config
        self.stream = stream if stream is not None else sys.stdout
        self.started_at = time.monotonic()
        self.event_count = 0
        self.objects_put = 0
        self.objects_get = 0
        self.bytes_put = 0
        self.bytes_get = 0
        self.checksums_verified = 0
        self.completed_phases: list[str] = []
        self.terminal_status = "RUNNING"
        self.failure_type: Optional[str] = None

    def emit(self, event: str, **fields: Any) -> None:
        record = {
            "component": self.config.component,
            "event": event,
            "node_id": self.config.node_id,
            "pool_id": self.config.pool_id,
            "mapping_offset": self.config.mapping_offset,
            "capacity": self.config.capacity,
            "role": self.config.role,
            "run_token": self.config.run_token,
            "tier": self.config.tier,
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        record.update(fields)
        print(json.dumps(record, sort_keys=True), file=self.stream, flush=True)
        self.event_count += 1

    def record_io(
        self,
        operation: str,
        phase: str,
        object_count: int,
        byte_count: int,
        elapsed_ms: float,
    ) -> None:
        if operation == "put":
            self.objects_put += object_count
            self.bytes_put += byte_count
        elif operation == "get":
            self.objects_get += object_count
            self.bytes_get += byte_count
            self.checksums_verified += object_count
        else:
            raise ValueError(f"invalid operation: {operation}")
        self.completed_phases.append(phase)
        self.emit(
            "io_complete",
            operation=operation,
            phase=phase,
            object_count=object_count,
            bytes=byte_count,
            elapsed_ms=round(elapsed_ms, 3),
            status="PASS",
        )

    def finish(self, status: str, failure_type: Optional[str] = None) -> dict[str, Any]:
        self.terminal_status = status
        self.failure_type = failure_type
        self.emit(
            "test_complete",
            status=status,
            failure_type=failure_type,
            elapsed_ms=round((time.monotonic() - self.started_at) * 1000, 3),
        )
        return self.summary()

    def summary(self) -> dict[str, Any]:
        return {
            "component": self.config.component,
            "role": self.config.role,
            "node_id": self.config.node_id,
            "pool_id": self.config.pool_id,
            "mapping_offset": self.config.mapping_offset,
            "capacity": self.config.capacity,
            "run_token": self.config.run_token,
            "tier": self.config.tier,
            "status": self.terminal_status,
            "failure_type": self.failure_type,
            "elapsed_ms": round((time.monotonic() - self.started_at) * 1000, 3),
            "event_count": self.event_count,
            "objects_put": self.objects_put,
            "objects_get": self.objects_get,
            "bytes_put": self.bytes_put,
            "bytes_get": self.bytes_get,
            "checksums_verified": self.checksums_verified,
            "completed_phases": list(self.completed_phases),
            "object_sizes": list(self.config.object_sizes),
            "checksum_algorithm": "sha256",
            "exact_byte_equality": True,
        }


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


def _require_zero(operation: str, result: Any) -> None:
    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
        codes = [int(code) for code in result]
        if not codes or any(code != 0 for code in codes):
            raise ProtocolError(f"{operation} failed with return codes {codes}")
        return
    try:
        code = int(result)
    except (TypeError, ValueError) as error:
        raise ProtocolError(f"{operation} returned invalid status {result!r}") from error
    if code != 0:
        raise ProtocolError(f"{operation} failed with return code {code}")


def _require_absent(store: Any, keys: Iterable[str], owner: str) -> None:
    for key in keys:
        result = int(store.is_exist(key))
        if result < 0:
            raise ProtocolError(f"is_exist failed for {key}: return code {result}")
        if result == 1:
            raise ConfigError(
                f"stale TODO1.5 key exists for {owner}: {key}; choose a new run_id "
                "or clean the previous run"
            )


def _put_marker(store: Any, config: TestConfig, replicate_config: Any, marker: str) -> None:
    result = store.put(
        marker_key(config, marker), marker_payload(config, marker), replicate_config
    )
    _require_zero(f"put marker {marker}", result)


def _wait_for_marker(
    store: Any, config: TestConfig, marker: str, recorder: StatusRecorder
) -> None:
    key = marker_key(config, marker)
    deadline = time.monotonic() + config.timeout_sec
    recorder.emit("marker_wait_begin", marker=marker, timeout_sec=config.timeout_sec)
    while time.monotonic() < deadline:
        exists = int(store.is_exist(key))
        if exists < 0:
            raise ProtocolError(f"is_exist failed while waiting for {marker}: {exists}")
        if exists == 1:
            actual = store.get(key)
            if actual is None:
                raise ProtocolError(f"marker {marker} exists but get returned no payload")
            actual_bytes = bytes(actual)
            expected = marker_payload(config, marker)
            if actual_bytes != expected:
                raise ProtocolError(
                    f"marker {marker} payload mismatch: expected_sha256="
                    f"{checksum(expected)} actual_sha256={checksum(actual_bytes)}"
                )
            recorder.emit("marker_observed", marker=marker, status="PASS")
            return
        time.sleep(config.poll_ms / 1000.0)
    raise MarkerTimeout(
        f"timed out after {config.timeout_sec}s waiting for Store marker {marker}"
    )


def _wait_for_marker_removal(
    store: Any, config: TestConfig, marker: str, recorder: StatusRecorder
) -> None:
    key = marker_key(config, marker)
    deadline = time.monotonic() + config.timeout_sec
    recorder.emit(
        "marker_removal_wait_begin", marker=marker, timeout_sec=config.timeout_sec
    )
    while time.monotonic() < deadline:
        exists = int(store.is_exist(key))
        if exists < 0:
            raise ProtocolError(
                f"is_exist failed while waiting for removal of {marker}: {exists}"
            )
        if exists == 0:
            recorder.emit("marker_removed", marker=marker, status="PASS")
            return
        time.sleep(config.poll_ms / 1000.0)
    raise MarkerTimeout(
        f"timed out after {config.timeout_sec}s waiting for Store marker "
        f"{marker} to be removed"
    )


def _put_single(
    store: Any,
    config: TestConfig,
    replicate_config: Any,
    recorder: StatusRecorder,
    direction: str,
) -> None:
    specs = object_specs(config, direction, "single")
    started = time.monotonic()
    for spec in specs:
        payload = deterministic_payload(config, spec)
        result = store.put(spec.key, payload, replicate_config)
        _require_zero(f"put {spec.key}", result)
        recorder.emit(
            "object_put",
            api="single",
            direction=direction,
            key=spec.key,
            bytes=spec.size,
            checksum=checksum(payload),
            status="PASS",
        )
    recorder.record_io(
        "put",
        f"{direction}.single.put",
        len(specs),
        sum(spec.size for spec in specs),
        (time.monotonic() - started) * 1000,
    )


def _put_batch(
    store: Any,
    config: TestConfig,
    replicate_config: Any,
    recorder: StatusRecorder,
    direction: str,
) -> None:
    specs = object_specs(config, direction, "batch")
    payloads = [deterministic_payload(config, spec) for spec in specs]
    started = time.monotonic()
    result = store.put_batch(
        [spec.key for spec in specs], payloads, replicate_config
    )
    _require_zero(f"put_batch {direction}", result)
    for spec, payload in zip(specs, payloads):
        recorder.emit(
            "object_put",
            api="batch",
            direction=direction,
            key=spec.key,
            bytes=spec.size,
            checksum=checksum(payload),
            status="PASS",
        )
    recorder.record_io(
        "put",
        f"{direction}.batch.put",
        len(specs),
        sum(spec.size for spec in specs),
        (time.monotonic() - started) * 1000,
    )


def _verify_payload(config: TestConfig, spec: ObjectSpec, actual: Any) -> bytes:
    if actual is None:
        raise ProtocolError(f"get miss for {spec.key}")
    actual_bytes = bytes(actual)
    expected = deterministic_payload(config, spec)
    if len(actual_bytes) != spec.size:
        raise ProtocolError(
            f"size mismatch for {spec.key}: expected={spec.size} "
            f"actual={len(actual_bytes)}"
        )
    actual_checksum = checksum(actual_bytes)
    expected_checksum = checksum(expected)
    if actual_checksum != expected_checksum:
        raise ProtocolError(
            f"checksum mismatch for {spec.key}: expected_sha256={expected_checksum} "
            f"actual_sha256={actual_checksum}"
        )
    if actual_bytes != expected:
        raise ProtocolError(
            f"exact byte mismatch for {spec.key} despite checksum comparison"
        )
    return actual_bytes


def _get_single(
    store: Any, config: TestConfig, recorder: StatusRecorder, direction: str
) -> None:
    specs = object_specs(config, direction, "single")
    started = time.monotonic()
    for spec in specs:
        actual = _verify_payload(config, spec, store.get(spec.key))
        recorder.emit(
            "object_verified",
            api="single",
            direction=direction,
            key=spec.key,
            bytes=len(actual),
            checksum=checksum(actual),
            exact_equal=True,
            status="PASS",
        )
    recorder.record_io(
        "get",
        f"{direction}.single.get",
        len(specs),
        sum(spec.size for spec in specs),
        (time.monotonic() - started) * 1000,
    )


def _get_batch(
    store: Any, config: TestConfig, recorder: StatusRecorder, direction: str
) -> None:
    specs = object_specs(config, direction, "batch")
    started = time.monotonic()
    results = list(store.get_batch([spec.key for spec in specs]))
    if len(results) != len(specs):
        raise ProtocolError(
            f"get_batch {direction} returned {len(results)} objects, "
            f"expected {len(specs)}"
        )
    for spec, result in zip(specs, results):
        actual = _verify_payload(config, spec, result)
        recorder.emit(
            "object_verified",
            api="batch",
            direction=direction,
            key=spec.key,
            bytes=len(actual),
            checksum=checksum(actual),
            exact_equal=True,
            status="PASS",
        )
    recorder.record_io(
        "get",
        f"{direction}.batch.get",
        len(specs),
        sum(spec.size for spec in specs),
        (time.monotonic() - started) * 1000,
    )


def _authored_keys(config: TestConfig, role: str) -> list[str]:
    if role == "node0":
        direction = "n0-n1"
        markers = ("n0_ready", "n0_verified", "n0_done")
    else:
        direction = "n1-n0"
        markers = ("n1_verified", "n1_ready", "n1_done")
    keys = [
        spec.key
        for api in ("single", "batch")
        for spec in object_specs(config, direction, api)
    ]
    keys.extend(marker_key(config, marker) for marker in markers)
    return keys


def all_test_keys(config: TestConfig) -> list[str]:
    return _authored_keys(config, "node0") + _authored_keys(config, "node1")


def _cleanup_namespace(store: Any, config: TestConfig, recorder: StatusRecorder) -> None:
    final_marker = marker_key(config, "n0_done")
    cleanup_keys = [key for key in all_test_keys(config) if key != final_marker]
    for key in cleanup_keys:
        result = store.remove(key, True)
        _require_zero(f"remove {key}", result)
    recorder.emit(
        "namespace_cleanup", object_count=len(cleanup_keys), status="PASS"
    )


def run_role(
    store: Any, config: TestConfig, replicate_config: Any, recorder: StatusRecorder
) -> None:
    """Run one side of the two-node protocol against a Store-like object."""

    config.validate(require_lab_sizes=False)
    _require_absent(store, _authored_keys(config, config.role), config.role)
    recorder.emit(
        "preflight_complete",
        metadata_server=config.metadata_server,
        master_server=config.master_server,
        object_sizes=list(config.object_sizes),
        status="PASS",
    )

    if config.role == "node0":
        _put_single(store, config, replicate_config, recorder, "n0-n1")
        _put_batch(store, config, replicate_config, recorder, "n0-n1")
        _put_marker(store, config, replicate_config, "n0_ready")
        recorder.emit("marker_published", marker="n0_ready", status="PASS")

        _wait_for_marker(store, config, "n1_verified", recorder)
        _wait_for_marker(store, config, "n1_ready", recorder)
        _get_single(store, config, recorder, "n1-n0")
        _get_batch(store, config, recorder, "n1-n0")
        _put_marker(store, config, replicate_config, "n0_verified")
        recorder.emit("marker_published", marker="n0_verified", status="PASS")
        _wait_for_marker(store, config, "n1_done", recorder)
        if config.cleanup:
            _cleanup_namespace(store, config, recorder)
        _put_marker(store, config, replicate_config, "n0_done")
        recorder.emit("marker_published", marker="n0_done", status="PASS")
        _wait_for_marker_removal(store, config, "n0_done", recorder)
        return

    _wait_for_marker(store, config, "n0_ready", recorder)
    _get_single(store, config, recorder, "n0-n1")
    _get_batch(store, config, recorder, "n0-n1")
    _put_marker(store, config, replicate_config, "n1_verified")
    recorder.emit("marker_published", marker="n1_verified", status="PASS")

    _put_single(store, config, replicate_config, recorder, "n1-n0")
    _put_batch(store, config, replicate_config, recorder, "n1-n0")
    _put_marker(store, config, replicate_config, "n1_ready")
    recorder.emit("marker_published", marker="n1_ready", status="PASS")
    _wait_for_marker(store, config, "n0_verified", recorder)
    _put_marker(store, config, replicate_config, "n1_done")
    recorder.emit("marker_published", marker="n1_done", status="PASS")
    _wait_for_marker(store, config, "n0_done", recorder)
    remove_result = store.remove(marker_key(config, "n0_done"), True)
    _require_zero("remove marker n0_done", remove_result)
    recorder.emit("marker_acknowledged", marker="n0_done", status="PASS")


def _load_store_module(module_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except Exception as error:
        raise ConfigError(
            f"cannot import {module_name!r}: {error}; build/stage the TODO1 wheel "
            "or set MOONCAKE_STORE_MODULE"
        ) from error
    for symbol in ("MooncakeDistributedStore", "ReplicateConfig"):
        if not hasattr(module, symbol):
            raise ConfigError(f"{module_name!r} does not expose {symbol}")
    return module


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=VALID_ROLES)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--pool-id", required=True)
    parser.add_argument("--device-name", required=True)
    parser.add_argument("--capacity", required=True, type=int)
    parser.add_argument("--local-hostname", required=True)
    parser.add_argument("--master-server", required=True)
    parser.add_argument("--component", default=DEFAULT_COMPONENT)
    parser.add_argument("--tier", default=DEFAULT_TIER)
    parser.add_argument("--metadata-server", default="P2PHANDSHAKE")
    parser.add_argument("--global-segment-size", type=int, default=0)
    parser.add_argument("--local-buffer-size", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--mapping-offset", type=int, default=0)
    parser.add_argument("--timeout-sec", type=float, default=180.0)
    parser.add_argument("--poll-ms", type=int, default=100)
    parser.add_argument("--summary-json")
    parser.add_argument("--cleanup", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    config = TestConfig(
        role=args.role,
        run_id=args.run_id,
        node_id=args.node_id,
        pool_id=args.pool_id,
        device_name=args.device_name,
        capacity=args.capacity,
        local_hostname=args.local_hostname,
        master_server=args.master_server,
        component=args.component,
        tier=args.tier,
        metadata_server=args.metadata_server,
        global_segment_size=args.global_segment_size,
        local_buffer_size=args.local_buffer_size,
        mapping_offset=args.mapping_offset,
        timeout_sec=args.timeout_sec,
        poll_ms=args.poll_ms,
        cleanup=args.cleanup,
        summary_json=args.summary_json,
    )
    recorder = StatusRecorder(config)
    store = None
    summary: dict[str, Any]
    try:
        config.validate(require_lab_sizes=True)
        validate_lab_environment(config)
        recorder.emit(
            "process_start",
            backend_kind="devdax",
            checksum_algorithm="sha256",
            store_checksum=True,
        )
        module_name = os.environ.get("MOONCAKE_STORE_MODULE", "mooncake.store")
        module = _load_store_module(module_name)
        store = module.MooncakeDistributedStore()
        setup_result = store.setup(
            config.local_hostname,
            config.metadata_server,
            config.global_segment_size,
            config.local_buffer_size,
            "cxl",
            config.device_name,
            config.master_server,
        )
        _require_zero("MooncakeDistributedStore.setup", setup_result)
        health = int(store.health_check())
        if health != 0:
            raise ProtocolError(f"Mooncake Store health_check failed: {health}")
        recorder.emit("store_ready", protocol="cxl", status="PASS")

        replicate_config = module.ReplicateConfig()
        replicate_config.replica_num = 1
        replicate_config.nof_replica_num = 0
        run_role(store, config, replicate_config, recorder)
        close_result = store.close()
        store = None
        _require_zero("MooncakeDistributedStore.close", close_result)
        recorder.emit("store_closed", status="PASS")
        summary = recorder.finish("PASS")
        write_summary(config.summary_json, summary)
        return 0
    except Exception as error:
        # Preserve the concrete exception type while turning both expected
        # protocol failures and unexpected binding errors into structured FAIL.
        failure_type = type(error).__name__
        if store is not None:
            try:
                close_result = store.close()
                recorder.emit(
                    "store_closed",
                    return_code=int(close_result),
                    status="PASS" if int(close_result) == 0 else "FAIL",
                )
            except Exception as close_error:
                recorder.emit(
                    "store_close_failure",
                    failure_type=type(close_error).__name__,
                    message=str(close_error)[:1000],
                    status="FAIL",
                )
            store = None
        recorder.emit(
            "test_failure",
            failure_type=failure_type,
            message=str(error)[:1000],
            status="FAIL",
        )
        summary = recorder.finish("FAIL", failure_type=failure_type)
        write_summary(config.summary_json, summary)
        print(f"[FAIL] {failure_type}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
