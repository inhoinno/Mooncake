#!/usr/bin/env python3
"""Bounded-memory two-node Mooncake CXL working-set correctness gate.

Node 0 issues only ``MooncakeDistributedStore.put`` calls and keeps the whole
working set live.  After the final object is published, Node 1 issues only
``get`` calls and validates length, SHA-256, and exact bytes.  The harness
materializes at most one object payload at a time; a 500 GiB run therefore
does not require a 500 GiB DRAM buffer in either process.
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
from collections.abc import Iterator, Sequence
from typing import Any, Optional


OBJECT_SIZES = (4 * 1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024)
DEFAULT_WSS_BYTES = 500 * 1024**3
DEFAULT_HEADROOM_BYTES = 8 * 1024**3
DEFAULT_PROGRESS_BYTES = 1024**3
VALID_ROLES = ("node0", "node1")
COMPONENT = "todo2_cxl_wss"
TIER = "T2_NATIVE_SHARED_CXL_WSS"
PLAN_VERSION = 1
SLAB_BYTES = 16 * 1024 * 1024


class ConfigError(ValueError):
    """The test cannot safely run with the supplied configuration."""


class ProtocolError(RuntimeError):
    """A Store call or payload invariant failed."""


class MarkerTimeout(TimeoutError):
    """The peer did not finish the expected phase before the deadline."""


class PeerFailure(RuntimeError):
    """The peer published a structured failure marker."""


@dataclasses.dataclass(frozen=True)
class WssPlan:
    target_bytes: int
    full_cycles: int
    extra_counts: tuple[int, ...]
    object_sizes: tuple[int, ...] = OBJECT_SIZES

    @property
    def size_counts(self) -> tuple[int, ...]:
        return tuple(
            self.full_cycles + extra
            for extra in self.extra_counts
        )

    @property
    def object_count(self) -> int:
        return sum(self.size_counts)

    @property
    def planned_bytes(self) -> int:
        return sum(
            size * count for size, count in zip(self.object_sizes, self.size_counts)
        )

    @property
    def digest(self) -> str:
        encoded = json.dumps(
            {
                "object_sizes": list(self.object_sizes),
                "plan_version": PLAN_VERSION,
                "size_counts": list(self.size_counts),
                "target_bytes": self.target_bytes,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def iter_sizes(self) -> Iterator[int]:
        for _ in range(self.full_cycles):
            yield from self.object_sizes
        for size, count in zip(reversed(self.object_sizes), reversed(self.extra_counts)):
            for _ in range(count):
                yield size


def build_wss_plan(target_bytes: int) -> WssPlan:
    """Build an exact, balanced plan containing every required object size."""

    cycle_bytes = sum(OBJECT_SIZES)
    if target_bytes < cycle_bytes:
        raise ConfigError(
            f"wss_bytes must be at least {cycle_bytes} so all four sizes occur"
        )
    if target_bytes % OBJECT_SIZES[0] != 0:
        raise ConfigError(
            f"wss_bytes must be {OBJECT_SIZES[0]}-byte aligned; got {target_bytes}"
        )

    full_cycles, remainder = divmod(target_bytes, cycle_bytes)
    extra_by_size = {size: 0 for size in OBJECT_SIZES}
    for size in reversed(OBJECT_SIZES):
        count, remainder = divmod(remainder, size)
        extra_by_size[size] = count
    if remainder != 0:  # Defensive: 4 KiB alignment should make this impossible.
        raise ConfigError(f"cannot represent {target_bytes} with the fixed size matrix")

    plan = WssPlan(
        target_bytes=target_bytes,
        full_cycles=full_cycles,
        extra_counts=tuple(extra_by_size[size] for size in OBJECT_SIZES),
    )
    if plan.planned_bytes != target_bytes:
        raise AssertionError("internal WSS plan byte accounting mismatch")
    return plan


@dataclasses.dataclass(frozen=True)
class WssConfig:
    role: str
    run_id: str
    node_id: str
    pool_id: str
    device_name: str
    capacity: int
    local_hostname: str
    master_server: str
    wss_bytes: int = DEFAULT_WSS_BYTES
    headroom_bytes: int = DEFAULT_HEADROOM_BYTES
    progress_bytes: int = DEFAULT_PROGRESS_BYTES
    metadata_server: str = "P2PHANDSHAKE"
    global_segment_size: int = 0
    local_buffer_size: int = 256 * 1024 * 1024
    mapping_offset: int = 0
    timeout_sec: float = 24 * 60 * 60
    poll_ms: int = 250
    cleanup: bool = True
    summary_json: Optional[str] = None

    @property
    def run_token(self) -> str:
        return hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()[:16]

    @property
    def plan(self) -> WssPlan:
        return build_wss_plan(self.wss_bytes)

    def validate(self) -> None:
        if self.role not in VALID_ROLES:
            raise ConfigError(f"role must be one of {VALID_ROLES}")
        for field_name in ("run_id", "node_id", "pool_id", "device_name"):
            value = getattr(self, field_name)
            if not value or any(ord(character) < 32 for character in value):
                raise ConfigError(f"{field_name} must be non-empty and printable")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,96}", self.run_id):
            raise ConfigError("run_id must be a portable token of at most 96 characters")
        _validate_endpoint("local_hostname", self.local_hostname)
        _validate_endpoint("master_server", self.master_server)
        if self.metadata_server != "P2PHANDSHAKE":
            raise ConfigError("metadata_server must be P2PHANDSHAKE")
        if self.global_segment_size != 0:
            raise ConfigError("global_segment_size must be 0 for protocol=cxl")
        for field_name in (
            "capacity",
            "local_buffer_size",
            "wss_bytes",
            "progress_bytes",
            "poll_ms",
        ):
            if getattr(self, field_name) <= 0:
                raise ConfigError(f"{field_name} must be positive")
        if self.headroom_bytes < 0 or self.mapping_offset < 0:
            raise ConfigError("headroom_bytes and mapping_offset must be non-negative")
        if self.timeout_sec <= 0:
            raise ConfigError("timeout_sec must be positive")
        if self.wss_bytes > self.capacity:
            raise ConfigError("wss_bytes exceeds the complete mapped CXL capacity")
        _ = self.plan


def _validate_endpoint(name: str, endpoint: str) -> None:
    if not endpoint or ":" not in endpoint:
        raise ConfigError(f"{name} must be host:port")
    host, port_text = endpoint.rsplit(":", 1)
    if not host or not port_text.isdigit() or not 1 <= int(port_text) <= 65535:
        raise ConfigError(f"{name} must contain a non-empty host and valid port")


def _environment_integer(name: str) -> int:
    value = os.environ.get(name, "")
    if not value.isdigit():
        raise ConfigError(f"{name} must be a non-negative decimal integer")
    return int(value)


def validate_lab_environment(config: WssConfig) -> None:
    """Validate native mapping and per-client partition ownership."""

    expected = {
        "MC_CXL_PROVIDER": "mooncake",
        "MC_CXL_BACKEND_KIND": "devdax",
        "MC_CXL_POOL_ID": config.pool_id,
        "MC_CXL_DEV_PATH": config.device_name,
        "MC_CXL_DEV_SIZE": str(config.capacity),
        "MC_CXL_MAP_OFFSET": str(config.mapping_offset),
        "MC_CXL_TEST_DESTRUCTIVE": "1",
        "MOONCAKE_STORE_CHECKSUM": "1",
    }
    for name, wanted in expected.items():
        actual = os.environ.get(name)
        if actual != wanted:
            raise ConfigError(f"{name} must be {wanted!r}, got {actual!r}")

    owned_offset = _environment_integer("MC_CXL_OWNED_OFFSET")
    owned_size = _environment_integer("MC_CXL_OWNED_SIZE")
    if owned_size == 0:
        raise ConfigError("MC_CXL_OWNED_SIZE must be positive")
    if owned_offset % SLAB_BYTES or owned_size % SLAB_BYTES:
        raise ConfigError(
            f"owned offset and size must be {SLAB_BYTES}-byte CacheLib slab aligned"
        )
    if owned_offset > config.capacity or owned_size > config.capacity - owned_offset:
        raise ConfigError("owned extent exceeds the complete mapped CXL pool")

    if config.role == "node0":
        required = config.wss_bytes + config.headroom_bytes
        if required > config.capacity:
            raise ConfigError("wss_bytes plus headroom exceeds complete pool capacity")
        if owned_size < required:
            raise ConfigError(
                "writer partition is too small: "
                f"owned={owned_size} required={required} "
                f"(wss={config.wss_bytes} headroom={config.headroom_bytes})"
            )
    elif owned_size < SLAB_BYTES:
        raise ConfigError("reader partition needs at least one slab for protocol markers")


@dataclasses.dataclass(frozen=True)
class ObjectSpec:
    index: int
    size: int
    key: str


def iter_object_specs(config: WssConfig) -> Iterator[ObjectSpec]:
    for index, size in enumerate(config.plan.iter_sizes()):
        yield ObjectSpec(
            index=index,
            size=size,
            key=f"{COMPONENT}-{config.run_token}-{index:08d}-{size}",
        )


def deterministic_payload(config: WssConfig, spec: ObjectSpec) -> bytes:
    seed = hashlib.sha256(
        (
            f"{COMPONENT}|{PLAN_VERSION}|{config.run_token}|{config.pool_id}|"
            f"{spec.index}|{spec.size}"
        ).encode("utf-8")
    ).digest()
    return (seed * ((spec.size + len(seed) - 1) // len(seed)))[: spec.size]


def checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def marker_key(config: WssConfig, marker: str) -> str:
    return f"{COMPONENT}-{config.run_token}-marker-{marker}"


def _marker_record(config: WssConfig, marker: str) -> dict[str, Any]:
    plan = config.plan
    return {
        "component": COMPONENT,
        "marker": marker,
        "object_count": plan.object_count,
        "object_sizes": list(OBJECT_SIZES),
        "plan_digest": plan.digest,
        "plan_version": PLAN_VERSION,
        "pool_id": config.pool_id,
        "run_token": config.run_token,
        "wss_bytes": config.wss_bytes,
    }


def marker_payload(config: WssConfig, marker: str) -> bytes:
    return json.dumps(
        _marker_record(config, marker), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


class StatusRecorder:
    """Low-volume JSONL status suitable for a 500 GiB streaming test."""

    def __init__(self, config: WssConfig, stream: Optional[io.TextIOBase] = None):
        self.config = config
        self.stream = stream if stream is not None else sys.stdout
        self.started_at = time.monotonic()
        self.event_count = 0
        self.objects_put = 0
        self.objects_get = 0
        self.objects_removed = 0
        self.bytes_put = 0
        self.bytes_get = 0
        self.bytes_removed = 0
        self.checksums_verified = 0
        self.completed_phases: list[str] = []
        self.terminal_status = "RUNNING"
        self.failure_type: Optional[str] = None

    def emit(self, event: str, **fields: Any) -> None:
        record = {
            "component": COMPONENT,
            "event": event,
            "node_id": self.config.node_id,
            "pool_id": self.config.pool_id,
            "role": self.config.role,
            "run_token": self.config.run_token,
            "tier": TIER,
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        record.update(fields)
        print(json.dumps(record, sort_keys=True), file=self.stream, flush=True)
        self.event_count += 1

    def record_object(self, operation: str, size: int, verified: bool = False) -> None:
        if operation == "put":
            self.objects_put += 1
            self.bytes_put += size
        elif operation == "get":
            self.objects_get += 1
            self.bytes_get += size
            self.checksums_verified += int(verified)
        elif operation == "remove":
            self.objects_removed += 1
            self.bytes_removed += size
        else:
            raise ValueError(f"unsupported operation {operation!r}")

    def phase_complete(
        self, phase: str, operation: str, objects: int, byte_count: int, elapsed: float
    ) -> None:
        self.completed_phases.append(phase)
        self.emit(
            "phase_complete",
            phase=phase,
            operation=operation,
            objects=objects,
            bytes=byte_count,
            elapsed_sec=round(elapsed, 6),
            mib_per_sec=round(byte_count / elapsed / 1024**2, 3) if elapsed else 0.0,
            status="PASS",
        )

    def finish(self, status: str, failure_type: Optional[str] = None) -> dict[str, Any]:
        self.terminal_status = status
        self.failure_type = failure_type
        self.emit(
            "test_complete",
            status=status,
            failure_type=failure_type,
            elapsed_sec=round(time.monotonic() - self.started_at, 6),
        )
        return self.summary()

    def summary(self) -> dict[str, Any]:
        plan = self.config.plan
        return {
            "bytes_get": self.bytes_get,
            "bytes_put": self.bytes_put,
            "bytes_removed": self.bytes_removed,
            "checksum_algorithm": "sha256",
            "checksums_verified": self.checksums_verified,
            "cleanup": self.config.cleanup,
            "completed_phases": list(self.completed_phases),
            "component": COMPONENT,
            "elapsed_sec": round(time.monotonic() - self.started_at, 6),
            "event_count": self.event_count,
            "exact_byte_equality": True,
            "failure_type": self.failure_type,
            "node_id": self.config.node_id,
            "object_count": plan.object_count,
            "object_size_counts": dict(zip(map(str, OBJECT_SIZES), plan.size_counts)),
            "object_sizes": list(OBJECT_SIZES),
            "objects_get": self.objects_get,
            "objects_put": self.objects_put,
            "objects_removed": self.objects_removed,
            "plan_digest": plan.digest,
            "pool_id": self.config.pool_id,
            "role": self.config.role,
            "run_token": self.config.run_token,
            "status": self.terminal_status,
            "tier": TIER,
            "wss_bytes": self.config.wss_bytes,
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
    ) as output:
        json.dump(summary, output, indent=2, sort_keys=True)
        output.write("\n")
        temporary = pathlib.Path(output.name)
    os.replace(temporary, destination)


def _require_zero(operation: str, result: Any) -> None:
    try:
        code = int(result)
    except (TypeError, ValueError) as error:
        raise ProtocolError(f"{operation} returned invalid status {result!r}") from error
    if code != 0:
        raise ProtocolError(f"{operation} failed with return code {code}")


def _put_marker(store: Any, config: WssConfig, replica_config: Any, marker: str) -> None:
    _require_zero(
        f"put marker {marker}",
        store.put(marker_key(config, marker), marker_payload(config, marker), replica_config),
    )


def _read_marker(store: Any, config: WssConfig, marker: str) -> Optional[bytes]:
    key = marker_key(config, marker)
    exists = int(store.is_exist(key))
    if exists < 0:
        raise ProtocolError(f"is_exist failed for marker {marker}: {exists}")
    if exists == 0:
        return None
    payload = store.get(key)
    if payload is None:
        raise ProtocolError(f"marker {marker} exists but get returned no payload")
    return bytes(payload)


def _wait_for_marker(
    store: Any,
    config: WssConfig,
    wanted: str,
    peer_failure: str,
    recorder: StatusRecorder,
) -> None:
    deadline = time.monotonic() + config.timeout_sec
    recorder.emit("marker_wait_begin", marker=wanted, timeout_sec=config.timeout_sec)
    while time.monotonic() < deadline:
        failure = _read_marker(store, config, peer_failure)
        if failure is not None:
            try:
                record = json.loads(failure)
                failure_type = str(record.get("failure_type", "unknown"))
            except Exception:
                failure_type = "malformed_failure_marker"
            raise PeerFailure(f"peer published {peer_failure}: {failure_type}")

        actual = _read_marker(store, config, wanted)
        if actual is not None:
            expected = marker_payload(config, wanted)
            if actual != expected:
                raise ProtocolError(
                    f"marker {wanted} mismatch: expected_sha256={checksum(expected)} "
                    f"actual_sha256={checksum(actual)}"
                )
            recorder.emit("marker_observed", marker=wanted, status="PASS")
            return
        time.sleep(config.poll_ms / 1000.0)
    raise MarkerTimeout(f"timed out after {config.timeout_sec}s waiting for {wanted}")


def _wait_for_marker_removal(
    store: Any, config: WssConfig, marker: str, recorder: StatusRecorder
) -> None:
    deadline = time.monotonic() + config.timeout_sec
    while time.monotonic() < deadline:
        if _read_marker(store, config, marker) is None:
            recorder.emit("marker_removed", marker=marker, status="PASS")
            return
        time.sleep(config.poll_ms / 1000.0)
    raise MarkerTimeout(f"timed out waiting for removal of marker {marker}")


def _publish_failure(
    store: Any,
    config: WssConfig,
    replica_config: Any,
    failure_type: str,
) -> None:
    marker = "writer_failed" if config.role == "node0" else "reader_failed"
    record = _marker_record(config, marker)
    record["failure_type"] = failure_type
    payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
    _require_zero(
        f"put marker {marker}", store.put(marker_key(config, marker), payload, replica_config)
    )


def _check_fresh_namespace(store: Any, config: WssConfig) -> None:
    # Node 1 is intentionally allowed to start after Node 0 has published
    # ``ready``.  Only the writer owns the full namespace freshness check;
    # the reader checks the two markers it can author without racing puts.
    marker_names = (
        ("ready", "verified", "complete", "writer_failed", "reader_failed")
        if config.role == "node0"
        else ("verified", "reader_failed")
    )
    keys = [marker_key(config, marker) for marker in marker_names]
    if config.role == "node0":
        iterator = iter_object_specs(config)
        first = next(iterator)
        last = first
        for last in iterator:
            pass
        keys.extend((first.key, last.key))
    for key in keys:
        exists = int(store.is_exist(key))
        if exists < 0:
            raise ProtocolError(f"is_exist failed for {key}: {exists}")
        if exists == 1:
            raise ConfigError(f"stale WSS namespace key exists: {key}; use a new run_id")


def _emit_progress(
    recorder: StatusRecorder,
    operation: str,
    objects: int,
    byte_count: int,
    total_objects: int,
    total_bytes: int,
    started: float,
) -> None:
    elapsed = max(time.monotonic() - started, 1e-9)
    recorder.emit(
        "io_progress",
        operation=operation,
        objects=objects,
        total_objects=total_objects,
        bytes=byte_count,
        total_bytes=total_bytes,
        percent=round(byte_count * 100.0 / total_bytes, 4),
        elapsed_sec=round(elapsed, 3),
        mib_per_sec=round(byte_count / elapsed / 1024**2, 3),
    )


def _put_working_set(
    store: Any, config: WssConfig, replica_config: Any, recorder: StatusRecorder
) -> None:
    plan = config.plan
    started = time.monotonic()
    next_progress = config.progress_bytes
    for spec in iter_object_specs(config):
        payload = deterministic_payload(config, spec)
        _require_zero(f"put {spec.key}", store.put(spec.key, payload, replica_config))
        recorder.record_object("put", spec.size)
        if recorder.bytes_put >= next_progress or recorder.objects_put == plan.object_count:
            _emit_progress(
                recorder,
                "put",
                recorder.objects_put,
                recorder.bytes_put,
                plan.object_count,
                plan.target_bytes,
                started,
            )
            while next_progress <= recorder.bytes_put:
                next_progress += config.progress_bytes
    elapsed = time.monotonic() - started
    recorder.phase_complete("node0.single_put", "put", plan.object_count, plan.target_bytes, elapsed)


def _get_working_set(store: Any, config: WssConfig, recorder: StatusRecorder) -> None:
    plan = config.plan
    started = time.monotonic()
    next_progress = config.progress_bytes
    for spec in iter_object_specs(config):
        actual = store.get(spec.key)
        if actual is None:
            raise ProtocolError(f"get miss for {spec.key}")
        actual_bytes = bytes(actual)
        if len(actual_bytes) != spec.size:
            raise ProtocolError(
                f"size mismatch for {spec.key}: expected={spec.size} actual={len(actual_bytes)}"
            )
        expected = deterministic_payload(config, spec)
        expected_checksum = checksum(expected)
        actual_checksum = checksum(actual_bytes)
        if actual_checksum != expected_checksum:
            raise ProtocolError(
                f"checksum mismatch for {spec.key}: expected_sha256={expected_checksum} "
                f"actual_sha256={actual_checksum}"
            )
        if actual_bytes != expected:
            raise ProtocolError(f"exact byte mismatch for {spec.key}")
        recorder.record_object("get", spec.size, verified=True)
        if recorder.bytes_get >= next_progress or recorder.objects_get == plan.object_count:
            _emit_progress(
                recorder,
                "get",
                recorder.objects_get,
                recorder.bytes_get,
                plan.object_count,
                plan.target_bytes,
                started,
            )
            while next_progress <= recorder.bytes_get:
                next_progress += config.progress_bytes
    elapsed = time.monotonic() - started
    recorder.phase_complete("node1.peer_single_get", "get", plan.object_count, plan.target_bytes, elapsed)


def _remove_working_set(store: Any, config: WssConfig, recorder: StatusRecorder) -> None:
    plan = config.plan
    started = time.monotonic()
    next_progress = config.progress_bytes
    for spec in iter_object_specs(config):
        _require_zero(f"remove {spec.key}", store.remove(spec.key, True))
        recorder.record_object("remove", spec.size)
        if (
            recorder.bytes_removed >= next_progress
            or recorder.objects_removed == plan.object_count
        ):
            _emit_progress(
                recorder,
                "remove",
                recorder.objects_removed,
                recorder.bytes_removed,
                plan.object_count,
                plan.target_bytes,
                started,
            )
            while next_progress <= recorder.bytes_removed:
                next_progress += config.progress_bytes
    elapsed = time.monotonic() - started
    recorder.phase_complete("node0.cleanup", "remove", plan.object_count, plan.target_bytes, elapsed)


def run_role(
    store: Any, config: WssConfig, replica_config: Any, recorder: StatusRecorder
) -> None:
    config.validate()
    _check_fresh_namespace(store, config)
    plan = config.plan
    recorder.emit(
        "preflight_complete",
        object_count=plan.object_count,
        object_size_counts=dict(zip(map(str, OBJECT_SIZES), plan.size_counts)),
        object_sizes=list(OBJECT_SIZES),
        plan_digest=plan.digest,
        wss_bytes=config.wss_bytes,
        status="PASS",
    )

    if config.role == "node0":
        _put_working_set(store, config, replica_config, recorder)
        _put_marker(store, config, replica_config, "ready")
        recorder.emit("marker_published", marker="ready", status="PASS")
        _wait_for_marker(store, config, "verified", "reader_failed", recorder)

        if config.cleanup:
            _remove_working_set(store, config, recorder)
            for marker in ("ready", "verified"):
                _require_zero(
                    f"remove marker {marker}", store.remove(marker_key(config, marker), True)
                )
        _put_marker(store, config, replica_config, "complete")
        recorder.emit("marker_published", marker="complete", status="PASS")
        _wait_for_marker_removal(store, config, "complete", recorder)
        return

    _wait_for_marker(store, config, "ready", "writer_failed", recorder)
    _get_working_set(store, config, recorder)
    _put_marker(store, config, replica_config, "verified")
    recorder.emit("marker_published", marker="verified", status="PASS")
    _wait_for_marker(store, config, "complete", "writer_failed", recorder)
    _require_zero(
        "remove marker complete", store.remove(marker_key(config, "complete"), True)
    )
    recorder.emit("marker_acknowledged", marker="complete", status="PASS")


def _load_store_module(module_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except Exception as error:
        raise ConfigError(f"cannot import {module_name!r}: {error}") from error
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
    parser.add_argument("--component", default=COMPONENT)
    parser.add_argument("--tier", default=TIER)
    parser.add_argument("--metadata-server", default="P2PHANDSHAKE")
    parser.add_argument("--global-segment-size", type=int, default=0)
    parser.add_argument("--local-buffer-size", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--mapping-offset", type=int, default=0)
    parser.add_argument("--wss-bytes", type=int, default=DEFAULT_WSS_BYTES)
    parser.add_argument("--headroom-bytes", type=int, default=DEFAULT_HEADROOM_BYTES)
    parser.add_argument("--progress-bytes", type=int, default=DEFAULT_PROGRESS_BYTES)
    parser.add_argument("--timeout-sec", type=float, default=24 * 60 * 60)
    parser.add_argument("--poll-ms", type=int, default=250)
    parser.add_argument("--summary-json")
    parser.add_argument("--cleanup", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    if args.component != COMPONENT or args.tier != TIER:
        print(
            f"[FAIL] WSS harness requires component={COMPONENT} and tier={TIER}",
            file=sys.stderr,
        )
        return 2
    config = WssConfig(
        role=args.role,
        run_id=args.run_id,
        node_id=args.node_id,
        pool_id=args.pool_id,
        device_name=args.device_name,
        capacity=args.capacity,
        local_hostname=args.local_hostname,
        master_server=args.master_server,
        metadata_server=args.metadata_server,
        global_segment_size=args.global_segment_size,
        local_buffer_size=args.local_buffer_size,
        mapping_offset=args.mapping_offset,
        wss_bytes=args.wss_bytes,
        headroom_bytes=args.headroom_bytes,
        progress_bytes=args.progress_bytes,
        timeout_sec=args.timeout_sec,
        poll_ms=args.poll_ms,
        cleanup=args.cleanup,
        summary_json=args.summary_json,
    )
    recorder = StatusRecorder(config)
    store = None
    replica_config = None
    try:
        config.validate()
        validate_lab_environment(config)
        recorder.emit(
            "process_start",
            backend_kind="devdax",
            provider="mooncake",
            checksum_algorithm="sha256",
            max_object_bytes=max(OBJECT_SIZES),
            max_python_validation_bytes=2 * max(OBJECT_SIZES),
        )
        module = _load_store_module(os.environ.get("MOONCAKE_STORE_MODULE", "mooncake.store"))
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
        recorder.emit("store_ready", protocol="cxl", status="PASS")

        replica_config = module.ReplicateConfig()
        replica_config.replica_num = 1
        replica_config.nof_replica_num = 0
        run_role(store, config, replica_config, recorder)
        _require_zero("MooncakeDistributedStore.close", store.close())
        store = None
        recorder.emit("store_closed", status="PASS")
        summary = recorder.finish("PASS")
        write_summary(config.summary_json, summary)
        return 0
    except Exception as error:
        failure_type = type(error).__name__
        if store is not None and replica_config is not None and not isinstance(error, PeerFailure):
            try:
                _publish_failure(store, config, replica_config, failure_type)
                recorder.emit("failure_marker_published", failure_type=failure_type)
            except Exception as marker_error:
                recorder.emit(
                    "failure_marker_error",
                    failure_type=type(marker_error).__name__,
                    message=str(marker_error)[:1000],
                )
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
        recorder.emit(
            "test_failure",
            failure_type=failure_type,
            message=str(error)[:1000],
            status="FAIL",
        )
        summary = recorder.finish("FAIL", failure_type)
        write_summary(config.summary_json, summary)
        print(f"[FAIL] {failure_type}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
