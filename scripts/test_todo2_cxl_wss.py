#!/usr/bin/env python3
"""Portable tests for the TODO #2 native-CXL working-set harness."""

from __future__ import annotations

import dataclasses
import io
import json
import os
import threading
import unittest
from unittest import mock

import todo2_cxl_wss as wss


class _SharedObjects:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.values: dict[str, bytes] = {}


class _FakeStore:
    """Store API model for orchestration tests, not a CXL implementation."""

    def __init__(self, shared: _SharedObjects, corrupt_key: str | None = None):
        self.shared = shared
        self.corrupt_key = corrupt_key

    def put(self, key, value, _config):
        with self.shared.lock:
            if key in self.shared.values:
                return -1
            self.shared.values[key] = bytes(value)
        return 0

    def get(self, key):
        with self.shared.lock:
            value = self.shared.values.get(key)
        if value is not None and key == self.corrupt_key:
            return value[:-1] + bytes([value[-1] ^ 0xFF])
        return value

    def is_exist(self, key):
        with self.shared.lock:
            return int(key in self.shared.values)

    def remove(self, key, _force=False):
        with self.shared.lock:
            if key not in self.shared.values:
                return -1
            del self.shared.values[key]
        return 0


class _ReplicateConfig:
    replica_num = 1
    nof_replica_num = 0


def _config(role: str, **overrides) -> wss.WssConfig:
    values = {
        "role": role,
        "run_id": "unit-wss-001",
        "node_id": role,
        "pool_id": "unit-native-cxl-pool",
        "device_name": "/dev/dax0.0",
        "capacity": 2 * 1024**3,
        "local_hostname": (
            "10.0.0.10:50071" if role == "node0" else "10.0.0.11:50072"
        ),
        "master_server": "10.0.0.10:50051",
        "wss_bytes": sum(wss.OBJECT_SIZES),
        "headroom_bytes": wss.SLAB_BYTES,
        "progress_bytes": sum(wss.OBJECT_SIZES),
        "timeout_sec": 3.0,
        "poll_ms": 1,
    }
    values.update(overrides)
    return wss.WssConfig(**values)


class Todo2CxlWssTest(unittest.TestCase):
    def test_preflight_500g_plan_and_capacity(self):
        plan = wss.build_wss_plan(wss.DEFAULT_WSS_BYTES)
        self.assertEqual(wss.DEFAULT_WSS_BYTES, plan.planned_bytes)
        self.assertEqual(120020, plan.object_count)
        self.assertEqual(
            (30000, 30013, 30007, 30000),
            plan.size_counts,
        )
        self.assertEqual(wss.OBJECT_SIZES, tuple(dict.fromkeys(plan.iter_sizes())))

        config = _config("node0")
        config.validate()
        valid_environment = {
            "MC_CXL_PROVIDER": "mooncake",
            "MC_CXL_BACKEND_KIND": "devdax",
            "MC_CXL_POOL_ID": config.pool_id,
            "MC_CXL_DEV_PATH": config.device_name,
            "MC_CXL_DEV_SIZE": str(config.capacity),
            "MC_CXL_MAP_OFFSET": "0",
            "MC_CXL_OWNED_OFFSET": "0",
            "MC_CXL_OWNED_SIZE": str(1024**3),
            "MC_CXL_TEST_DESTRUCTIVE": "1",
            "MOONCAKE_STORE_CHECKSUM": "1",
        }
        with mock.patch.dict(os.environ, valid_environment, clear=True):
            wss.validate_lab_environment(config)
            os.environ["MC_CXL_PROVIDER"] = "faketract"
            with self.assertRaisesRegex(wss.ConfigError, "PROVIDER"):
                wss.validate_lab_environment(config)
        with mock.patch.dict(os.environ, valid_environment, clear=True):
            os.environ["MC_CXL_OWNED_SIZE"] = str(wss.SLAB_BYTES)
            with self.assertRaisesRegex(wss.ConfigError, "too small"):
                wss.validate_lab_environment(config)

    def test_functional_single_put_peer_get(self):
        shared = _SharedObjects()
        writer = _config("node0", cleanup=True)
        reader = _config("node1", cleanup=True)
        writer_recorder = wss.StatusRecorder(writer, io.StringIO())
        reader_recorder = wss.StatusRecorder(reader, io.StringIO())
        failures: list[BaseException] = []

        def run(store, config, recorder):
            try:
                wss.run_role(store, config, _ReplicateConfig(), recorder)
            except BaseException as error:
                failures.append(error)

        reader_thread = threading.Thread(
            target=run, args=(_FakeStore(shared), reader, reader_recorder)
        )
        writer_thread = threading.Thread(
            target=run, args=(_FakeStore(shared), writer, writer_recorder)
        )
        reader_thread.start()
        writer_thread.start()
        writer_thread.join(timeout=10)
        reader_thread.join(timeout=10)

        self.assertFalse(writer_thread.is_alive())
        self.assertFalse(reader_thread.is_alive())
        self.assertEqual([], failures)
        self.assertEqual(4, writer_recorder.objects_put)
        self.assertEqual(4, reader_recorder.objects_get)
        self.assertEqual(4, reader_recorder.checksums_verified)
        self.assertEqual(sum(wss.OBJECT_SIZES), writer_recorder.bytes_put)
        self.assertEqual(sum(wss.OBJECT_SIZES), reader_recorder.bytes_get)
        self.assertEqual({}, shared.values)

    def test_failure_corruption_blocks_verified(self):
        shared = _SharedObjects()
        config = _config("node1")
        producer = _FakeStore(shared)
        for spec in wss.iter_object_specs(config):
            producer.put(
                spec.key, wss.deterministic_payload(config, spec), _ReplicateConfig()
            )
        producer.put(
            wss.marker_key(config, "ready"),
            wss.marker_payload(config, "ready"),
            _ReplicateConfig(),
        )
        corrupt_key = next(wss.iter_object_specs(config)).key
        recorder = wss.StatusRecorder(config, io.StringIO())
        with self.assertRaisesRegex(wss.ProtocolError, "checksum mismatch"):
            wss.run_role(
                _FakeStore(shared, corrupt_key=corrupt_key),
                config,
                _ReplicateConfig(),
                recorder,
            )
        self.assertEqual(
            0,
            producer.is_exist(wss.marker_key(config, "verified")),
            "corrupt data must not publish the verified marker",
        )

    def test_status_is_bounded_and_safe(self):
        config = _config("node0")
        output = io.StringIO()
        recorder = wss.StatusRecorder(config, output)
        recorder.emit("preflight_complete", status="PASS")
        for size in wss.OBJECT_SIZES:
            recorder.record_object("put", size)
        recorder.phase_complete(
            "node0.single_put",
            "put",
            len(wss.OBJECT_SIZES),
            sum(wss.OBJECT_SIZES),
            0.5,
        )
        summary = recorder.finish("PASS")
        records = [json.loads(line) for line in output.getvalue().splitlines()]

        self.assertEqual("PASS", summary["status"])
        self.assertEqual(list(wss.OBJECT_SIZES), summary["object_sizes"])
        self.assertEqual("sha256", summary["checksum_algorithm"])
        self.assertTrue(summary["exact_byte_equality"])
        self.assertEqual("test_complete", records[-1]["event"])
        serialized = json.dumps({"summary": summary, "records": records})
        for forbidden in ("payload", "raw_pointer", "rkey", "base_address"):
            self.assertNotIn(forbidden, serialized)


if __name__ == "__main__":
    unittest.main(verbosity=2)
