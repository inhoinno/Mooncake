#!/usr/bin/env python3
"""Portable protocol tests for the TODO1.5 two-node CXL harness.

These tests validate orchestration, data verification, failure publication,
and safe status output.  They do not claim shared-CXL hardware coverage; the
two-node launcher is the acceptance test for that path.
"""

from __future__ import annotations

import dataclasses
import io
import json
import pathlib
import tempfile
import threading
import unittest
from unittest import mock

import todo15_shared_cxl_stage1 as todo15


class _SharedObjects:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.values: dict[str, bytes] = {}


class _FakeStore:
    """Minimal shared Store model; intentionally not a CXL implementation."""

    def __init__(self, shared: _SharedObjects, corrupt_key: str | None = None):
        self.shared = shared
        self.corrupt_key = corrupt_key

    def put(self, key, value, _config):
        with self.shared.lock:
            self.shared.values[key] = bytes(value)
        return 0

    def put_batch(self, keys, values, _config):
        if len(keys) != len(values):
            return -1
        with self.shared.lock:
            for key, value in zip(keys, values):
                self.shared.values[key] = bytes(value)
        return 0

    def get(self, key):
        with self.shared.lock:
            value = self.shared.values.get(key)
        if value is not None and key == self.corrupt_key:
            return value[:-1] + bytes([value[-1] ^ 0xFF])
        return value

    def get_batch(self, keys):
        return [self.get(key) for key in keys]

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


def _config(role: str, **overrides) -> todo15.TestConfig:
    values = {
        "role": role,
        "run_id": "unit-two-node-001",
        "node_id": role,
        "pool_id": "unit-shared-pool",
        "device_name": "/dev/dax0.0",
        "capacity": 1024 * 1024 * 1024,
        "local_hostname": "127.0.0.1:50071" if role == "node0" else "127.0.0.1:50072",
        "master_server": "127.0.0.1:50051",
        "timeout_sec": 1.0,
        "poll_ms": 1,
        "object_sizes": (64, 512, 4096, 16384),
    }
    values.update(overrides)
    return todo15.TestConfig(**values)


class Todo15SharedCxlProtocolTest(unittest.TestCase):
    def test_preflight_validation(self):
        lab_config = dataclasses.replace(
            _config("node0"), object_sizes=todo15.REQUIRED_OBJECT_SIZES
        )
        lab_config.validate(require_lab_sizes=True)
        valid_environment = {
            "MC_CXL_BACKEND_KIND": "devdax",
            "MC_CXL_POOL_ID": lab_config.pool_id,
            "MC_CXL_DEV_PATH": lab_config.device_name,
            "MC_CXL_DEV_SIZE": "1073741824",
            "MC_CXL_MAP_OFFSET": "0",
            "MC_CXL_TEST_DESTRUCTIVE": "1",
            "MOONCAKE_STORE_CHECKSUM": "1",
        }
        with mock.patch.dict("os.environ", valid_environment, clear=True):
            todo15.validate_lab_environment(lab_config)
        self.assertNotEqual(
            todo15.marker_payload(lab_config, "n0_ready"),
            todo15.marker_payload(
                dataclasses.replace(lab_config, capacity=lab_config.capacity * 2),
                "n0_ready",
            ),
            "phase markers must reject a peer with a different mapped capacity",
        )
        self.assertNotEqual(
            todo15.marker_payload(lab_config, "n0_ready"),
            todo15.marker_payload(
                dataclasses.replace(lab_config, mapping_offset=4096), "n0_ready"
            ),
            "phase markers must reject a peer with a different mapping offset",
        )

        with self.assertRaisesRegex(todo15.ConfigError, "lab sizes"):
            _config("node0").validate(require_lab_sizes=True)
        with self.assertRaisesRegex(todo15.ConfigError, "P2PHANDSHAKE"):
            dataclasses.replace(
                lab_config, metadata_server="etcd://127.0.0.1:2379"
            ).validate()
        with mock.patch.dict("os.environ", valid_environment, clear=True):
            del __import__("os").environ["MC_CXL_TEST_DESTRUCTIVE"]
            with self.assertRaisesRegex(todo15.ConfigError, "DESTRUCTIVE"):
                todo15.validate_lab_environment(lab_config)

    def test_functional_bidirectional_single_and_batch(self):
        shared = _SharedObjects()
        config0 = _config("node0", cleanup=True)
        config1 = _config("node1")
        recorder0 = todo15.StatusRecorder(config0, io.StringIO())
        recorder1 = todo15.StatusRecorder(config1, io.StringIO())
        errors: list[BaseException] = []

        def run(store, config, recorder):
            try:
                todo15.run_role(store, config, _ReplicateConfig(), recorder)
            except BaseException as error:  # reported in the parent test thread
                errors.append(error)

        thread1 = threading.Thread(
            target=run, args=(_FakeStore(shared), config1, recorder1)
        )
        thread0 = threading.Thread(
            target=run, args=(_FakeStore(shared), config0, recorder0)
        )
        thread1.start()
        thread0.start()
        thread0.join(timeout=4)
        thread1.join(timeout=4)

        self.assertFalse(thread0.is_alive(), "node0 protocol thread did not finish")
        self.assertFalse(thread1.is_alive(), "node1 protocol thread did not finish")
        self.assertEqual([], errors)
        self.assertEqual(8, recorder0.objects_put)
        self.assertEqual(8, recorder0.objects_get)
        self.assertEqual(8, recorder1.objects_put)
        self.assertEqual(8, recorder1.objects_get)
        self.assertEqual(8, recorder0.checksums_verified)
        self.assertEqual(8, recorder1.checksums_verified)
        self.assertEqual({}, shared.values, "node0 cleanup must be namespace-scoped")

    def test_failure_checksum_mismatch_blocks_publication(self):
        shared = _SharedObjects()
        config0 = _config("node0")
        config1 = _config("node1")
        producer = _FakeStore(shared)
        for api in ("single", "batch"):
            for spec in todo15.object_specs(config0, "n0-n1", api):
                producer.put(
                    spec.key,
                    todo15.deterministic_payload(config0, spec),
                    _ReplicateConfig(),
                )
        producer.put(
            todo15.marker_key(config0, "n0_ready"),
            todo15.marker_payload(config0, "n0_ready"),
            _ReplicateConfig(),
        )

        corrupt_key = todo15.object_specs(config1, "n0-n1", "single")[0].key
        consumer = _FakeStore(shared, corrupt_key=corrupt_key)
        recorder = todo15.StatusRecorder(config1, io.StringIO())
        with self.assertRaisesRegex(todo15.ProtocolError, "checksum mismatch"):
            todo15.run_role(consumer, config1, _ReplicateConfig(), recorder)

        self.assertEqual(
            0,
            consumer.is_exist(todo15.marker_key(config1, "n1_verified")),
            "a corrupt payload must never publish the verified marker",
        )

    def test_status_summary_is_safe_and_complete(self):
        config = _config(
            "node0", component="todo2_native_cxl", tier="T2_NATIVE_SHARED_CXL"
        )
        output = io.StringIO()
        recorder = todo15.StatusRecorder(config, output)
        recorder.emit("preflight_complete", status="PASS")
        recorder.record_io("put", "n0-n1.single.put", 4, 20480, 1.25)
        summary = recorder.finish("PASS")

        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "nested" / "summary.json"
            todo15.write_summary(str(path), summary)
            persisted = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual("PASS", persisted["status"])
        self.assertEqual(config.component, persisted["component"])
        self.assertEqual(config.tier, persisted["tier"])
        self.assertTrue(todo15.marker_key(config, "ready").startswith(config.component))
        self.assertEqual(config.pool_id, persisted["pool_id"])
        self.assertEqual(config.run_token, persisted["run_token"])
        self.assertEqual("sha256", persisted["checksum_algorithm"])
        self.assertTrue(persisted["exact_byte_equality"])
        self.assertEqual(4, persisted["objects_put"])
        log_records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual("test_complete", log_records[-1]["event"])
        self.assertEqual("PASS", log_records[-1]["status"])
        serialized = json.dumps({"summary": persisted, "logs": log_records})
        for forbidden in ("payload", "raw_pointer", "rkey", "base_address"):
            self.assertNotIn(forbidden, serialized)


if __name__ == "__main__":
    unittest.main(verbosity=2)
