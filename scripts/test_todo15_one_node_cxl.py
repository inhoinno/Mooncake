#!/usr/bin/env python3
"""Portable tests for the one-node native-CXL Store harness.

The fake Store below validates harness behavior only.  It does not claim a
device-DAX mapping; the lab launcher is the T2 acceptance gate.
"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

import todo15_one_node_cxl as one_node


class _FakeStore:
    def __init__(self, corrupt_key: str | None = None) -> None:
        self.values: dict[str, bytes] = {}
        self.corrupt_key = corrupt_key

    def put(self, key, value, _config):
        if key in self.values:
            return -1
        self.values[key] = bytes(value)
        return 0

    def put_batch(self, keys, values, _config):
        if len(keys) != len(values) or any(key in self.values for key in keys):
            return [-1 for _ in keys]
        for key, value in zip(keys, values):
            self.values[key] = bytes(value)
        return [0 for _ in keys]

    def get(self, key):
        value = self.values.get(key)
        if value is not None and key == self.corrupt_key:
            return value[:-1] + bytes([value[-1] ^ 0xFF])
        return value

    def get_batch(self, keys):
        return [self.get(key) for key in keys]

    def is_exist(self, key):
        return int(key in self.values)

    def remove(self, key, _force=False):
        if key not in self.values:
            return -1
        del self.values[key]
        return 0


class _ReplicateConfig:
    replica_num = 1
    nof_replica_num = 0


def _config(**overrides) -> one_node.TestConfig:
    values = {
        "run_id": "one-node-unit-001",
        "pool_id": "unit-native-pool",
        "device_name": "/dev/dax0.0",
        "capacity": 1024 * 1024 * 1024,
        "local_hostname": "127.0.0.1:50071",
        "master_server": "127.0.0.1:50051",
        "owned_capacity": 1024 * 1024 * 1024,
        "object_sizes": (64, 512, 4096, 16384),
    }
    values.update(overrides)
    return one_node.TestConfig(**values)


class Todo15OneNodeCxlTest(unittest.TestCase):
    def test_preflight_native_environment_and_full_pool_ownership(self):
        config = _config(object_sizes=one_node.REQUIRED_OBJECT_SIZES)
        config.validate(require_lab_sizes=True)
        environment = {
            "MC_CXL_PROVIDER": "mooncake",
            "MC_CXL_BACKEND_KIND": "devdax",
            "MC_CXL_POOL_ID": config.pool_id,
            "MC_CXL_DEV_PATH": config.device_name,
            "MC_CXL_DEV_SIZE": str(config.capacity),
            "MC_CXL_MAP_OFFSET": "0",
            "MC_CXL_OWNED_OFFSET": "0",
            "MC_CXL_OWNED_SIZE": str(config.capacity),
            "MC_CXL_TEST_DESTRUCTIVE": "1",
            "MOONCAKE_STORE_CHECKSUM": "1",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            one_node.validate_lab_environment(config)
        with self.assertRaisesRegex(one_node.ConfigError, "owned_capacity"):
            _config(owned_capacity=config.capacity // 2).validate(False)
        with mock.patch.dict(os.environ, environment, clear=True):
            os.environ["MC_CXL_PROVIDER"] = "faketract"
            with self.assertRaisesRegex(one_node.ConfigError, "MC_CXL_PROVIDER"):
                one_node.validate_lab_environment(config)

    def test_functional_single_batch_put_get_remove(self):
        config = _config()
        store = _FakeStore()
        summary = one_node.run_matrix(store, config, _ReplicateConfig())

        expected_bytes = sum(config.object_sizes) * 2
        self.assertEqual("PASS", summary["status"])
        self.assertEqual(8, summary["objects_put"])
        self.assertEqual(8, summary["objects_get"])
        self.assertEqual(8, summary["checksums_verified"])
        self.assertEqual(expected_bytes, summary["bytes_written"])
        self.assertEqual(expected_bytes, summary["bytes_read"])
        self.assertEqual(8, summary["objects_removed"])
        self.assertEqual({}, store.values)

    def test_failure_checksum_mismatch_fails_and_cleans_up(self):
        config = _config()
        corrupt_key = one_node.object_key(config, "single", config.object_sizes[0])
        store = _FakeStore(corrupt_key=corrupt_key)

        with self.assertRaisesRegex(one_node.ProtocolError, "checksum mismatch"):
            one_node.run_matrix(store, config, _ReplicateConfig())
        self.assertEqual({}, store.values, "failed verification must clean test keys")

    def test_status_summary_is_atomic_bounded_and_payload_free(self):
        config = _config()
        summary = one_node.run_matrix(_FakeStore(), config, _ReplicateConfig())
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "nested" / "summary.json"
            one_node.write_summary(str(path), summary)
            persisted = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(config.pool_id, persisted["pool_id"])
        self.assertEqual(config.run_token, persisted["run_token"])
        self.assertEqual("sha256", persisted["checksum_algorithm"])
        self.assertTrue(persisted["exact_byte_equality"])
        serialized = json.dumps(persisted)
        for forbidden in ("payload", "raw_pointer", "base_address", "rkey"):
            self.assertNotIn(forbidden, serialized)


if __name__ == "__main__":
    unittest.main(verbosity=2)
