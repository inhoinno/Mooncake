#!/usr/bin/env python3
"""Dependency-light gates for CXL and TODO#Extra perf orchestration."""

import unittest

import cxl_gpu_perf as cxl
import dram_rdma_perf as rdma


class _Buffer:
    def __init__(self, endpoint, protocol="rdma", size=4096):
        self.transport_endpoint = endpoint
        self.protocol = protocol
        self.size = size


class _Memory:
    def __init__(self, endpoint, protocol="rdma", size=4096):
        self.buffer_descriptor = _Buffer(endpoint, protocol, size)


class _Replica:
    def __init__(self, endpoint, protocol="rdma", size=4096):
        self.memory = _Memory(endpoint, protocol, size)

    def is_memory_replica(self):
        return True

    def get_memory_descriptor(self):
        return self.memory


class _Store:
    def get_replica_desc(self, _key):
        return [
            _Replica("192.168.3.44:50200", size=16),
            _Replica("192.168.3.44:50201", size=16),
            _Replica("192.168.3.44:50200", size=8),
        ]


class PerfHarnessTest(unittest.TestCase):
    def test_cxl_client_working_sets_are_disjoint_and_deterministic(self):
        a = cxl._client_object_order(0, 64, 7)
        b = cxl._client_object_order(64, 64, 7)
        self.assertEqual(a, cxl._client_object_order(0, 64, 7))
        self.assertFalse(set(a) & set(b))
        self.assertEqual(set(a), set(range(64)))

    def test_rdma_placement_reports_distinct_source_endpoints(self):
        placement = rdma._placement_for_key(_Store(), "key")
        self.assertEqual(3, placement["replica_slices"])
        self.assertEqual(2, placement["source_segment_count"])
        self.assertEqual(40, placement["descriptor_bytes"])
        self.assertEqual(["rdma"], placement["source_protocols"])

    def test_rdma_multi_key_placement_aggregates_distinct_sources(self):
        placements = {
            "key-0": {
                "replica_slices": 1, "source_endpoints": ["m1:1"],
                "source_segment_count": 1, "source_protocols": ["rdma"],
                "descriptor_bytes": 8,
            },
            "key-1": {
                "replica_slices": 1, "source_endpoints": ["m2:1"],
                "source_segment_count": 1, "source_protocols": ["rdma"],
                "descriptor_bytes": 8,
            },
        }
        aggregate = rdma._placement_for_keys(placements)
        self.assertEqual(["m1:1", "m2:1"], aggregate["source_endpoints"])
        self.assertEqual(2, aggregate["source_segment_count"])
        self.assertEqual(16, aggregate["descriptor_bytes"])

    def test_rdma_object_keys_preserve_legacy_single_key(self):
        single = type("Args", (), {"key": "object", "object_count": 1})()
        multi = type("Args", (), {"key": "object", "object_count": 3})()
        self.assertEqual(["object"], rdma._object_keys(single))
        self.assertEqual(
            ["object-0000", "object-0001", "object-0002"],
            rdma._object_keys(multi),
        )

    def test_rdma_multi_key_targets_are_deterministic(self):
        args = type(
            "Args", (),
            {"source_endpoints": ["m4:50200", "m4:50201", "m4:50202"]},
        )()
        self.assertEqual("m4:50200", rdma._target_endpoint(args, 0))
        self.assertEqual("m4:50201", rdma._target_endpoint(args, 1))
        self.assertEqual("m4:50202", rdma._target_endpoint(args, 2))

    def test_rdma_target_is_optional_for_legacy_single_key(self):
        args = type("Args", (), {"source_endpoints": []})()
        self.assertIsNone(rdma._target_endpoint(args, 0))

    def test_rate_is_total_bytes_over_elapsed_time(self):
        rate = rdma._rate(4_000_000_000, 2.0, 4)
        self.assertEqual(2.0, rate["GBps"])
        self.assertEqual(0.5, rate["latency_sec_avg"])


if __name__ == "__main__":
    unittest.main()
