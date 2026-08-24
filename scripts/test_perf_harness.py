#!/usr/bin/env python3
"""Dependency-light gates for CXL and TODO#Extra perf orchestration."""

import unittest

import cxl_gpu_perf as cxl
import dram_rdma_perf as rdma
import monitor_master_distribution as master_distribution
import summarize_dram_rdma_results as rdma_summary


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


class _CleanupStore:
    def __init__(self):
        self.keys = {"object-0000", "object-0001"}
        self.closed = False

    def is_exist(self, key):
        return int(key in self.keys)

    def remove(self, key, _hard):
        self.keys.remove(key)
        return 0

    def close(self):
        self.closed = True


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

    def test_rdma_batch_groups_preserve_per_source_key_groups(self):
        keys = [f"key-{i}" for i in range(8)]
        groups = rdma._batch_groups(keys, list(range(8)), [1] * 8, 4)
        self.assertEqual(2, len(groups))
        self.assertEqual(keys[:4], groups[0][0])
        self.assertEqual(keys[4:], groups[1][0])

    def test_rdma_zero_batch_group_means_one_global_batch(self):
        keys = ["a", "b", "c"]
        groups = rdma._batch_groups(keys, [1, 2, 3], [4, 4, 4], 0)
        self.assertEqual(1, len(groups))
        self.assertEqual(keys, groups[0][0])

    def test_rdma_cleanup_removes_the_complete_dataset(self):
        store = _CleanupStore()
        args = type("Args", (), {"key": "object", "object_count": 2})()
        original = rdma._open_store
        rdma._open_store = lambda *_: (None, store)
        try:
            result = rdma.run_cleanup(args)
        finally:
            rdma._open_store = original
        self.assertEqual(2, result["removed"])
        self.assertFalse(store.keys)
        self.assertTrue(store.closed)

    def test_rate_is_total_bytes_over_elapsed_time(self):
        rate = rdma._rate(4_000_000_000, 2.0, 4)
        self.assertEqual(2.0, rate["GBps"])
        self.assertEqual(0.5, rate["latency_sec_avg"])

    def test_rdma_compact_summary_omits_per_key_dump(self):
        base = {
            "status": "PASS", "object_count": 4, "block_bytes": 16,
            "source_segment_count": 2,
            "source_endpoints": ["m1:1", "m2:1"],
            "source_protocols": ["rdma"],
            "gpu_path_selected": "rdma_host_staged",
            "per_key_placement": {"large": "must not leak"},
        }
        split = {
            "rdma_to_host": {"sec": 1.0, "GBps": 8.0},
            "host_to_gpu": {"sec": 1.0, "GBps": 8.0},
            "unattributed_sec": 0.1,
        }
        single = dict(base, to_gpu_single_staged={
            "iterations": 2, "api_calls": 8, "objects_per_call": 1,
            "sec": 4.0, "GBps": 4.0}, staged_breakdown=split)
        batch = dict(base, to_gpu_batch_staged={
            "iterations": 2, "api_calls": 4, "objects_per_call": 2,
            "sec": 2.0, "GBps": 8.0}, staged_breakdown=split)
        original = rdma_summary._load
        rdma_summary._load = lambda path: (
            single if "single" in path.name else batch)
        try:
            summary = rdma_summary.build_summary(
                __import__("pathlib").Path("unused"))
        finally:
            rdma_summary._load = original
        self.assertNotIn("per_key_placement", summary)
        self.assertEqual(2.0, summary["batch_throughput_speedup"])
        self.assertEqual(4, summary["single"]["api_calls_per_iteration"])

    def test_gdr_summary_requires_and_reports_direct_path(self):
        base = {
            "status": "PASS", "object_count": 2, "block_bytes": 16,
            "source_segment_count": 2,
            "source_endpoints": ["m1:1", "m2:1"],
            "source_protocols": ["rdma"],
            "gpu_path_selected": "rdma_gpu_direct",
        }
        single = dict(base, to_gpu_single_gpudirect={
            "iterations": 1, "api_calls": 2, "objects_per_call": 1,
            "sec": 2.0, "GBps": 4.0})
        batch = dict(base, to_gpu_batch_gpudirect={
            "iterations": 1, "api_calls": 1, "objects_per_call": 2,
            "sec": 1.0, "GBps": 8.0})
        original = rdma_summary._load
        rdma_summary._load = lambda path: (
            single if "single" in path.name else batch)
        try:
            summary = rdma_summary.build_summary(
                __import__("pathlib").Path("unused"), "gdr")
        finally:
            rdma_summary._load = original
        self.assertEqual("rdma_gpu_direct", summary["gpu_path"])
        self.assertNotIn("rdma_to_host_GBps", summary["single"])
        self.assertIn("GPU-direct", rdma_summary.render(summary))

    def test_master_distribution_separates_population_and_get_evidence(self):
        metrics = '''
segment_allocated_bytes{segment="m1:50200"} 1073741824
segment_allocated_bytes{segment="m2:50200"} 1073741824
segment_total_capacity_bytes{segment="m1:50200"} 8589934592
segment_total_capacity_bytes{segment="m2:50200"} 8589934592
master_get_advertised_replicas_total{segment="m1:50200",protocol="rdma"} 5
master_get_advertised_replicas_total{segment="m2:50200",protocol="rdma"} 7
master_get_advertised_bytes_total{segment="m1:50200",protocol="rdma"} 80
master_get_advertised_bytes_total{segment="m2:50200",protocol="rdma"} 112
'''
        samples = master_distribution.parse_prometheus(metrics)
        baseline = {
            "objects": {"m1:50200": 1, "m2:50200": 1},
            "bytes": {"m1:50200": 16, "m2:50200": 16},
        }
        result = master_distribution.build_snapshot(samples, baseline, 2)
        self.assertTrue(result["expected_segments_ready"])
        self.assertEqual(2, result["population_bytes"]["active_segments"])
        self.assertEqual(10, result["get_advertised_objects_delta"]["total"])
        self.assertFalse(result["evidence_scope"]["data_plane_bytes_observed"])


if __name__ == "__main__":
    unittest.main()
