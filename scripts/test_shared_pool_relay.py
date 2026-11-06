#!/usr/bin/env python3
"""Functional T0 tests: actual model bytes, no NIC/DAX/CUDA required."""

import unittest

from shared_pool_relay import Member, SharedPoolRelay


class SharedPoolRelayTest(unittest.TestCase):
    def setup_relay(self, senders=2, receivers=2):
        relay = SharedPoolRelay()
        relay.add_pool("rack-a", 4096)
        relay.add_pool("rack-b", 4096)
        for index in range(senders):
            relay.add_member(Member(f"s{index}", "rack-a", 0x100000 * (index + 1), can_send=True))
        for index in range(receivers):
            relay.add_member(Member(f"r{index}", "rack-b", 0x800000 * (index + 1), can_receive=True))
        relay.add_member(Member("target", "rack-b", 0x90000000))
        # Nonzero and different source/destination offsets catch accidental
        # pointer equality and whole-pool copying in the happy path.
        relay.seed("source-prefix", "rack-a", b"prefix-a")
        relay.seed("target-prefix", "rack-b", b"prefix-in-target")
        payload = bytes((index * 17 + 3) % 256 for index in range(257))
        relay.seed("kv", "rack-a", payload, generation=7)
        return relay, payload

    def test_master_lookup_and_receiver_mr_partition(self):
        relay, payload = self.setup_relay()
        record, senders = relay.lookup("kv", "rack-a")
        self.assertEqual(["s0", "s1"], senders)
        self.assertEqual(7, record.generation)
        transfer = relay.prepare("kv", "rack-a", "target", "worker-0", 32)
        mrs = relay.mrs[transfer.transfer_id]
        self.assertEqual(["r0", "r1"], [mr.receiver for mr in mrs])
        self.assertEqual(len(payload), sum(mr.location.length for mr in mrs))
        self.assertEqual(transfer.reservation.offset, mrs[0].location.offset)
        self.assertEqual(mrs[0].location.offset + mrs[0].location.length, mrs[1].location.offset)
        self.assertEqual(transfer.reservation.offset + len(payload),
                         mrs[-1].location.offset + mrs[-1].location.length)
        self.assertNotIn(("kv", "rack-b"), relay.objects)

    def test_multiple_senders_write_receiver_regions(self):
        for senders, receivers in [(1, 2), (2, 2), (4, 2), (2, 4)]:
            with self.subTest(senders=senders, receivers=receivers):
                relay, payload = self.setup_relay(senders, receivers)
                transfer = relay.prepare("kv", "rack-a", "target", "worker-0", 16)
                self.assertEqual(senders, len({r.sender for r in transfer.requests}))
                self.assertEqual(receivers, len({r.receiver for r in transfer.requests}))
                self.assertEqual(len(payload), sum(r.source.length for r in transfer.requests))
                for request in reversed(transfer.requests):
                    relay.complete_write(request)
                self.assertEqual(payload, relay.read(transfer.reservation))
                relay.publish(transfer)
                self.assertEqual(payload, relay.worker_read(transfer))
                self.assertEqual(b"prefix-in-target", relay.read(relay.objects["target-prefix", "rack-b"].location))

    def test_publish_then_worker_local_reuse(self):
        relay, payload = self.setup_relay()
        transfer = relay.prepare("kv", "rack-a", "target", "worker-3", 64)
        for request in transfer.requests[:-1]:
            relay.complete_write(request)
        self.assertNotIn(("kv", "rack-b"), relay.objects)
        self.assertEqual(1, relay.source_pins["kv", "rack-a"])
        relay.complete_write(transfer.requests[-1])
        relay.publish(transfer)
        self.assertEqual("COMPLETE", transfer.state)
        self.assertEqual(0, relay.source_pins["kv", "rack-a"])
        self.assertNotIn(transfer.transfer_id, relay.mrs)
        self.assertEqual(7, relay.objects["kv", "rack-b"].generation)
        writes = len([e for e in relay.events if e["event"] == "write_complete"])
        self.assertEqual(payload, relay.worker_read(transfer))
        self.assertEqual(payload, relay.worker_read(transfer))
        self.assertEqual(writes, len([e for e in relay.events if e["event"] == "write_complete"]))

    def test_multi_key_dispatch_and_correlated_completion(self):
        relay, payload = self.setup_relay()
        relay.seed("kv-next", "rack-a", payload[::-1])
        first = relay.prepare("kv", "rack-a", "target", "worker-0", 32)
        second = relay.prepare("kv-next", "rack-a", "target", "worker-1", 32)
        self.assertLessEqual(first.reservation.offset + first.reservation.length, second.reservation.offset)
        for left, right in zip(first.requests, second.requests):
            relay.complete_write(right)
            relay.complete_write(left)
        for transfer, expected in [(second, payload[::-1]), (first, payload)]:
            relay.publish(transfer)
            self.assertEqual(expected, relay.worker_read(transfer))
            events = [e for e in relay.events if e["transfer_id"] == transfer.transfer_id]
            names = [e["event"] for e in events]
            self.assertLess(names.index("master_targets_published"), names.index("requests_dispatched"))
            self.assertLess(max(i for i, name in enumerate(names) if name == "write_complete"),
                            names.index("destination_published"))
            self.assertEqual({transfer.worker}, {e["worker"] for e in events})
            self.assertEqual(len(payload), sum(e["bytes"] for e in events if e["event"] == "write_complete"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
