"""Adapter contracts only. Fixture completion is not CUDA or Dynamo evidence."""
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest

from callosum_initiation import (
    CacheMiss, Delivery, Demand, Intent, Superseded, Target, WorkerInitiator,
)


class InitiationTest(unittest.TestCase):
    def setUp(self):
        self.intent = Intent('request', 'model-layout-tenant', 'a1', 'epoch1', 1)
        self.demand = Demand('block-0', 'opaque-key', 'rank0-layout', 4096)
        self.owner = object()
        self.target = Target(0x10000, 4096, 'allocation-generation-1', self.owner)

    def test_preflight_identity_and_delayed_allocation(self):
        calls = []
        worker = WorkerInitiator('a1', 'epoch1', lambda *args: calls.append(args))
        worker.accept(self.intent)
        worker.accept(self.intent)
        self.assertEqual(worker.status(), [])
        self.assertEqual(calls, [])  # Intent acceptance cannot start a WRITE.
        with self.assertRaises(Superseded):
            worker.accept(Intent('request', 'model-layout-tenant', 'a1', 'old-epoch', 2))
        with self.assertRaises(ValueError):
            Target(2**64-8, 16, 'overflow', self.owner)
        with self.assertRaises(ValueError):
            Demand('consumer', 'key', 'layout', 0)

    def test_function_duplicate_and_separate_consumers(self):
        entered, finish = threading.Event(), threading.Event()
        calls = []

        def deliver(*args):
            calls.append(args)
            entered.set()
            if not finish.wait(5):
                raise TimeoutError('fixture completion not released')
            return Delivery('host_validated', {'checksum': 'fixture'})

        worker = WorkerInitiator('a1', 'epoch1', deliver)
        worker.accept(self.intent)
        with ThreadPoolExecutor(2) as executor:
            first = executor.submit(worker.load, self.intent, self.demand, self.target)
            try:
                self.assertTrue(entered.wait(5))
                # Connector notification repeats the same accepted assignment.
                worker.accept(self.intent)
                duplicate = executor.submit(worker.load, self.intent, self.demand, self.target)
                self.assertEqual(worker.status()[0]['state'], 'LOADING')
            finally:
                finish.set()
            self.assertEqual(first.result(5), duplicate.result(5))
        self.assertEqual(len(calls), 1)
        worker.load(self.intent, self.demand, self.target)
        self.assertEqual(len(calls), 1)
        second = Demand('block-1', self.demand.key, self.demand.layout_id, 4096)
        other_target = Target(0x20000, 4096, 'allocation-generation-2', object())
        worker.load(self.intent, second, other_target)
        self.assertEqual(len(calls), 2)  # Same source key is not the same consumer.
        worker.release(self.intent, 'block-0')
        worker.release(self.intent, 'block-0')
        with self.assertRaises(Superseded):
            worker.load(self.intent, self.demand, self.target)

    def test_failure_reroute_before_post(self):
        calls = []
        worker = WorkerInitiator('a1', 'epoch1', lambda *args: calls.append(args))
        worker.accept(self.intent)
        newer = Intent('request', 'model-layout-tenant', 'a1', 'epoch1', 2)
        worker.accept(newer)
        with self.assertRaises(Superseded):
            worker.load(self.intent, self.demand, self.target)
        self.assertEqual(calls, [])
        worker.cancel(newer)
        worker.cancel(newer)
        with self.assertRaises(Superseded):
            worker.accept(newer)

    def test_failure_cancel_during_delivery_and_late_completion(self):
        entered, finish = threading.Event(), threading.Event()

        def deliver(*args):
            entered.set()
            if not finish.wait(5):
                raise TimeoutError('fixture completion not released')
            return Delivery('host_validated', {})

        worker = WorkerInitiator('a1', 'epoch1', deliver)
        worker.accept(self.intent)
        newer = Intent('request', 'model-layout-tenant', 'a1', 'epoch1', 2)
        with ThreadPoolExecutor(1) as executor:
            pending = executor.submit(worker.load, self.intent, self.demand, self.target)
            try:
                self.assertTrue(entered.wait(5))
                worker.accept(newer)
                self.assertEqual(worker.status()[0]['state'], 'CANCEL_PENDING')
                with self.assertRaisesRegex(RuntimeError, 'uncertain'):
                    worker.release(self.intent, self.demand.consumer_id)
                with self.assertRaisesRegex(ValueError, 'aliases'):
                    worker.load(newer, self.demand, self.target)
            finally:
                finish.set()
            with self.assertRaises(Superseded):
                pending.result(5)
        self.assertEqual(worker.status()[0]['state'], 'CANCELLED_DRAINED')
        self.assertTrue(worker.status()[0]['retained'])
        worker.release(self.intent, self.demand.consumer_id)
        worker.load(newer, self.demand, self.target)
        self.assertEqual(worker.status()[1]['state'], 'READY')

    def test_failure_uncertainty_alias_and_invalid_evidence(self):
        for failure in (TimeoutError('completion unknown'), RuntimeError('partial post')):
            def deliver(*args):
                raise failure
            worker = WorkerInitiator('a1', 'epoch1', deliver)
            worker.accept(self.intent)
            with self.assertRaises(type(failure)):
                worker.load(self.intent, self.demand, self.target)
            self.assertEqual(worker.status()[0]['state'], 'QUARANTINED')
            with self.assertRaisesRegex(RuntimeError, 'uncertain'):
                worker.release(self.intent, 'block-0')
            alias = Demand('another-consumer', 'other-key', 'rank0-layout', 4096)
            with self.assertRaisesRegex(ValueError, 'aliases'):
                worker.load(self.intent, alias, self.target)
        worker = WorkerInitiator('a1', 'epoch1', lambda *args: {'ack': True})
        worker.accept(self.intent)
        with self.assertRaisesRegex(RuntimeError, 'completion evidence'):
            worker.load(self.intent, self.demand, self.target)
        self.assertEqual(worker.status()[0]['state'], 'QUARANTINED')

    def test_function_miss_and_changed_replay(self):
        calls = []
        def miss(*args):
            calls.append(args)
            raise CacheMiss('authoritative miss before submission')
        worker = WorkerInitiator('a1', 'epoch1', miss)
        worker.accept(self.intent)
        for _ in range(2):
            with self.assertRaises(CacheMiss):
                worker.load(self.intent, self.demand, self.target)
        self.assertEqual(len(calls), 1)
        changed = Demand('block-0', 'another-key', 'rank0-layout', 4096)
        with self.assertRaisesRegex(ValueError, 'replay changed'):
            worker.load(self.intent, changed, self.target)
        self.assertEqual(worker.status()[0]['state'], 'MISS')
        worker.release(self.intent, 'block-0')  # No transport or target access occurred.

    def test_function_release_miss_owner(self):
        import gc
        import weakref
        class Allocation:
            pass
        owner = Allocation()
        reference = weakref.ref(owner)
        destination = Target(0x30000, 4096, 'miss-allocation', owner)
        def miss(*args):
            raise CacheMiss('missing before submission')
        worker = WorkerInitiator('a1', 'epoch1', miss)
        worker.accept(self.intent)
        # assertRaises clears traceback frames and would hide this regression.
        try:
            worker.load(self.intent, self.demand, destination)
        except CacheMiss:
            pass
        else:
            self.fail('expected an authoritative miss')
        del destination, owner
        self.assertIsNotNone(reference())
        worker.release(self.intent, self.demand.consumer_id)
        gc.collect()
        self.assertIsNone(reference())  # No cached traceback may retain this owner.

    def test_status_bounds_and_completion_phase(self):
        worker = WorkerInitiator('a1', 'epoch1',
            lambda *args: Delivery('host_validated', {}), max_entries=1)
        worker.accept(self.intent)
        worker.load(self.intent, self.demand, self.target)
        record = worker.status()[0]
        self.assertEqual(record['validation_scope'], 'ADAPTER_ONLY')
        self.assertEqual(record['phase'], 'host_validated')
        self.assertEqual(record['worker_epoch'], 'epoch1')
        self.assertGreater(record['elapsed_ns'], 0)
        self.assertNotIn('address', record)
        with self.assertRaisesRegex(RuntimeError, 'ledger full'):
            worker.accept(Intent('another', 'context', 'a1', 'epoch1', 1))

    def test_failure_worker_checks_before_cuda(self):
        from unittest.mock import Mock, patch
        from callosum_worker import PoolWorker
        worker = PoolWorker.__new__(PoolWorker)
        worker.config, worker.node = {'master': 'unused'}, {'id': 'a1'}
        worker.mapping, worker.cuda = Mock(), Mock()
        fetched = dict(length=4096, version='source-v1')
        with patch('callosum_worker.rpc', return_value=fetched):
            with self.assertRaisesRegex(RuntimeError, 'generation differs'):
                worker.load_into('key', self.target.address, 4096, 'request',
                                 expected_length=4096, expected_version='source-v2')
            with self.assertRaisesRegex(RuntimeError, 'length differs'):
                worker.load_into('key', self.target.address, 8192, 'request', expected_length=8192)
        worker.cuda.copy.assert_not_called()
        worker.mapping.digest.assert_not_called()

    def test_function_worker_adapter_reuses_loader(self):
        from unittest.mock import Mock
        from callosum_worker import PoolWorker
        worker = PoolWorker.__new__(PoolWorker)
        worker.epoch, worker.log = 'epoch1', Mock()
        worker.load_into = Mock(return_value={'transfer_id': 'fixture-completion'})
        delivery = worker._deliver_intent(self.intent, self.demand, self.target)
        worker.load_into.assert_called_once_with(self.demand.key, self.target.address,
            self.target.capacity, self.intent.request_id, tp_rank=0,
            expected_length=4096, expected_version='')
        self.assertEqual(delivery.phase, 'gpu_ready')  # Loader fixture, NOT CUDA evidence.
        self.assertFalse(worker.log.emit.call_args.kwargs['serving_proven'])


if __name__ == '__main__':
    unittest.main()
