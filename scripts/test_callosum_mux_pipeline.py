"""T0: real dispatch/WRITE handlers and native address checks; fake RPC/DMA."""
from concurrent.futures import ThreadPoolExecutor
import ctypes
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from callosum_dispatch import SourceDispatcher
import callosum_runtime as runtime


class MuxPipelineTest(unittest.TestCase):
    def test_source_handler_scatter_two_senders_two_receivers_and_gather(self):
        nodes = [dict(id=n, pool=n[0].upper(), url='http://' + n,
                      can_send=n.startswith('b'), can_receive=n.startswith('a'))
                 for n in ('a1', 'a2', 'b1', 'b2')]
        config = dict(nodes=nodes, mode='rdma', batch_cap=1, max_inflight_bytes=32)
        buffers = {p: ctypes.create_string_buffer(8192) for p in ('A', 'B')}
        data = bytes(range(64))
        ctypes.memmove(ctypes.addressof(buffers['B']) + 4096, data, len(data))
        agents, calls = {}, []
        entered = {n: threading.Event() for n in ('b1', 'b2')}
        second_done, release = threading.Event(), threading.Event()

        class Engine:
            def __init__(self, name):
                self.name, self.count = name, 0

            def relay_write_batch_drained(self, segments, sources, targets, lengths):
                self.count += 1
                calls.append((self.name, list(segments), list(lengths)))
                if self.count == 1:
                    entered[self.name].set()
                    if self.name == 'b1' and not release.wait(5):
                        raise TimeoutError('test did not release the first sender')
                for src, dst, n in zip(sources, targets, lengths):
                    ctypes.memmove(dst, src, n)
                if self.name == 'b2' and self.count == 2:
                    second_done.set()
                return lengths

        for node in nodes:
            a = runtime.Agent.__new__(runtime.Agent)
            a.c, a.node, a.epoch, a.segment = config, node, node['id'] + '-epoch', node['id'] + ':19000'
            a.lock, a.targets, a.jobs = threading.Lock(), {}, {}
            a.log = SimpleNamespace(emit=lambda *args, **kwargs: None)
            a.mapping = SimpleNamespace(address=ctypes.addressof(buffers[node['pool']]),
                                        capacity=8192, bounds=lambda o, n: runtime.require(
                                            o >= 0 and n > 0 and o + n <= 8192, 'fixture bounds'))
            a.engine = Engine(node['id']) if node['can_send'] else None
            a.dispatcher = SourceDispatcher(a.send_group)
            agents[node['id']] = a
        targets = [agents[n].handle('prepare', dict(transfer_id='request-1',
                   extent=dict(pool='A', offset=4096 + i * 32, length=32)))
                   for i, n in enumerate(('a1', 'a2'))]
        writes = [dict(chunk=i, source=dict(pool='B', offset=4096 + i * 16, length=16),
                       destination=dict(pool='A', offset=4096 + i * 16, length=16),
                       target_index=i // 2, remote_address=targets[i // 2]['remote_base'] + (i % 2) * 16)
                  for i in range(4)]
        plan = dict(transfer_id='request-1', source=dict(pool='B', offset=4096, length=64),
                    source_version='generation-1', targets=targets,
                    sender_epochs={n: agents[n].epoch for n in ('b1', 'b2')},
                    senders=[dict(sender='b1', writes=writes), dict(sender='b2', writes=[])])

        def rpc(url, method, request, **kwargs):
            return agents[url.removeprefix('http://')].handle(method, request)

        with patch.object(runtime, 'rpc', side_effect=rpc), ThreadPoolExecutor() as executor:
            future = executor.submit(agents['b1'].handle, 'dispatch', plan)
            try:
                self.assertTrue(entered['b1'].wait(2) and entered['b2'].wait(2))
                self.assertTrue(second_done.wait(2))
                self.assertFalse(future.done())  # One sender cannot complete the gather.
                self.assertEqual(agents['b1'].dispatcher.status()['request-1'], 'DISPATCHED')
            finally:
                release.set()
            completions = future.result(timeout=5)
            self.assertEqual(sorted(c['chunk'] for c in completions), list(range(4)))
            self.assertTrue(all(c['bytes'] == 16 and c['success'] for c in completions))
            self.assertEqual(ctypes.string_at(ctypes.addressof(buffers['A']) + 4096, 64), data)
            self.assertEqual({name for name, _, _ in calls}, {'b1', 'b2'})
            self.assertEqual(len(calls), 4)  # batch_cap=1 is honored per sender.
            self.assertEqual({segment for _, segments, _ in calls for segment in segments},
                             {'a1:19000', 'a2:19000'})
            # A repeated dispatch joins the original result; it posts no writer.
            self.assertEqual(agents['b1'].handle('dispatch', plan), completions)
            self.assertEqual(len(calls), 4)

    def test_fifo_admission_does_not_block_independent_completion(self):
        first_entered, release, third_entered = threading.Event(), threading.Event(), threading.Event()
        order = []

        def send(plan, group):
            identity = plan['transfer_id']
            order.append(identity)
            if identity == 'first':
                first_entered.set()
                if not release.wait(5):
                    raise TimeoutError('test did not release first')
            if identity == 'third':
                third_entered.set()
            return [dict(chunk=0, bytes=1, success=True)]

        dispatcher = SourceDispatcher(send, capacity=2)
        def run(identity):
            return dispatcher.execute(dict(transfer_id=identity, senders=[dict(sender='b1')]))
        with ThreadPoolExecutor(max_workers=3) as executor:
            first = executor.submit(run, 'first')
            try:
                self.assertTrue(first_entered.wait(2))
                executor.submit(run, 'second').result(timeout=2)
                third = executor.submit(run, 'third')
                self.assertTrue(third_entered.wait(2))
                self.assertFalse(first.done())
                third.result(timeout=2)
            finally:
                release.set()
                first.result(timeout=5)
        self.assertEqual(order, ['first', 'second', 'third'])


if __name__ == '__main__':
    unittest.main()
