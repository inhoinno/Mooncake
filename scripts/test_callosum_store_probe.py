#!/usr/bin/env python3
"""T0 probe orchestration/report tests. Fake Store, no RDMA/Store integration claim."""
import copy
import ctypes
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import os
import stat

import callosum_store_probe as probe


class Replica:
    status = 'COMPLETE'

    def __init__(self, endpoint, size, protocol='rdma'):
        self.descriptor = SimpleNamespace(transport_endpoint=endpoint, size=size, protocol=protocol)

    def is_memory_replica(self):
        return True

    def get_memory_descriptor(self):
        return SimpleNamespace(buffer_descriptor=self.descriptor)


MODULE = SimpleNamespace(ReplicaStatus=SimpleNamespace(COMPLETE='COMPLETE'))


class Store:
    def __init__(self, c):
        self.c = c
        self.replicas = [Replica('192.0.2.2:17000', c['block_bytes'])]
        self.calls = []
        self.done = False
        self.corrupt = False

    def get_replica_desc(self, name):
        return self.replicas

    def is_exist(self, name):
        return int(self.done)

    def register_buffer(self, address, length):
        self.calls.append(('register', length))
        return 0

    def batch_get_into_multi_buffers(self, keys, buffers, sizes):
        self.calls.append(('get', keys, sizes))
        data = probe.payload(self.c)
        if self.corrupt:
            data = b'!' + data[1:]
        ctypes.memmove(buffers[0][0], data, len(data))
        if len(self.replicas) == 1:
            self.replicas.append(Replica('192.0.2.1:17001', len(data)))
        return [len(data)]

    def unregister_buffer(self, address):
        self.calls.append(('unregister',))
        return 0

    def put(self, name, value):
        self.done = True
        self.calls.append(('put', name))
        return 0


class ProbeTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        node = dict(local_hostname='192.0.2.1:19401', rdma_device='test-rnic',
                    extent_authorization='fixture only; not lab ownership', expected_store_sha256='a' * 64,
                    bridge=dict(pool_id='A', path='/dev/dax0.0', backend_kind='devdax', capacity=67108864,
                                mapping_offset=2097152, alignment=2097152, owned_offset=0,
                                owned_size=67108864, token_file='control.token'))
        peer = copy.deepcopy(node)
        peer['local_hostname'] = '192.0.2.2:19401'
        peer['bridge']['pool_id'] = 'B'
        self.config = dict(schema_version=1, run_id='fixture-1234', block_bytes=65539, wait_seconds=1,
                           master_address='192.0.2.1:50051', metadata='P2PHANDSHAKE',
                           source='sender', requester='receiver', nodes=dict(receiver=node, sender=peer))
        self.path = self.root / 'experiment.json'
        self.path.write_text(json.dumps(self.config))
        self.c = probe.load_config(self.path)

    def test_real_probe_requester_api_sequence_and_retained_backing_on_failure(self):
        store = Store(self.c)
        retained = []
        result = probe.requester(store, MODULE, self.c, retained)
        self.assertEqual([r['phase'] for r in result['loads']], ['materialize', 'local_reuse'])
        self.assertEqual(len([c for c in store.calls if c[0] == 'get']), 2)
        self.assertEqual(retained, [])
        self.assertEqual(store.calls[-2][0], 'unregister')
        self.assertTrue(store.done)
        bad = Store(self.c)
        bad.corrupt = True
        with self.assertRaisesRegex(ValueError, 'SHA-256'):
            probe.requester(bad, MODULE, self.c, retained)
        self.assertEqual(len(retained), 1)
        self.assertNotIn(('unregister',), bad.calls)
        self.assertFalse(bad.done)

    def test_preexisting_destination_and_tcp_fail_before_load(self):
        for rows in ([Replica('192.0.2.1:17000', self.c['block_bytes'])],
                     [Replica('192.0.2.2:17000', self.c['block_bytes'], 'tcp')]):
            store = Store(self.c)
            store.replicas = rows
            with self.assertRaises(ValueError):
                probe.requester(store, MODULE, self.c, [])
            self.assertEqual(store.calls, [])

    def receipts(self):
        store = Store(self.c)
        result = probe.requester(store, MODULE, self.c, [])
        base = dict(status='CLIENT_PATH_VALIDATED_NATIVE_REVIEW_REQUIRED', run_id=self.c['run_id'],
                    config_sha256=self.c['config_sha256'], store_sha256='a' * 64)
        source = dict(base, node='sender', result=dict(before=result['before'],
                      after=result['loads'][0]['replicas'], payload_sha256=result['payload_sha256']))
        requester = dict(base, node='receiver', result=result)
        return source, requester

    def logs(self):
        crc = probe.crc64(probe.key(self.c).encode())
        n = self.c['block_bytes']
        tx = [f'event=receiver_authorized key_crc={crc} attempt=x source_generation=11 target_generation=22 source_pool=B destination_pool=A bytes={n} receiver_epoch=epoch-a receiver=192.0.2.1:17001',
              f'event=write_drained attempt=x protocol=rdma opcode=WRITE source_generation=11 target_generation=22 batch_bytes={n} completed_end={n} native_write_ns=50 native_start_ns=140 native_end_ns=190 sender=192.0.2.2:17000',
              f'event=copy_verified attempt=x key_crc={crc} bytes={n}',
              f'event=replica_published key_crc={crc} source_generation=11 target_generation=22']
        tx[1:1] = [f'event=mux_queued attempt=x bytes={n} timestamp_ns=100 queue_wait_ns=0 dispatch_ns=0',
                    f'event=mux_admitted attempt=x bytes={n} timestamp_ns=120 queue_wait_ns=20 dispatch_ns=0']
        tx.insert(4, f'event=mux_gathered attempt=x bytes={n} timestamp_ns=200 queue_wait_ns=20 dispatch_ns=80')
        tx.insert(0, f'event=store_task_dispatch key_crc={crc} task=store-task-1')
        rx = [f'event=initial_local_miss key_crc={crc}', f'event=remote_materialization key_crc={crc} task=store-task-1',
              f'event=receiver_validated attempt=x bytes={n} content_ok=1 epoch=epoch-a',
              f'event=connector_load_complete key_crc={crc} success=1 bytes={n}',
              f'event=connector_load_complete key_crc={crc} success=1 bytes={n}']
        rx += [f'event=replica_usable key_crc={crc} phase=remote request_to_usable_replica_ns=1000',
               f'event=replica_usable key_crc={crc} phase=local request_to_usable_replica_ns=200']
        return tx, rx

    def verify(self, tx, rx, receipts=None):
        paths = [self.root / 'sender.log', self.root / 'receiver.log']
        for path, rows in zip(paths, (tx, rx)):
            path.write_text('\n'.join('component=callosum_store ' + row for row in rows))
        return probe.verify(self.c, *(receipts or self.receipts()), *paths)

    def test_complete_receipts_and_correlated_native_log_gate(self):
        tx, rx = self.logs()
        report = self.verify(tx, rx)
        self.assertEqual(report['status'], 'PASS')
        self.assertFalse(report['serving_proven'])
        self.assertFalse(report['gpu_delivery_proven'])
        self.assertEqual(probe.crc64(b'123456789'), 0x6C40DF5F0B497347)

    def test_incomplete_wrong_transport_wrong_generation_and_early_publication_rejected(self):
        tx, rx = self.logs()
        cases = [(tx[:1] + tx[2:], rx), (tx, rx[:2] + rx[3:]),
                 ([r.replace('protocol=rdma', 'protocol=tcp') for r in tx], rx),
                 ([r.replace('completed_end=65539', 'completed_end=65538') for r in tx], rx),
                 ([r.replace('target_generation=22', 'target_generation=23') if 'event=replica_published' in r else r for r in tx], rx),
                 ([tx[-1]] + tx[:-1], rx), (tx, rx[:-1]),
                 (tx, [r.replace('content_ok=1', 'content_ok=0') for r in rx]),
                 (tx + tx, rx), (tx, [r.replace('epoch=epoch-a', 'epoch=stale') for r in rx]),
                 ([r for r in tx if 'event=mux_' not in r], rx),
                 ([r.replace('task=store-task-1', 'task=other-task') for r in tx], rx)]
        for a, b in cases:
            with self.subTest(tx=a, rx=b), self.assertRaises(ValueError):
                self.verify(a, b)
        receipts = self.receipts()
        receipts[1]['store_sha256'] = 'b' * 64
        with self.assertRaisesRegex(ValueError, 'module'):
            self.verify(tx, rx, receipts)

    def test_configuration_and_first_run_do_not_depend_on_historical_paths(self):
        self.assertEqual(self.c['nodes']['sender']['bridge']['token_file'], str(self.root / 'control.token'))
        self.config['nodes']['sender']['local_hostname'] = '192.0.2.1:19402'
        self.path.write_text(json.dumps(self.config))
        with self.assertRaisesRegex(ValueError, 'physical hosts'):
            probe.load_config(self.path)
        missing = self.root / 'missing.json'
        self.assertEqual(probe.main(['check', '--config', str(missing), '--node', 'receiver',
                                     '--output', str(self.root / 'unused')]), 1)
        self.assertFalse((self.root / 'unused').exists())

    def test_report_cli_handoff_and_rerun_preserves_existing_evidence(self):
        tx, rx = self.logs()
        self.verify(tx, rx)
        receipts = self.receipts()
        args = ['verify', '--config', str(self.path), '--output', str(self.root / 'report')]
        for name, receipt in zip(('source-receipt', 'requester-receipt'), receipts):
            path = self.root / (name + '.json')
            path.write_text(json.dumps(receipt))
            args.extend(['--' + name, str(path)])
        args.extend(['--source-log', str(self.root / 'sender.log'),
                     '--requester-log', str(self.root / 'receiver.log')])
        self.assertEqual(probe.main(args), 0)
        self.assertTrue((self.root / 'report/requests.csv').is_file())
        summary = (self.root / 'report/summary.json').read_bytes()
        self.assertEqual(probe.main(args), 1)
        self.assertEqual((self.root / 'report/summary.json').read_bytes(), summary)
        self.assertFalse((self.root / 'report/failure.json').exists())

    def test_scoped_resource_prelude_changes_only_selected_acl_and_current_process(self):
        b = self.c['nodes']['sender']['bridge']
        sysroot = self.root / 'sys'
        geometry = sysroot / 'bus/dax/devices/dax0.0'
        geometry.mkdir(parents=True)
        (geometry / 'size').write_text(str(2**30))
        (geometry / 'align').write_text(str(2**21))
        output = self.root / 'preparation'
        output.mkdir()
        info = SimpleNamespace(st_mode=stat.S_IFCHR | 0o600, st_uid=0, st_rdev=1, st_ino=2)
        acl = 'user::rw-\ngroup::---\nother::---\n'
        commands = []
        def run(argv, **kwargs):
            commands.append(argv)
            return SimpleNamespace(stdout=acl)
        needed = b['owned_size'] + 16 * 1024**2 + self.c['block_bytes']
        with patch.object(probe.Path, 'lstat', return_value=info), \
             patch.object(probe.os, 'open', side_effect=[PermissionError('denied'), 100]), \
             patch.object(probe.os, 'close'), \
             patch.object(probe.subprocess, 'run', side_effect=run), \
             patch.object(probe.resource, 'getrlimit', side_effect=[(8*1024**2, 8*1024**2), (needed, needed)]):
            result = probe.prepare_resources(b, self.c['block_bytes'], output, authorize=True, sysroot=sysroot)
        self.assertEqual(result['status'], 'PASS')
        self.assertFalse(result['devices_mapped'])
        self.assertTrue(any(a[:3] == ['sudo', '-n', 'setfacl'] and b['path'] == a[-1] for a in commands))
        self.assertTrue(any(a[:5] == ['sudo', '-n', 'prlimit', '--pid', str(os.getpid())] for a in commands))
        self.assertFalse(any('chmod' in a or 'chown' in a for a in commands))
        self.assertEqual(len([a for a in commands if a == ['sudo', '-v']]), 1)

    def test_resource_permission_denial_records_no_mutation_without_authorization(self):
        b = self.c['nodes']['sender']['bridge']
        sysroot = self.root / 'sys'
        geometry = sysroot / 'bus/dax/devices/dax0.0'
        geometry.mkdir(parents=True)
        (geometry / 'size').write_text(str(2**30))
        (geometry / 'align').write_text(str(2**21))
        output = self.root / 'readonly-check'
        output.mkdir()
        info = SimpleNamespace(st_mode=stat.S_IFCHR | 0o600, st_uid=0, st_rdev=1, st_ino=2)
        with patch.object(probe.Path, 'lstat', return_value=info), \
             patch.object(probe.os, 'open', side_effect=PermissionError('denied')), \
             patch.object(probe.subprocess, 'run') as command:
            with self.assertRaisesRegex(ValueError, 'preparation required'):
                probe.prepare_resources(b, self.c['block_bytes'], output, sysroot=sysroot)
        command.assert_not_called()
        record = json.loads((output / 'resources.json').read_text())
        self.assertEqual(record['status'], 'FAIL')
        self.assertEqual(record['changes'], [])

    def test_root_and_unknown_node_do_not_create_evidence(self):
        output = self.root / 'not-created'
        args = ['check', '--config', str(self.path), '--output', str(output)]
        with patch.object(probe.os, 'geteuid', return_value=0):
            self.assertEqual(probe.main(args + ['--node', 'sender']), 1)
        self.assertEqual(probe.main(args + ['--node', '../wrong-node']), 1)
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
