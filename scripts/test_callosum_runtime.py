"""Two/four worker processes + native coordinator; file copies, NOT RDMA acceptance."""
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import shutil
import time
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace

import callosum_runtime as runtime


class RuntimeTest(unittest.TestCase):
    def test_initiation_native_file_path(self):
        import ctypes
        import hashlib
        from callosum_initiation import (
            CacheMiss, Delivery, Demand, Intent, Target, WorkerInitiator,
        )

        def exercise(c, root):
            cfg = runtime.config(root / 'config.json')
            source = runtime.rpc(c['master'], 'seed', dict(node='n2',
                key='initiation-object', length=65537))
            # This test owns both temporary files. Do not relax Mapping's guard.
            with patch.dict(os.environ, CALLOSUM_DESTRUCTIVE_ACK='1'):
                mapping = runtime.Mapping(cfg, cfg['nodes'][0])
            owner = ctypes.create_string_buffer(source['length'])
            target = Target(ctypes.addressof(owner), source['length'], 'host-allocation-1', owner)
            calls = []

            def deliver(intent, demand, destination):
                calls.append(demand.key)
                fetched = runtime.rpc(c['master'], 'fetch', dict(key=demand.key,
                    requester='n1', worker='n1', request_id=intent.request_id,
                    tp_rank=demand.tp_rank, chunk_bytes=4096))
                self.assertEqual(fetched['length'], demand.length)
                self.assertEqual(fetched['version'], demand.source_generation)
                self.assertEqual(mapping.digest(fetched['offset'], demand.length), fetched['checksum'])
                ctypes.memmove(destination.address, mapping.address+fetched['offset'], demand.length)
                self.assertEqual(hashlib.sha256(owner.raw).hexdigest(), source['checksum'])
                return Delivery('host_validated', fetched)

            worker = WorkerInitiator('n1', 'worker-incarnation-1', deliver)
            intent = Intent('routed-fixture', 'opaque-fixture-compatibility', 'n1', worker.epoch, 1)
            worker.accept(intent)
            self.assertEqual(calls, [])  # No allocations/transfer on routing intent.
            demand = Demand('block0-rank0', 'initiation-object', 'contiguous-host-fixture',
                            source['length'], source_generation=source['version'])
            before = len(runtime.rpc(c['master'], 'status')['operations'])
            first = worker.load(intent, demand, target)
            duplicate = worker.load(intent, demand, target)
            self.assertEqual(first, duplicate)
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(runtime.rpc(c['master'], 'status')['operations']), before+1)
            self.assertFalse(first.result['local_hit'])
            self.assertEqual(first.phase, 'host_validated')  # No fake GPU-ready.
            self.assertEqual(worker.status()[0]['validation_scope'], 'ADAPTER_ONLY')
            worker.release(intent, demand.consumer_id)
            missing = Demand('missing-block', 'not-present', 'host-fixture', source['length'])
            with self.assertRaises(CacheMiss):
                worker.load(intent, missing, target)
            self.assertEqual(worker.status()[1]['state'], 'MISS')
            worker.release(intent, missing.consumer_id)

        self.run_cluster(2, exercise=exercise)

    def test_status_progress_during_fetch(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            log=runtime.Log(root/'events.jsonl','master')
            c=dict(fingerprint='status-fixture',mode='local-file-test',pools={'A':dict(capacity=65536)},
                   nodes=[dict(id='a1',pool='A')])
            master=runtime.Master(c,root/'state.sqlite',log)
            master.topology_ok=True
            entered,release=threading.Event(),threading.Event()
            def waiting_fetch(data):
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('test did not release fetch')
                return dict(local_hit=False)
            master.fetch=waiting_fetch
            with ThreadPoolExecutor(max_workers=2) as executor:
                pending=executor.submit(master.handle,'fetch',dict(key='K',requester='a1'))
                try:
                    self.assertTrue(entered.wait(5))
                    status=executor.submit(master.handle,'status',{}).result(timeout=2)
                    self.assertTrue(status['topology_verified'])
                finally:
                    release.set()
                    pending.result(timeout=5)
                    master.db.close()
                    log.file.close()

    def test_transport_baseline_exact_bytes_and_units(self):
        from callosum_transport_benchmark import SIZES, TOTAL_BYTES, block_bytes, write_loop
        for unit in ('MiB','MB'):
            for size in SIZES:
                block=block_bytes(size,unit)
                posted=[]
                def write(src,dst,n):
                    self.assertGreaterEqual(src,0x1000)
                    self.assertLessEqual(src+n,0x1000+block)
                    self.assertEqual(dst-src,0x100000000)
                    posted.append(n)
                    return n
                result=write_loop(write,0x1000,0x100001000,block,TOTAL_BYTES,64*1024**2)
                self.assertEqual(result['bytes'],8589934592)
                self.assertEqual(sum(posted),8589934592)
                self.assertEqual(result['native_calls'],len(posted))
                self.assertEqual(result['logical_blocks'],(TOTAL_BYTES+block-1)//block)
        with self.assertRaisesRegex(RuntimeError,'short'):
            write_loop(lambda s,d,n:n-1,4096,8192,64,128,32)
        with self.assertRaises(ValueError):
            block_bytes(16,'unknown')
        large=write_loop(lambda s,d,n:n,4096,2**40,512*1024**2,TOTAL_BYTES,512*1024**2)
        self.assertEqual(large['native_calls'],16)

    def test_transport_baseline_reservation_and_final_validation(self):
        from callosum_transport_benchmark import TOTAL_BYTES
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            log=runtime.Log(root/'events.jsonl','master')
            nodes=[dict(id=n,pool=p,url='http://'+n,capacity=2**30,can_send=True,
                        can_receive=True,configured_qps='16')
                   for n,p in [('a1','A'),('b1','B')]]
            c=dict(fingerprint='transport-fixture',mode='rdma',
                   pools={p:dict(capacity=2**30) for p in ('A','B')},nodes=nodes)
            master=runtime.Master(c,root/'state.sqlite',log)
            master.topology_ok=True
            master.members=lambda:nodes
            source=dict(key='source',pool='B',offset=4096,length=512*1024**2,
                        version='v1',checksum='full-source-checksum')
            master.db.execute('INSERT INTO objects VALUES (?,?,?)',('source','B',json.dumps(source)))
            master.db.commit()
            def network(url,method,data,**kwargs):
                if method=='prepare':
                    return dict(receiver='a1',extent=data['extent'],plan_id=data['transfer_id'])
                if method=='transport_write_loop':
                    return dict(bytes=TOTAL_BYTES,block_bytes=data['block_bytes'],
                                sender_write_loop_ns=1000)
                if method=='digest':
                    self.assertEqual(data['length'],1024**2)
                    return 'window-checksum'
                self.fail('unexpected baseline RPC '+method)
            try:
                with patch.object(runtime,'rpc',side_effect=network):
                    result=master.handle('transport_benchmark',dict(node='b1',receiver='a1',
                        key='source',block_size=1,block_unit='MiB'))
                self.assertEqual(result['bytes'],TOTAL_BYTES)
                self.assertFalse(result['active_qps_verified'])
                self.assertEqual(master.db.execute('SELECT state FROM operations').fetchone()[0],
                                 'DRAINED_RETAINED')
                self.assertEqual(master.db.execute("SELECT count(*) FROM objects WHERE pool='A'").fetchone()[0],0)
            finally:
                master.db.close()
                log.file.close()

    def test_scatter_tensor_projection_and_hybrid_readiness(self):
        import ctypes
        from callosum_relay_native import project_scatter, ReadinessLedger
        sources = [ctypes.create_string_buffer(bytes(range(7)),7),
                   ctypes.create_string_buffer(bytes(range(7,17)),10)]
        destinations = [ctypes.create_string_buffer(5),ctypes.create_string_buffer(12)]
        src = [[0, ctypes.addressof(sources[0]), 7], [7, ctypes.addressof(sources[1]), 10]]
        dst = [[0, ctypes.addressof(destinations[0]), 5], [5, ctypes.addressof(destinations[1]), 12]]
        rows = project_scatter(src, dst, 17, 0, 17, 4)
        source = bytearray(range(17))
        copied = bytearray(17)
        for logical, s, d, n in rows:
            se = next(e for e in src if e[1] <= s < e[1]+e[2])
            de = next(e for e in dst if e[1] <= d < e[1]+e[2])
            self.assertEqual(se[0]+s-se[1], logical)
            self.assertEqual(de[0]+d-de[1], logical)
            copied[logical:logical+n] = source[logical:logical+n]
            ctypes.memmove(d,s,n)
        self.assertEqual(copied, source)
        self.assertEqual(b''.join(d.raw for d in destinations),bytes(source))
        for bad in ([[0, 0x8000, 5], [6, 0x9000, 11]],
                    [[0, 0x8000, 5], [5, 0x8001, 12]]):
            with self.assertRaises(ValueError):
                project_scatter(src, bad, 17, 0, 17, 4)
        ledger = ReadinessLedger('attempt-1', 'generation-1', 17)
        ledger.assign('direct', 0, 7, False)
        ledger.assign('helper', 7, 10, True)
        # Helper transport is complete but local GPU copy remains pending.
        for phase in ('transport', 'validated'):
            ledger.event('attempt-1', 'generation-1', 'helper', phase)
        with self.assertRaises(ValueError):
            ledger.event('attempt-1', 'generation-1', 'helper', 'gpu_ready')
        self.assertFalse(ledger.ready(17))
        for phase in ('transport', 'validated', 'gpu_ready'):
            ledger.event('attempt-1', 'generation-1', 'direct', phase)
        self.assertTrue(ledger.ready(7))
        self.assertFalse(ledger.ready(17))
        with self.assertRaises(ValueError):
            ledger.event('old-attempt', 'generation-1', 'helper', 'copy_complete')
        with self.assertRaises(ValueError):
            ledger.event('attempt-1', 'old-generation', 'helper', 'copy_complete')
        ledger.event('attempt-1', 'generation-1', 'helper', 'copy_complete')
        ledger.event('attempt-1', 'generation-1', 'helper', 'gpu_ready')
        ledger.event('attempt-1', 'generation-1', 'helper', 'gpu_ready')
        self.assertTrue(ledger.ready(17))
        with self.assertRaises(ValueError):
            ledger.assign('conflict', 6, 1, True)
        prefix = ReadinessLedger('prefix','fixture-generation',4)
        for i in range(4):
            prefix.assign(f'K{i+1}',i,1,i>=2)
        for key in ('K4','K2','K1'):
            for phase in ('transport','validated'):
                prefix.event('prefix','fixture-generation',key,phase)
            if key=='K4':
                prefix.event('prefix','fixture-generation',key,'copy_complete')
            prefix.event('prefix','fixture-generation',key,'gpu_ready')
        self.assertTrue(prefix.ready(2))
        self.assertFalse(prefix.ready(4))  # K3 hole despite K4 completion.

    def test_receiver_rejects_cross_plan_alias(self):
        import threading
        agent=runtime.Agent.__new__(runtime.Agent)
        agent.node=dict(id='a1',pool='A',can_receive=True)
        agent.mapping=SimpleNamespace(address=0x100000,bounds=Mock())
        agent.lock=threading.Lock()
        agent.targets={}
        agent.segment,agent.epoch='a1-segment','epoch-a'
        first=dict(transfer_id='one',extent=dict(pool='A',offset=4096,length=8192))
        self.assertEqual(agent.handle('prepare',first),agent.handle('prepare',first))
        with self.assertRaisesRegex(RuntimeError,'aliases'):
            agent.handle('prepare',dict(transfer_id='two',
                         extent=dict(pool='A',offset=8192,length=8192)))
        self.assertNotIn('two',agent.targets)
        agent.handle('prepare',dict(transfer_id='three',
                     extent=dict(pool='A',offset=12288,length=4096)))

    def test_source_dispatch_fcfs_and_idempotent_completion(self):
        import threading
        from callosum_dispatch import SourceDispatcher
        entered, release = threading.Event(), threading.Event()
        calls = []
        def send(plan, group):
            calls.append(plan['transfer_id'])
            if plan['transfer_id'] == 'first':
                entered.set()
                self.assertTrue(release.wait(5))
            return [dict(chunk=0, bytes=1, success=True)]
        dispatch = SourceDispatcher(send, capacity=1)
        def plan(name):
            return dict(transfer_id=name, senders=[dict(sender='b1')])
        errors = []
        def run(name):
            try:
                dispatch.execute(plan(name))
            except Exception as e:
                errors.append(e)
        first = threading.Thread(target=run,args=('first',))
        second = threading.Thread(target=run,args=('second',))
        third = threading.Thread(target=run,args=('third',))
        first.start()
        try:
            self.assertTrue(entered.wait(5))
            second.start()
            deadline=time.monotonic()+5
            while dispatch.status().get('second') != 'QUEUED' and time.monotonic()<deadline:
                time.sleep(.001)
            self.assertEqual(dispatch.status().get('second'), 'QUEUED')
            third.start()
        finally:
            release.set()
            for thread in (first,second,third):
                if thread.ident is not None:
                    thread.join(5)
        self.assertFalse(errors)
        self.assertEqual(calls,['first','second','third'])
        self.assertEqual(dispatch.execute(plan('first'))[0]['bytes'],1)
        self.assertEqual(len(calls),3)
        with self.assertRaisesRegex(RuntimeError,'conflicting'):
            dispatch.execute(dict(plan('first'), changed=True))
        def fail(plan, group):
            raise TimeoutError('possible posted writer')
        uncertain=SourceDispatcher(fail)
        with self.assertRaises(TimeoutError):
            uncertain.execute(plan('failed'))
        self.assertEqual(uncertain.status()['failed'],'QUARANTINED')
        with self.assertRaisesRegex(RuntimeError,'quarantined'):
            uncertain.execute(plan('failed'))

    def test_cxl_mapping_identity(self):
        from callosum_relay_native import same_cxl_mapping
        self.assertTrue(same_cxl_mapping('A',0,65536,'A',0,65536))
        self.assertFalse(same_cxl_mapping('A',0,65536,'B',0,65536))
        self.assertFalse(same_cxl_mapping('A',0,65536,'A',4096,65536))
        self.assertFalse(same_cxl_mapping('A',0,65536,'A',0,0))
        self.assertFalse(same_cxl_mapping('',0,65536,'',0,65536))

    def test_delivery_policy_fixtures(self):
        from callosum_relay_native import select_delivery
        for inputs, expected in [
            ((True,False,False,False,-1,-1,1,.1,1),'LOCAL'),
            ((False,False,True,True,-1,-1,1,.1,1),'UNAVAILABLE'),
            ((False,True,False,True,10,100,1,.1,1),'UNAVAILABLE'),
            ((False,True,True,True,-1,-1,1,.1,1),'TRANSFER'),
            ((False,True,True,False,-1,100,1,.1,1),'UNAVAILABLE'),
            ((False,True,True,False,10,100,1,.1,1),'TRANSFER'),
            ((False,True,True,False,99,100,1,.1,1),'RECOMPUTE')]:
            self.assertEqual(select_delivery(*inputs)['action'],expected)
        for bad in (float('nan'),float('inf'),-.5):
            with self.assertRaises(ValueError):
                select_delivery(False,True,True,False,bad,100,1,.1,1)

    def test_policy_rejection_before_reservation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            log=runtime.Log(root/'trace.jsonl','master')
            members=[dict(id=n,pool=p,capacity=65536,can_send=True,can_receive=True)
                     for n,p in [('a1','A'),('b1','B')]]
            c=dict(fingerprint='policy-fixture',pools={p:dict(capacity=65536) for p in ('A','B')},
                   nodes=members,policy=dict(forced=False,transfer_us=100,recompute_us=10))
            master=runtime.Master(c,root/'state.sqlite',log)
            source=dict(key='fixture',pool='B',offset=4096,length=4096,version='v1',checksum='fixture')
            master.db.execute('INSERT INTO objects VALUES (?,?,?)',('fixture','B',json.dumps(source)))
            master.db.commit()
            master.members=lambda:members
            with patch.object(runtime,'rpc') as network:
                with self.assertRaisesRegex(RuntimeError,'policy selected RECOMPUTE'):
                    master.fetch(dict(requester='a1',key='fixture'))
                network.assert_not_called()
            self.assertEqual(master.db.execute('SELECT count(*) FROM operations').fetchone()[0],0)
            self.assertEqual(master.db.execute("SELECT next FROM pools WHERE id='A'").fetchone()[0],4096)
            master.db.close()
            log.file.close()

    def test_native_batch_projection(self):
        from callosum_relay_native import validate_write_batch
        validate_write_batch(['a', 'b'], [4096, 8192], [65536, 65536], [4096, 17])
        for args in [([],[],[],[]), (['a'],[4096],[],[1]),
                     (['a','a'],[4096,8192],[65536,65537],[2,1]),
                     (['a'],[2**64-1],[65536],[2])]:
            with self.assertRaises(ValueError):
                validate_write_batch(*args)

    def test_sender_batch_cardinality_and_stale_target(self):
        for cap in (1,4,16):
            agent = runtime.Agent.__new__(runtime.Agent)
            agent.c = dict(batch_cap=cap, nodes=[dict(id='a1',pool='A',url='http://receiver')],
                           mode='rdma')
            agent.node = dict(id='b1',pool='B',can_send=True)
            agent.epoch, agent.jobs, agent.lock = 'boot-b', {}, __import__('threading').Lock()
            agent.mapping = SimpleNamespace(address=0x100000, bounds=Mock())
            agent.log = Mock()
            agent.engine = Mock()
            agent.engine.relay_write_batch_drained.side_effect = lambda s,a,b,l: l
            target = dict(receiver='a1',extent=dict(pool='A',offset=4096,length=17*4096),
                segment='receiver-segment',remote_base=0x800000,epoch='boot-a',
                address_kind='classic_va',plan_id='plan')
            writes = [dict(chunk=i,source=dict(pool='B',offset=4096+i*4096,length=4096),
                destination=dict(pool='A',offset=4096+i*4096,length=4096),target_index=0,
                remote_address=0x800000+i*4096) for i in range(17)]
            data = dict(transfer_id='plan',sender='b1',epoch='boot-b',targets=[target],
                        writes=writes,source_object_offset=4096)
            with patch.object(runtime, 'rpc', return_value=target):
                self.assertEqual(len(agent.write(data)),17)
            calls = agent.engine.relay_write_batch_drained.call_args_list
            self.assertEqual(len(calls), (17+cap-1)//cap)
            self.assertEqual(sum(len(c.args[0]) for c in calls),17)
            agent.jobs.clear()
            agent.engine.reset_mock()
            with patch.object(runtime, 'rpc', return_value=dict(target,epoch='restarted')):
                with self.assertRaisesRegex(RuntimeError, 'stale receiver'):
                    agent.write(data)
            agent.engine.relay_write_batch_drained.assert_not_called()

    def test_preflight_rejects_duplicate_members(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'config.json'
            path.write_text(json.dumps(dict(mode='rdma', pools={'a':{}, 'b':{}},
                                           nodes=[dict(id='same')]*4)))
            with self.assertRaisesRegex(RuntimeError, 'duplicate node'):
                runtime.config(path)

    def test_preflight_geometry_and_modes(self):
        import copy
        base = dict(mode='rdma',abi_version=2,path_mode='direct_dax_write',metadata='P2PHANDSHAKE',
            master='http://control:18000',pools={
                p:dict(capacity=64*1024**2,owner='fixture-owner',physical_pool_id=p,
                       mapping_alignment=2*1024**2) for p in ('A','B')},
            nodes=[dict(id=n,pool=p,url=f'http://{n}:18001',path='/dev/dax-test-only',
                        te_endpoint=f'{n}:19001',rdma_device='fixture-only',can_send=True,can_receive=True)
                   for n,p in [('a1','A'),('b1','B')]])
        mutations = [lambda c:c.update(path_mode='staged_write'),
                     lambda c:c.update(abi_version=1),
                     lambda c:c.update(evidence_class='STORE_COMPATIBLE'),
                     lambda c:c['pools']['A'].update(map_offset=4096),
                     lambda c:c['pools']['A'].update(capacity=2**63),
                     lambda c:c['pools']['A'].update(owner=''),
                     lambda c:c['pools']['B'].update(physical_pool_id='A'),
                     lambda c:c.update(batch_cap=0),
                     lambda c:c.update(policy={'force':True}),
                     lambda c:c['nodes'][1].update(pool='A')]
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'manifest.json'
            path.write_text(json.dumps(base))
            self.assertEqual(runtime.config(path)['abi_version'],2)
            for mutate in mutations:
                bad=copy.deepcopy(base)
                mutate(bad)
                path.write_text(json.dumps(bad))
                with self.assertRaises(RuntimeError):
                    runtime.config(path)

    def test_failure_restart_quarantines_uncertain_operations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = runtime.Log(root/'log.jsonl', 'master')
            c = dict(fingerprint='test', pools={'a':dict(capacity=65536)})
            m = runtime.Master(c, root/'state.sqlite', log)
            extent = m.allocate('a', 4096)
            m.operation('inflight', 'DISPATCHED', dict(destination=extent))
            m.db.close()
            restored = runtime.Master(c, root/'state.sqlite', log)
            self.assertEqual(restored.db.execute('SELECT state FROM operations').fetchone()[0],
                             'QUARANTINED')
            self.assertGreater(restored.allocate('a', 4096)['offset'], extent['offset'])
            self.assertEqual(restored.db.execute('SELECT count(*) FROM objects').fetchone()[0], 0)
            restored.db.close()
            log.file.close()

    def test_timeout_late_writer_quarantine(self):
        import threading
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            log=runtime.Log(root/'events.jsonl','master')
            members=[dict(id=n,pool=p,capacity=65536,can_send=True,can_receive=True,epoch=n)
                     for n,p in [('a1','A'),('b1','B')]]
            c=dict(fingerprint='late-writer-fixture',pools={p:dict(capacity=65536) for p in ('A','B')},
                   nodes=[dict(m,url='http://'+m['id']) for m in members])
            master=runtime.Master(c,root/'state.sqlite',log)
            source=dict(key='fixture',pool='B',offset=4096,length=4096,version='v1',checksum='fixture')
            master.db.execute('INSERT INTO objects VALUES (?,?,?)',('fixture','B',json.dumps(source)))
            master.db.commit()
            master.members=lambda: members
            release=threading.Event()
            finished=threading.Event()
            target_bytes=bytearray(65536)
            writers=[]

            def control(url,method,data,**kwargs):
                if method=='dispatch':
                    group=data['senders'][0]
                    return control(url,'write',dict(group,targets=data['targets']))
                if method=='prepare':
                    return dict(receiver='a1',extent=data['extent'],segment='a1',remote_base=0x100000,
                                epoch='a1',address_kind='classic_va',plan_id=data['transfer_id'])
                if method=='write':
                    def late_write():
                        release.wait()
                        for w in data['writes']:
                            offset=w['destination']['offset']
                            length=w['destination']['length']
                            target_bytes[offset:offset+length]=b'L'*length
                        finished.set()
                    thread=threading.Thread(target=late_write)
                    thread.start()
                    writers.append(thread)
                    raise TimeoutError('caller deadline; writer still active')
                self.fail('unexpected RPC after uncertain WRITE: '+method)

            try:
                with patch.object(runtime,'rpc',side_effect=control):
                    with self.assertRaisesRegex(TimeoutError,'writer still active'):
                        master.fetch(dict(requester='a1',key='fixture',chunk_bytes=4096))
                state,record=master.db.execute('SELECT state,record FROM operations').fetchone()
                reserved=json.loads(record)['destination']
                self.assertEqual(state,'QUARANTINED')
                self.assertFalse(finished.is_set())
                next_extent=master.allocate('A',4096)
                self.assertGreaterEqual(next_extent['offset'],reserved['offset']+reserved['length'])
                release.set()
                self.assertTrue(finished.wait(5))
                self.assertEqual(target_bytes[next_extent['offset']:next_extent['offset']+4096],bytes(4096))
                self.assertEqual(master.db.execute("SELECT count(*) FROM objects WHERE pool='A'").fetchone()[0],0)
                self.assertEqual(master.db.execute('SELECT state FROM operations').fetchone()[0],'QUARANTINED')
            finally:
                release.set()
                for thread in writers:
                    thread.join(5)
                master.db.close()
                log.file.close()

    def test_status_preserves_coordinator_and_sink_clocks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'events.jsonl'
            log = runtime.Log(path, 'master')
            log.emit('published', monotonic_ns=123, transfer_id='x', request_id='r')
            log.file.close()
            event = json.loads(path.read_text())
            self.assertEqual(event['coordinator_monotonic_ns'], 123)
            self.assertGreater(event['monotonic_ns'], 123)
            self.assertEqual(event['request_id'], 'r')

    def test_four_workers_transfer_reuse_and_journal(self):
        self.run_cluster(4)

    def test_two_workers_forward_reverse_batch_and_reuse(self):
        self.run_cluster(2)

    def run_cluster(self, workers, exercise=None):
        with tempfile.TemporaryDirectory(prefix='callosum-runtime-') as directory:
            root = Path(directory)
            sockets = []
            for _ in range(workers+1):
                sock = socket.socket()
                sock.bind(('127.0.0.1', 0))
                sockets.append(sock)
            urls = [f'http://127.0.0.1:{s.getsockname()[1]}' for s in sockets]
            for s in sockets:
                s.close()
            capacity = 64*1024**2 if workers == 2 else 2*1024**2
            for pool in ('p1', 'p2'):
                with open(root / pool, 'wb') as f:
                    f.truncate(capacity)
            c = dict(mode='local-file-test', master=urls[0], pools={
                p:dict(capacity=capacity) for p in ('p1','p2')}, nodes=[])
            for i in range(1, workers+1):
                pool = 'p1' if i <= workers//2 else 'p2'
                c['nodes'].append(dict(id=f'n{i}', pool=pool, url=urls[i],
                    path=str(root/pool), can_send=True, can_receive=True))
            cfg = root / 'config.json'
            cfg.write_text(json.dumps(c))
            env = dict(os.environ, CALLOSUM_CONTROL_TOKEN='functional-test-token-'*3,
                       CALLOSUM_DESTRUCTIVE_ACK='1')
            old_token = os.environ.get('CALLOSUM_CONTROL_TOKEN')
            os.environ['CALLOSUM_CONTROL_TOKEN'] = env['CALLOSUM_CONTROL_TOKEN']
            processes, logs = [], []

            def launch(role, node=None):
                out = open(root / f'{role}-{node}.log', 'w')
                logs.append(out)
                cmd = [sys.executable, runtime.__file__, role, '--config', str(cfg),
                       '--state-dir', str(root/'state')]
                if node:
                    cmd += ['--node', node]
                p = subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.STDOUT)
                processes.append(p)

            def retry(call):
                error = None
                for _ in range(150):
                    try:
                        return call()
                    except Exception as e:
                        error = e
                        time.sleep(.05)
                details = '\n'.join(p.read_text() for p in root.glob('*.log'))
                self.fail(f'{error}\n{details}')

            try:
                launch('master')
                retry(lambda: runtime.rpc(urls[0], 'status'))
                for i in range(1,workers+1):
                    launch('agent', f'n{i}')
                retry(lambda: runtime.rpc(urls[0], 'preflight'))
                seed = runtime.rpc(urls[0], 'seed', dict(node=f'n{workers}', key='kv-a', length=65537))
                result = runtime.rpc(urls[1], 'consume', dict(key='kv-a', chunk_bytes=4096,
                                                            request_id='req-1'))
                self.assertEqual(result['checksum'], seed['checksum'])
                self.assertEqual((result['senders'], result['receivers']), (workers//2,workers//2))
                self.assertFalse(result['local_hit'])
                dispatch_status = runtime.rpc(urls[workers//2+1], 'dispatch_status')
                self.assertEqual(dispatch_status[result['transfer_id']], 'DRAINED')
                # Non-requesting helper observes the same published object.
                reuse = runtime.rpc(urls[workers//2], 'consume', dict(key='kv-a'))
                self.assertTrue(reuse['local_hit'])
                self.assertEqual(reuse['checksum'], seed['checksum'])
                status = runtime.rpc(urls[0], 'status')
                self.assertEqual([o['state'] for o in status['operations']], ['COMMITTED'])
                events = [json.loads(x) for x in (root/'state/master-None.jsonl').read_text().splitlines()]
                stages = [e['stage'] for e in events]
                self.assertLess(stages.index('verified'), stages.index('published'))
                self.assertTrue(any(e.get('request_id') == 'req-1' and e['stage']=='published' for e in events))
                import validate_relay_trace
                trace = events + [json.loads(line) for path in (root/'state').glob('agent-*.jsonl')
                                  for line in path.read_text().splitlines()]
                verdict = validate_relay_trace.validate(trace, result['transfer_id'],65537,'file-copy')
                self.assertEqual(verdict['status'],'PASS')
                self.assertFalse(verdict['serving_proven'])
                with self.assertRaisesRegex(ValueError, 'transport'):
                    validate_relay_trace.validate(trace,result['transfer_id'],65537,'rdma')
                corrupted_trace = list(trace)
                corrupted_trace.append(next(e for e in trace if e['stage']=='chunk_complete'))
                with self.assertRaisesRegex(ValueError, 'duplicate chunk'):
                    validate_relay_trace.validate(corrupted_trace,result['transfer_id'],65537,'file-copy')
                # No absent-key publication or synthetic success.
                with self.assertRaisesRegex(RuntimeError, 'KV not found'):
                    runtime.rpc(urls[1], 'consume', dict(key='absent'))
                # A changed source cannot become a published destination.
                bad = runtime.rpc(urls[0], 'seed', dict(node=f'n{workers}', key='kv-corrupt', length=8192))
                with open(root/'p2', 'r+b') as f:
                    f.seek(bad['offset'])
                    f.write(b'corrupt!')
                with self.assertRaisesRegex(RuntimeError, 'SHA-256 mismatch'):
                    runtime.rpc(urls[1], 'consume', dict(key='kv-corrupt', chunk_bytes=4096))
                states = [o['state'] for o in runtime.rpc(urls[0], 'status')['operations']]
                self.assertEqual(states, ['COMMITTED', 'QUARANTINED'])
                with sqlite3.connect(root/'state/master.sqlite') as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM objects WHERE key='kv-corrupt' AND pool='p1'").fetchone()[0], 0)
                # A second member observes writes through its own mapping.
                with open(root/'p1', 'r+b') as f:
                    f.write(b'X'*32)
                self.assertEqual(runtime.rpc(urls[workers//2], 'marker'), (b'X'*32).hex())
                if workers == 2:
                    reverse = runtime.rpc(urls[0], 'seed', dict(node='n1',key='reverse',length=65539))
                    received = runtime.rpc(urls[2], 'consume',dict(key='reverse',chunk_bytes=4096))
                    self.assertEqual(reverse['checksum'],received['checksum'])
                    batch_events = [json.loads(line) for line in
                        (root/'state/agent-n2.jsonl').read_text().splitlines()
                        if json.loads(line)['stage']=='batch_complete']
                    first = [e for e in batch_events if e['transfer_id']==result['transfer_id']]
                    self.assertEqual(len(first),1)
                    self.assertEqual(first[0]['cardinality'],17)
                    self.assertEqual(first[0]['opcode'],'WRITE')
                    # Required fixture sizes, in both directions. Actual model
                    # block size remains a separate serving-environment gate.
                    for size in (4096,65536,1048576,16777216-4096):
                        for src,dst in ((2,1),(1,2)):
                            key=f'size-{size}-from-{src}'
                            expected=runtime.rpc(urls[0],'seed',dict(node=f'n{src}',key=key,length=size))
                            actual=runtime.rpc(urls[dst],'consume',dict(key=key))
                            self.assertEqual(actual['checksum'],expected['checksum'])
                    # Distinct Store-like opaque fixture keys, not a claim of
                    # batch_get_into or actual connector-key compatibility.
                    for count in (1,4,16):
                        for index in range(count):
                            key=f'key-batch-{count}-{index}'
                            expected=runtime.rpc(urls[0],'seed',dict(node='n2',key=key,length=4097))
                            actual=runtime.rpc(urls[1],'consume',dict(key=key,chunk_bytes=4096))
                            self.assertEqual(actual['checksum'],expected['checksum'])
                    # Concurrent duplicate demand must materialize once.
                    import concurrent.futures
                    runtime.rpc(urls[0],'seed',dict(node='n2',key='single-flight',length=65537))
                    before=len(runtime.rpc(urls[0],'status')['operations'])
                    with concurrent.futures.ThreadPoolExecutor(2) as executor:
                        results=list(executor.map(lambda _: runtime.rpc(urls[1],'consume',
                            dict(key='single-flight',chunk_bytes=4096)),range(2)))
                    self.assertEqual(sum(not r['local_hit'] for r in results),1)
                    self.assertEqual(len(runtime.rpc(urls[0],'status')['operations']),before+1)
                    if exercise is not None:
                        exercise(c, root)
                    # Stop only the owned file-model source process. The
                    # committed destination must not probe that source again.
                    processes[-1].terminate()
                    processes[-1].wait(timeout=10)
                    reused=runtime.rpc(urls[1],'consume',dict(key='kv-a'))
                    self.assertTrue(reused['local_hit'])
                    self.assertEqual(reused['checksum'],seed['checksum'])
            finally:
                for p in reversed(processes):
                    p.terminate()
                for p in processes:
                    p.wait(timeout=10)
                for f in logs:
                    f.close()
                if os.environ.get('M1_TEST_ARTIFACT_DIR'):
                    dest = Path(os.environ['M1_TEST_ARTIFACT_DIR']) / f'{workers}-workers'
                    dest.mkdir(parents=True,exist_ok=False)
                    for path in root.glob('*.log'):
                        shutil.copy2(path,dest/path.name)
                    shutil.copy2(cfg,dest/'config.json')
                    shutil.copytree(root/'state',dest/'state')
                if old_token is None:
                    os.environ.pop('CALLOSUM_CONTROL_TOKEN', None)
                else:
                    os.environ['CALLOSUM_CONTROL_TOKEN'] = old_token


if __name__ == '__main__':
    unittest.main()
