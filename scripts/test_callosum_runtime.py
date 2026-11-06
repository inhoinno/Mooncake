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
