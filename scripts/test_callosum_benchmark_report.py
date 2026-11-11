"""Deterministic timing/accounting/report tests; no network performance evidence."""
import csv
import ctypes
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from callosum_transport_benchmark import write_loop, access_strategy, written_ranges, final_window_digest
from callosum_benchmark_report import enrich_trial, write_report


class BenchmarkReportTest(unittest.TestCase):
    def test_strategy_offsets_wrap_random_reproducibility_and_partial_tail(self):
        for pattern in ('overwrite','sequential','uniform_random'):
            sequences=[]
            for repeat in range(2):
                posted=[]; source=bytes(range(1,17)); target=bytearray(16)
                def write(src,dst,n):
                    self.assertEqual(dst-src,1000)
                    begin=src-1000; self.assertTrue(0<=begin<16 and begin+n<=16)
                    target[begin:begin+n]=source[begin:begin+n]; posted.append((begin,n)); return n
                row=write_loop(write,1000,2000,4,39,2,access_pattern=pattern,window_bytes=16,random_seed=17)
                self.assertEqual(row['bytes'],39)
                offsets=[r['source_offset_bytes'] for r in row['requests']]
                self.assertEqual([r['request_bytes'] for r in row['requests']], [4]*9+[3])
                if pattern=='overwrite': self.assertEqual(offsets,[0]*10)
                if pattern=='sequential': self.assertEqual(offsets,[0,4,8,12,0,4,8,12,0,4])
                ranges=written_ranges(row['requests'],16)
                import hashlib
                self.assertEqual(hashlib.sha256(target).hexdigest(),final_window_digest(source,16,ranges))
                sequences.append(offsets)
            self.assertEqual(sequences[0],sequences[1])
        strategy=access_strategy('uniform_random',4,16,42)
        values=[strategy.offset(i) for i in range(1000)]
        self.assertEqual(set(values),{0,4,8,12})
        self.assertTrue(all(180<values.count(n)<320 for n in set(values)))
        for pattern,block,window,seed in [('bad',4,16,0),('sequential',8,4,0),('uniform_random',4,16,-1)]:
            with self.assertRaises(ValueError): access_strategy(pattern,block,window,seed)

    def test_real_cxl_control_path_rotates_and_reuses_scratch_only_after_drain(self):
        """Actual Master/Agent handlers and registered-address interface; fake DMA."""
        import hashlib, os
        import callosum_runtime as runtime
        for pattern in ('overwrite','sequential','uniform_random'):
            with self.subTest(pattern=pattern),tempfile.TemporaryDirectory() as directory:
                root=Path(directory); window=2*1024**2
                nodes=[dict(id=n,pool=p,url='http://'+n,can_send=True,can_receive=True,
                    configured_qps='16',transport_access_patterns=['overwrite','sequential','uniform_random'])
                    for n,p in [('a1','A'),('b1','B')]]
                c=dict(fingerprint='scratch-fixture',mode='rdma',nodes=nodes,master='http://master',
                    pools={p:dict(capacity=4*1024**2) for p in ('A','B')})
                master_log=runtime.Log(root/'master.jsonl','master')
                master=runtime.Master(c,root/'master.sqlite',master_log); master.topology_ok=True
                master.members=lambda:nodes
                agents={}
                for node in nodes:
                    agent=runtime.Agent.__new__(runtime.Agent); agent.c=c; agent.node=node
                    agent.mapping=runtime.Mapping.__new__(runtime.Mapping)
                    agent.mapping.memory=bytearray(4*1024**2); agent.mapping.capacity=len(agent.mapping.memory)
                    agent.mapping.address=ctypes.addressof(ctypes.c_char.from_buffer(agent.mapping.memory))
                    agent.targets={}; agent.jobs={}; agent.lock=threading.RLock(); agent.epoch=node['id']
                    agent.segment='registered-'+node['id']; agent.log=runtime.Log(root/(node['id']+'.jsonl'),node['id'])
                    agents[node['url']]=agent
                sender=agents['http://b1']; receiver=agents['http://a1']
                sender.mapping.memory[4096:4096+window]=(bytes(range(1,252))*(window//251+1))[:window]
                source=dict(pool='B',offset=4096,length=window,key='source',version='v1')
                master.db.execute('INSERT INTO objects VALUES (?,?,?)',('source','B',json.dumps(source))); master.db.commit()
                fail=dict(value=False); posted=[]
                def native(segments,sources,destinations,lengths):
                    if fail['value']: return [lengths[0]-1]
                    for src,dst,n in zip(sources,destinations,lengths):
                        self.assertTrue(sender.mapping.address+4096<=src and src+n<=sender.mapping.address+4096+window)
                        self.assertTrue(receiver.mapping.address+4096<=dst and dst+n<=receiver.mapping.address+4096+window)
                        posted.append((src-sender.mapping.address-4096,dst-receiver.mapping.address-4096))
                        ctypes.memmove(dst,src,n)
                    return lengths
                sender.engine=SimpleNamespace(relay_write_batch_drained=native)
                request=dict(node='b1',receiver='a1',key='source',block_size=1,block_unit='MiB',
                             access_pattern=pattern,window_bytes=window,random_seed=7)
                def rpc(url,method,data,**kwargs):
                    return master.handle(method,data) if url=='http://master' else agents[url].handle(method,data)
                try:
                    with patch.object(runtime,'rpc',side_effect=rpc),patch.object(runtime,'TOTAL_BYTES',5*1024**2+3), \
                         patch.dict(os.environ,MC_NUM_QP_PER_EP='16'):
                        first=master.transport_benchmark(request); second=master.transport_benchmark(request)
                        self.assertEqual(first['bytes'],5*1024**2+3)
                        self.assertEqual(first['window_bytes'],window)
                        self.assertEqual(len(receiver.targets),1)
                        plan_id=next(iter(receiver.targets))
                        receiver.mapping.memory[4096]=99
                        old=receiver.targets[plan_id]
                        receiver.handle('prepare',dict(transfer_id=plan_id,extent=old['extent'],
                            clear_for_benchmark=True,benchmark_trial_id=first['transfer_id']))
                        self.assertEqual(receiver.mapping.memory[4096],99)  # Late replay must not clear data.
                        with self.assertRaisesRegex(RuntimeError,'current master reservation'):
                            receiver.handle('prepare',dict(transfer_id=plan_id,extent=old['extent'],
                                clear_for_benchmark=True,benchmark_trial_id='0'*32))
                        self.assertEqual(len(master.benchmark_windows),1)
                        self.assertTrue(all(a==b for a,b in posted))
                        self.assertEqual(master.db.execute("SELECT next FROM pools WHERE id='A'").fetchone()[0],4096+window)
                        self.assertEqual(master.db.execute("SELECT count(*) FROM objects WHERE pool='A'").fetchone()[0],0)
                        fail['value']=True
                        with self.assertRaisesRegex(RuntimeError,'short'): master.transport_benchmark(request)
                        previous=bytes(receiver.mapping.memory)
                        with self.assertRaisesRegex(RuntimeError,'active/quarantined'): master.transport_benchmark(request)
                        self.assertEqual(bytes(receiver.mapping.memory),previous)
                        self.assertEqual(master.db.execute("SELECT count(*) FROM operations WHERE state='QUARANTINED'").fetchone()[0],1)
                finally:
                    master.db.close(); master_log.file.close()
                    for agent in agents.values(): agent.log.file.close()

    def trial(self):
        with patch('callosum_transport_benchmark.time.monotonic_ns',side_effect=[0,10,25,30,50,60]):
            row=write_loop(lambda s,d,n:n,4096,8192,4,6,2)
        return dict(row,status='PASS',actual_transport='rdma',path_mode='direct_dax_write',
                    block_unit='MiB',transfer_id='a'*32)

    def test_strategy_report_groups_and_csv_keep_offsets_window_and_seed(self):
        rows=[]
        for pattern in ('overwrite','sequential','uniform_random'):
            row=dict(self.trial(),**write_loop(lambda s,d,n:n,4096,8192,4,13,4,
                access_pattern=pattern,window_bytes=16,random_seed=42))
            rows.append(row)
        with tempfile.TemporaryDirectory() as directory:
            out=Path(directory); report=write_report(out,rows)
            with (out/'summary.csv').open() as stream: summaries=list(csv.DictReader(stream))
            self.assertEqual({r['access_pattern'] for r in summaries},set(('overwrite','sequential','uniform_random')))
            with (out/'requests.csv').open() as stream: samples=list(csv.DictReader(stream))
            self.assertTrue(all(r['window_bytes']=='16' and r['random_seed']=='42' for r in samples))
            self.assertEqual([r['source_offset_bytes'] for r in samples if r['access_pattern']=='sequential'],['0','4','8','12'])
            self.assertTrue(all(r['source_offset_bytes']==r['destination_offset_bytes'] for r in samples))
        bad=dict(rows[1]); bad['requests']=[dict(s) for s in bad['requests']]
        bad['requests'][1].update(source_offset_bytes=0,destination_offset_bytes=0)
        with self.assertRaisesRegex(ValueError,'declared strategy'): enrich_trial(bad)

    def test_function_every_logical_request_tail_and_fragment_timed(self):
        row=self.trial()
        self.assertEqual(row['native_calls'],3)
        self.assertEqual([s['native_calls'] for s in row['requests']],[2,1])
        self.assertEqual([s['request_bytes'] for s in row['requests']],[4,2])
        self.assertEqual([s['write_request_e2e_ns'] for s in row['requests']],[15,20])
        self.assertEqual(row['sender_write_loop_ns'],60)
        self.assertEqual(enrich_trial(row)['GBps'],.1)
        rich=enrich_trial(dict(row,requester_rpc_ns=100,sender_start_unix_ns=1000,sender_end_unix_ns=1060))
        self.assertEqual(rich['schema_version'],2)
        self.assertEqual(rich['latency']['write_request_e2e']['p50'],15)
        self.assertEqual(rich['latency']['write_request_e2e']['p99'],20)
        self.assertEqual(rich['bandwidth']['sender_loop']['GB_per_second'],.1)
        self.assertEqual(rich['bandwidth']['trial_protocol']['GB_per_second'],.06)
        self.assertEqual(rich['wall_time']['sender_elapsed_ns'],60)
        self.assertEqual(rich['wall_time']['sender_start_unix_ns'],1000)
        self.assertEqual(rich['request_counts']['observed'],2)
        self.assertAlmostEqual(rich['requests'][0]['write_request_GBps'],4/15)
        self.assertAlmostEqual(rich['requests'][0]['write_request_GiBps'],4/2**30/(15/1e9))
        self.assertEqual(rich['byte_accounting']['completed_payload_bytes'],6)
        self.assertIsNone(rich['byte_accounting']['wire_bytes'])
        self.assertNotIn('write_request_GBps',row['requests'][0])
        self.assertEqual(enrich_trial(rich),rich)

    def test_status_page_modes_are_not_pooled_in_one_aggregate(self):
        with tempfile.TemporaryDirectory() as directory:
            result=write_report(directory,[dict(self.trial(),mr_page_mode=m) for m in ('native','huge2MB')])
            self.assertEqual(len(result['aggregates']),2)

    def test_function_csv_contains_observed_samples_not_loop_average(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            metadata=write_report(root,[self.trial()])
            with (root/'requests.csv').open() as stream:
                rows=list(csv.DictReader(stream))
            self.assertEqual([int(r['write_request_e2e_ns']) for r in rows],[15,20])
            self.assertAlmostEqual(float(rows[0]['write_request_GBps']),4/15)
            self.assertEqual(metadata['request_samples'],2)
            self.assertEqual(metadata['aggregates'][0]['write_p99_us'],.02)
            self.assertIn('WRITE latency excludes',(root/'report.md').read_text())

    def test_failure_missing_or_invalid_samples_cannot_be_passed_as_complete(self):
        row=self.trial()
        for duration in (0,-1,True):
            with self.assertRaises(ValueError):
                enrich_trial(dict(row,requester_rpc_ns=duration))
        for samples in ([],row['requests'][:1],list(reversed(row['requests']))):
            with self.assertRaises(ValueError):
                enrich_trial(dict(row,requests=samples))
        row['requests'][0]['write_request_e2e_ns']=999
        with self.assertRaises(ValueError): enrich_trial(row)

    def test_failure_failed_write_preserves_elapsed_not_completion(self):
        samples=[]
        with patch('callosum_transport_benchmark.time.monotonic_ns',side_effect=[0,10,30]):
            with self.assertRaisesRegex(RuntimeError,'short'):
                write_loop(lambda s,d,n:0,4096,8192,4,4,4,samples)
        self.assertEqual(samples[0]['status'],'FAILED_OR_UNKNOWN')
        self.assertEqual(samples[0]['write_request_e2e_ns'],20)
        from callosum_benchmark_report import write_request_csv
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'failed.csv'
            write_request_csv(path,dict(status='FAIL',requests=samples))
            with path.open() as stream: observation=next(csv.DictReader(stream))
            self.assertEqual(observation['write_request_GBps'],'')
            self.assertEqual(observation['status'],'FAILED_OR_UNKNOWN')

    def test_function_repetitions_report_dispersion_not_mean_rate_as_aggregate(self):
        first=self.trial()
        second=dict(first,sender_write_loop_ns=120,requests=None)
        second.pop('request_timing_version')
        with tempfile.TemporaryDirectory() as directory:
            item=write_report(directory,[first,second])['aggregates'][0]
            self.assertAlmostEqual(item['GBps'],12/180)
            self.assertAlmostEqual(item['trial_GBps_p50'],.075)
            self.assertAlmostEqual(item['trial_GBps_min'],.05)
            self.assertAlmostEqual(item['trial_GBps_max'],.1)
            self.assertIsNotNone(item['trial_GBps_stddev_sample'])
            self.assertFalse(item['latency_available'])

    def test_status_old_results_generate_throughput_without_inventing_cdf(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            old=dict(status='PASS',actual_transport='rdma',path_mode='direct_dax_write',
                     bytes=8589934592,block_bytes=1048576,sender_write_loop_ns=706892428)
            result=write_report(root,[old])
            self.assertAlmostEqual(result['aggregates'][0]['GBps'],12.15168698,places=5)
            self.assertFalse(result['latency_complete'])
            self.assertIn('UNAVAILABLE',(root/'report.md').read_text())
            with (root/'requests.csv').open() as stream:
                self.assertEqual(list(csv.DictReader(stream)),[])

    def test_preflight_offline_cli_does_not_import_native_or_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            source=root/'input'; source.mkdir()
            (source/'block-1.json').write_text(json.dumps(self.trial()))
            entry=Path(__file__).with_name('run_callosum_relay.sh')
            command=['bash',str(entry),'benchmark-report','--input',str(source),'--output',str(root/'report')]
            import os
            env=dict(os.environ,PYTHON_BIN=sys.executable)
            result=subprocess.run(command,env=env,text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            before=(root/'report/requests.csv').read_bytes()
            result=subprocess.run(command,env=env,text=True,capture_output=True)
            self.assertNotEqual(result.returncode,0)
            self.assertEqual(before,(root/'report/requests.csv').read_bytes())

    def test_function_cxl_agent_retains_observations_on_success_and_failure(self):
        """Actual Agent handler and artifact paths; fake native DMA, no RDMA evidence."""
        import os
        import callosum_runtime as runtime
        for fail in (False,True):
            with self.subTest(fail=fail),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                source=ctypes.create_string_buffer(8192)
                destination=ctypes.create_string_buffer(8192)
                ctypes.memset(ctypes.addressof(source),0x5A,8192)
                identity='b'*32
                target=dict(receiver='a1',extent=dict(pool='A',offset=4096,length=4096),
                            remote_base=ctypes.addressof(destination)+4096,address_kind='classic_va',
                            plan_id=identity,segment='fake-test-segment')
                def dma(segments,sources,targets,lengths):
                    if fail: return [0]
                    ctypes.memmove(targets[0],sources[0],lengths[0])
                    return lengths
                agent=runtime.Agent.__new__(runtime.Agent)
                agent.node=dict(id='b1',pool='B',can_send=True)
                agent.c=dict(nodes=[dict(id='a1',pool='A',url='http://test-only')])
                agent.mapping=SimpleNamespace(address=ctypes.addressof(source),
                    bounds=lambda offset,length: self.assertLessEqual(offset+length,8192))
                agent.engine=SimpleNamespace(relay_write_batch_drained=dma)
                agent.lock=threading.Lock(); agent.jobs={}
                agent.log=runtime.Log(root/'agent.jsonl','b1')
                request=dict(transfer_id=identity,source=dict(pool='B',offset=4096,length=4096),
                             target=target,block_bytes=4096)
                try:
                    with patch.dict(os.environ,MC_NUM_QP_PER_EP='16'), \
                         patch.object(runtime,'TOTAL_BYTES',8192),patch.object(runtime,'rpc',return_value=target):
                        if fail:
                            with self.assertRaisesRegex(RuntimeError,'short'):
                                agent.handle('transport_write_loop',request)
                        else:
                            result=agent.handle('transport_write_loop',request)
                            self.assertEqual(len(result['requests']),2)
                    kind='failed' if fail else 'observed'
                    with (root/f'transport-{kind}-{identity}.csv').open() as stream:
                        rows=list(csv.DictReader(stream))
                    self.assertEqual(len(rows),1 if fail else 2)
                    self.assertEqual(rows[-1]['status'],'FAILED_OR_UNKNOWN' if fail else 'PASS')
                    self.assertEqual(agent.jobs[identity],'QUARANTINED' if fail else 'DRAINED')
                    self.assertNotIn('remote_base',(root/f'transport-{kind}-{identity}.csv').read_text())
                    if not fail:
                        event=json.loads((root/'agent.jsonl').read_text())
                        self.assertNotIn('requests',event)
                finally:
                    agent.log.file.close()


if __name__=='__main__':
    unittest.main()
