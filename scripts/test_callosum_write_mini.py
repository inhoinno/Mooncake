"""Mini entry/config/control tests. Fake native transport is not RDMA evidence."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from unittest.mock import mock_open
from types import SimpleNamespace

import callosum_write_mini as mini
from callosum_lab import Runner
from callosum_transport_benchmark import write_loop, TOTAL_BYTES, diagnostic_write_loop, rdma_snapshot, rdma_trial_progress


class MiniTest(unittest.TestCase):
    def test_diagnostic_loops_submit_identical_bytes_and_offsets(self):
        def run(driver):
            posted = []
            def one(segment, src, dst, length):
                posted.append((segment,src,dst,length))
                return (1,1,1)
            def native(segment, sources, destinations, lengths):
                return [(i*8,i*8+5,*one(segment,s,d,n))
                        for i,(s,d,n) in enumerate(zip(sources,destinations,lengths))]
            engine=SimpleNamespace(relay_write_block_profiled=one,relay_write_blocks_diagnostic=native)
            row=diagnostic_write_loop(engine,'target',4096,8192,16,66,16,[],
                access_pattern='sequential',window_bytes=32,random_seed=0,driver=driver)
            self.assertEqual(row['bytes'],66)
            self.assertEqual(row['native_calls'],5)
            self.assertEqual(row['diagnostic_driver'],driver)
            self.assertEqual(sum(row['diagnostic_requests'][0][k] for k in ('check_ns','submit_ns','wait_ns')),3)
            keys=('check_ns','submit_ns','wait_ns','between_calls_ns','in_call_residual_ns')
            self.assertEqual(sum(s[k] for s in row['diagnostic_requests'] for k in keys)+
                             row['diagnostic_loop_tail_ns']+row.get('native_schedule_build_ns',0),
                             row['sender_write_loop_ns'])
            return posted
        expected=[('target',4096+i%2*16,8192+i%2*16,min(16,66-i*16)) for i in range(5)]
        self.assertEqual(run('python'),expected)
        self.assertEqual(run('native'),expected)

    def test_diagnostic_common_interval_does_not_sum_the_shared_port(self):
        def samples(direction, completed):
            return dict(direction=direction,sender_queue_samples=[
                dict(begin_ns=t,end_ns=t+1,contexts=[dict(device='hca0',port=1,tracked=True,
                     completed_write_bytes=b,port_tx_counter_bytes=tx)])
                for t,b,tx in ((15,0,0),(25,0,0),(55,completed,100),(90,999,1000))])
        rows=[samples('a1->b1',10),samples('a2->b2',20)]
        result=mini.sampled_progress(rows,0,100)
        self.assertEqual(result['status'],'BOTH_ADVANCED')
        self.assertEqual((result['begin_ns'],result['end_ns']),(20,80))
        self.assertEqual(result['completed_bytes_lower_bound'],{'a1->b1':10,'a2->b2':20})
        self.assertEqual(result['sender_port_tx_counter_deltas_bytes'],{'hca0/1':100})
        self.assertEqual(result['aggregate_GBps_lower_bound'],0.5)
        self.assertEqual(mini.sampled_progress(rows,0,5)['status'],'INSUFFICIENT_SAMPLES')
        self.assertEqual(mini.sampled_progress(rows[:1],0,100)['status'],'NO_PAIRED_OVERLAP')
        self.assertEqual(mini.sampled_progress([rows[0],samples('a2->b2',0)],0,100)['status'],
                         'NOT_BOTH_OBSERVED_ADVANCING')

    def test_counter_snapshot_preserves_missing_and_converts_word_units(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            counters=root/'class/infiniband/hca0/ports/1/counters'
            counters.mkdir(parents=True)
            (counters/'port_xmit_data').write_text('123\n')
            engine=SimpleNamespace(relay_rdma_snapshot=lambda:[dict(device='hca0',port=i) for i in (1,2)])
            observed=rdma_snapshot(engine,root)
            self.assertLessEqual(observed['begin_ns'],observed['end_ns'])
            self.assertEqual([x['port_tx_counter_bytes'] for x in observed['contexts']],[492,None])

    def test_native_progress_uses_per_trial_qp_deltas_and_rejects_incomplete_counters(self):
        def snapshot(**fields):
            context=dict(device='hca0',port=1,tracked=True,pending_write_wrs=0,
                         qp_counters_truncated=False)
            return dict(contexts=[dict(context,**fields)])
        before=snapshot(qp_completed_bytes={11:128,12:64},completed_write_bytes=192,write_outstanding_ns=8)
        after=snapshot(qp_completed_bytes={11:128,12:128,13:128},completed_write_bytes=384,write_outstanding_ns=16)
        result=rdma_trial_progress(before,after,192)
        self.assertEqual(result['status'],'ACCOUNTED')
        self.assertEqual(result['contexts'][0]['qp_completed_bytes'],{12:64,13:128})
        self.assertEqual(result['contexts'][0]['tracked_outstanding_ns'],8)
        self.assertEqual(rdma_trial_progress(before,after,193)['status'],'INCOMPLETE')
        after['contexts'][0]['pending_write_wrs']=1
        self.assertEqual(rdma_trial_progress(before,after,192)['status'],'OBSERVATION_BOUNDARY_NOT_IDLE')

    def test_rotating_strategy_cli_payload_and_old_master_gate(self):
        for supported in (False,True,'old-agent'):
            with self.subTest(supported=supported),tempfile.TemporaryDirectory() as directory:
                root=Path(directory); path,spec=self.fixture(root)
                spec.update(access_pattern='sequential',access_window_bytes=2*1024**2,random_seed=19)
                path.write_text(json.dumps(spec))
                spec,c,topology,env,out=mini.load_spec(path,2); out.mkdir(parents=True)
                runner=Runner(out); runner.config_sha256='fixture'; calls=[]
                def call(stage,argv,*a,**kw):
                    calls.append((stage,argv))
                    if stage=='effective-config': return json.dumps(dict(config=c))
                    if stage=='status-before': return json.dumps(dict(fingerprint=c['fingerprint'],topology_verified=True,
                        registered_nodes=['a1','b1'],operations=[],benchmark_request_timing_versions={'b1':1},
                        transport_access_patterns=['sequential'] if supported else [],
                        benchmark_access_patterns={n:[] if supported=='old-agent' and n=='b1' else ['sequential'] for n in ('a1','b1')}))
                    if stage.startswith('seed-'):
                        self.assertEqual(argv[argv.index('--bytes')+1],2*1024**2)
                        return '{}'
                    self.assertEqual(argv[argv.index('--access-pattern')+1],'sequential')
                    self.assertEqual(argv[argv.index('--access-window-bytes')+1],str(2*1024**2))
                    self.assertEqual(argv[argv.index('--random-seed')+1],'19')
                    row=write_loop(lambda s,d,n:n,4096,8192,1024**2,3*1024**2,1024**2,
                        access_pattern='sequential',window_bytes=2*1024**2,random_seed=19)
                    return json.dumps(dict(row,status='PASS',actual_transport='rdma',path_mode='direct_dax_write',
                        request_timing_version=1))
                try:
                    with patch.object(Runner,'call',side_effect=call),patch.object(mini,'TOTAL_BYTES',3*1024**2), \
                         contextlib.redirect_stdout(io.StringIO()):
                        if supported is True: mini.execute(spec,c,topology,env,out,runner)
                        else:
                            with self.assertRaisesRegex(RuntimeError,'access-strategy'): mini.execute(spec,c,topology,env,out,runner)
                    if supported is True:
                        result=json.loads((out/'summary.json').read_text())
                        self.assertEqual(result['results'][0]['access_pattern'],'sequential')
                    else: self.assertFalse(any(stage.startswith('seed-') for stage,_ in calls))
                finally: runner.log.close()

    def fixture(self,root,workers=2):
        pairs=[{'source':'b1','receiver':'a1'}]
        if workers==4: pairs.append({'source':'b2','receiver':'a2'})
        nodes=[]
        for i in range(workers):
            pool='A' if i<workers//2 else 'B'
            index=i%(workers//2)+1
            nodes.append(dict(id=pool.lower()+str(index),pool=pool,url=f'http://127.0.0.1:{19101+i}',
                path='/dev/not-opened',te_endpoint=f'127.0.0.1:{19301+i}',rdma_device='test-hca',
                can_send=True,can_receive=True))
        topology=dict(mode='rdma',path_mode='direct_dax_write',abi_version=2,metadata='P2PHANDSHAKE',
            master='http://127.0.0.1:19100',max_inflight_bytes=67108864,nodes=nodes,
            pools={p:dict(capacity=1024**3,map_offset=0,mapping_alignment=2097152,
                          owner=p,physical_pool_id='test-'+p) for p in ('A','B')})
        (root/'topology.json').write_text(json.dumps(topology))
        (root/'control.token').write_text('private-mini-fixture-token-'*3)
        (root/'control.token').chmod(0o600)
        spec=dict(schema_version=1,topology_file='topology.json',token_file='control.token',
                  runtime=dict(python=sys.executable,native_build=str(root),python_paths=[str(root)],library_paths=[]),
                  pairs=pairs,schedule='concurrent_pairs' if workers==4 else 'serial_pairs',
                  sizes=[1],unit='MiB',repetitions=1,output_root='results')
        path=root/'mini.json'; path.write_text(json.dumps(spec))
        return path,spec

    def test_preflight_config_is_relative_json_not_shell_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,spec=self.fixture(root)
            with patch.dict(os.environ,{'PYTHON_BIN':'/wrong','PYTHONPATH':'/wrong','MC_CXL_DEV_PATH':'/dev/forbidden'}):
                actual,c,topology,env,output=mini.load_spec(path,2)
            self.assertEqual(Path(env['PYTHON_BIN']),Path(sys.executable).resolve())
            self.assertEqual(Path(env['PYTHONPATH']),root.resolve())
            self.assertNotIn('MC_CXL_DEV_PATH',env)
            self.assertEqual(topology,root.resolve()/'topology.json')
            self.assertFalse(output.exists())
            _,_,_,_,second=mini.load_spec(path,2)
            self.assertNotEqual(output,second)

    def test_preflight_setup_receipt_does_not_need_env_script_or_campaign_token(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,spec=self.fixture(root)
            receipt=root/'receipt'; receipt.mkdir()
            (receipt/'setup.json').write_text(json.dumps(dict(status='PASS',env=dict(
                PYTHON_BIN=sys.executable,CALLOSUM_NATIVE_BUILD=str(root),PYTHONPATH=str(root),LD_LIBRARY_PATH=''))))
            spec['runtime']={'setup_run':'receipt'}; path.write_text(json.dumps(spec))
            mini.load_spec(path,2)
            self.assertFalse((receipt/'env.sh').exists())

    def test_failure_wrong_topology_overlap_and_unknown_fields_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,spec=self.fixture(root,4)
            with self.assertRaises(RuntimeError): mini.load_spec(path,2)
            for changed in (dict(spec,unexpected=True),dict(spec,pairs=[spec['pairs'][0]]),
                            dict(spec,sizes=[128]),dict(spec,pairs=[spec['pairs'][0]]*2)):
                path.write_text(json.dumps(changed))
                with self.assertRaises(RuntimeError): mini.load_spec(path,4)
            self.assertFalse((root/'results').exists())

    def test_preflight_one_pair_can_use_a_larger_registered_topology(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,spec=self.fixture(root,4)
            spec['pairs']=spec['pairs'][:1]
            spec['schedule']='serial_pairs'
            path.write_text(json.dumps(spec))
            actual,c,_,_,_=mini.load_spec(path,2)
            self.assertEqual(len(c['nodes']),4)
            self.assertEqual(actual['pairs'],[dict(source='b1',receiver='a1')])
            with self.assertRaisesRegex(RuntimeError,'all four workers'):
                mini.load_spec(path,4)

    def test_function_same_implementation_for_two_and_four_workers(self):
        for workers in (2,4):
            with self.subTest(workers=workers),tempfile.TemporaryDirectory() as directory:
                root=Path(directory); path,_=self.fixture(root,workers)
                spec,c,topology,env,output=mini.load_spec(path,workers)
                output.mkdir(parents=True)
                runner=Runner(output); runner.config_sha256='test-source'; runner.remote_mutation_possible=False
                active=0; maximum=0; lock=threading.Lock()
                barrier=threading.Barrier(2) if workers==4 else None
                def call(stage,argv,env,capture=False,log_stdout=True):
                    nonlocal active,maximum
                    if stage=='effective-config': return json.dumps({'config':{'fingerprint':c['fingerprint']}})
                    if stage=='status-before': return json.dumps(dict(fingerprint=c['fingerprint'],topology_verified=True,
                        registered_nodes=[n['id'] for n in c['nodes']],operations=[],
                        benchmark_request_timing_versions={n['id']:1 for n in c['nodes']}))
                    if stage.startswith('seed-'): return '{}'
                    with lock: active+=1; maximum=max(maximum,active)
                    if barrier: barrier.wait(timeout=3)
                    time.sleep(.01)
                    # Two logical requests, each represented by one fake native call.
                    row=write_loop(lambda s,d,n:n,4096,8192,1024**2,2*1024**2,1024**2)
                    row.update(status='PASS',actual_transport='rdma',path_mode='direct_dax_write',
                               block_unit='MiB',transfer_id=stage,requester_rpc_ns=100000000)
                    with lock: active-=1
                    return json.dumps(row)
                try:
                    with patch.object(Runner,'call',side_effect=call),patch.object(mini,'TOTAL_BYTES',2*1024**2), \
                         contextlib.redirect_stdout(io.StringIO()):
                        mini.execute(spec,c,topology,env,output,runner)
                    rows=[json.loads(p.read_text()) for p in output.glob('block*.json')]
                    self.assertEqual(len(rows),workers//2)
                    self.assertTrue(all(r['schema_version']==2 and r['latency']['write_request_e2e']['count']==2 for r in rows))
                    self.assertTrue(all(r['bandwidth']['sender_loop']['GB_per_second']>0 for r in rows))
                    summary=json.loads((output/'summary.json').read_text())
                    self.assertEqual(maximum,workers//2)
                    self.assertFalse(summary['multipath_object_assembly_proven'])
                    group=summary['groups'][0]
                    self.assertAlmostEqual(group['aggregate_protocol_GBps'],group['useful_bytes']/group['controller_elapsed_ns'])
                    self.assertTrue((output/'groups.csv').exists())
                    self.assertEqual(len(list(output.glob('observed-block*.json'))),workers//2)
                finally: runner.log.close()

    def test_failure_rejected_trial_keeps_exact_response_without_success_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,_=self.fixture(root)
            spec,c,topology,env,output=mini.load_spec(path,2)
            output.mkdir(parents=True)
            runner=Runner(output); runner.config_sha256='test-source'
            raw='{"status":"PASS","bytes":1,"request_timing_version":1}\n'
            calls=[]
            def call(stage,*args,**kwargs):
                calls.append(stage)
                if stage=='effective-config': return json.dumps(dict(config=c))
                if stage=='status-before': return json.dumps(dict(fingerprint=c['fingerprint'],topology_verified=True,
                    registered_nodes=['a1','b1'],operations=[],benchmark_request_timing_versions={'b1':1}))
                if stage.startswith('seed-'): return '{}'
                return raw
            try:
                with patch.object(Runner,'call',side_effect=call),contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError,'byte accounting'):
                        mini.execute(spec,c,topology,env,output,runner)
                self.assertEqual(next(output.glob('observed-block*.json')).read_text(),raw)
                self.assertEqual(list(output.glob('block*.json')),[])
                self.assertFalse((output/'summary.json').exists())
                self.assertEqual(len([s for s in calls if s.startswith('block-')]),1)
            finally: runner.log.close()

    def test_status_both_public_wrappers_need_no_runtime_env_for_help(self):
        project=Path(__file__).resolve().parents[2]
        for name in ('cxl-write-mini.sh','cxl-write-4worker-mini.sh'):
            result=subprocess.run(['bash',str(project/'scripts'/name),'--help'],cwd='/tmp',
                env={'PATH':os.environ['PATH']},text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertIn('--config',result.stdout)

    def test_preflight_page_mode_rejected_before_output_or_device_open(self):
        import callosum_runtime as runtime
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,spec=self.fixture(root)
            spec['mr_page_mode']='huge2MB'; path.write_text(json.dumps(spec))
            with self.assertRaisesRegex(RuntimeError,'before agent'):
                mini.load_spec(path,2)
            topology=json.loads((root/'topology.json').read_text())
            topology['mr_page_mode']='huge2MB'
            (root/'topology.json').write_text(json.dumps(topology))
            mini.load_spec(path,2)
            spec.pop('mr_page_mode'); path.write_text(json.dumps(spec))
            self.assertEqual(mini.load_spec(path,2,'huge2MB')[0]['mr_page_mode'],'huge2MB')
            topology['pools']['A']['mapping_alignment']=4096
            (root/'topology.json').write_text(json.dumps(topology))
            with self.assertRaisesRegex(RuntimeError,'2097152'):
                runtime.config(root/'topology.json')
            self.assertFalse((root/'results').exists())

    def test_preflight_public_entry_missing_json_is_actionable_without_native_import(self):
        project=Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            result=subprocess.run(['bash',str(project/'scripts/cxl-write-mini.sh'),
                                   '--config',str(Path(directory)/'missing.json'),'--mode=native'],
                                  cwd=directory,env={'PATH':os.environ['PATH']},text=True,capture_output=True)
            self.assertEqual(result.returncode,1)
            failure=json.loads(result.stderr)
            self.assertEqual(failure['stage'],'configuration-validation')
            self.assertFalse(failure['remote_mutation_possible'])
            self.assertFalse(failure['local_devices_mapped'])
            self.assertEqual(list(Path(directory).iterdir()),[])

    def test_failure_geometry_rejection_closes_unregistered_mapping(self):
        import callosum_runtime as runtime
        import stat
        opened=[]; original_open=os.open
        def tracked_open(*args,**kwargs):
            fd=original_open(*args,**kwargs); opened.append(fd); return fd
        with tempfile.TemporaryDirectory() as directory:
            device=Path(directory)/'model-device'
            with device.open('wb') as stream: stream.truncate(2**21)
            c=dict(mode='rdma',mr_page_mode='huge2MB',pools={'A':dict(capacity=2**21)})
            with patch.dict(os.environ,CALLOSUM_DESTRUCTIVE_ACK='1'), \
                 patch.object(runtime.os,'open',side_effect=tracked_open), \
                 patch.object(runtime.os,'fstat',return_value=SimpleNamespace(st_mode=stat.S_IFCHR)), \
                 patch.object(runtime,'mapping_page_evidence',side_effect=RuntimeError('geometry rejected')):
                with self.assertRaisesRegex(RuntimeError,'geometry rejected'):
                    runtime.Mapping(c,dict(pool='A',path=str(device)))
            with self.assertRaises(OSError): os.fstat(opened[0])

    def test_function_huge_dax_evidence_is_not_hugetlb_or_rnic_proof(self):
        import callosum_runtime as runtime
        import stat
        smaps='00200000-00400000 rw-s 00000000 00:00 1 /dev/dax-test\nKernelPageSize: 2048 kB\nMMUPageSize: 4 kB\n'
        with patch.object(runtime.os,'stat',return_value=SimpleNamespace(st_mode=stat.S_IFCHR,st_rdev=0)), \
             patch.object(Path,'read_text',return_value='2097152'), \
             patch('builtins.open',mock_open(read_data=smaps)):
            evidence=runtime.mapping_page_evidence(2**21,2**21,'/dev/dax-test','huge2MB')
            self.assertFalse(evidence['hugetlb'])
            self.assertEqual(evidence['kernel_page_size_bytes'],2**21)
            self.assertEqual(evidence['mmu_page_size_bytes'],4096)
            self.assertIsNone(evidence['rnic_page_size_bytes'])
            with self.assertRaisesRegex(RuntimeError,'no fallback'):
                runtime.mapping_page_evidence(2**21+4096,2**21,'/dev/dax-test','huge2MB')
        with patch.object(runtime.os,'stat',side_effect=FileNotFoundError('no Linux sysfs')):
            self.assertEqual(runtime.mapping_page_evidence(4096,4096,'/absent','native')['vma_evidence_status'],'UNAVAILABLE')
            with self.assertRaisesRegex(RuntimeError,'no fallback'):
                runtime.mapping_page_evidence(2**21,2**21,'/absent','huge2MB')

    def test_failure_huge_mode_rejects_live_geometry_before_seed(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,spec=self.fixture(root)
            actual,c,topology,env,output=mini.load_spec(path,2)
            actual['mr_page_mode']='huge2MB'; output.mkdir(parents=True)
            runner=Runner(output); stages=[]
            def call(stage,*args,**kwargs):
                stages.append(stage)
                if stage=='effective-config': return json.dumps(dict(config=c))
                return json.dumps(dict(fingerprint=c['fingerprint'],topology_verified=True,
                    registered_nodes=['a1','b1'],operations=[],benchmark_request_timing_versions={'b1':1}))
            try:
                with patch.object(Runner,'call',side_effect=call),contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError,'live MR mode mismatch'):
                        mini.execute(actual,c,topology,env,output,runner)
                self.assertEqual(stages,['effective-config','status-before'])
            finally: runner.log.close()


if __name__=='__main__': unittest.main()
