"""Matrix plumbing/allocation tests. No hardware throughput claims."""
import contextlib
import ctypes
import io
import json
import mmap
import os
import builtins
import runpy
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import callosum_write_matrix as matrix
import callosum_dram_benchmark as dram
from callosum_lab import Runner
from callosum_transport_benchmark import write_loop
import test_callosum_write_mini as mini_tests
import test_callosum_dram_benchmark as dram_tests


class MatrixTest(unittest.TestCase):
    def test_script_drops_sudo_before_any_project_import_or_argparse(self):
        state=dict(uid=0); imports=[]; original=builtins.__import__
        def importer(name,*a,**kw):
            if name.startswith('callosum_'): imports.append((name,state['uid']))
            return original(name,*a,**kw)
        with patch.object(sys,'argv',['matrix','--help']), \
             patch.object(matrix.os,'geteuid',side_effect=lambda:state['uid']), \
             patch.object(matrix.os,'getuid',side_effect=lambda:state['uid']), \
             patch.object(matrix.os,'getresuid',side_effect=lambda:(state['uid'],)*3,create=True), \
             patch.object(matrix.os,'getresgid',return_value=(1000,)*3,create=True), \
             patch.dict(os.environ,SUDO_UID='1000',SUDO_USER='fixture',XDG_CACHE_HOME='/root/cache'), \
             patch.object(matrix.pwd,'getpwnam',return_value=SimpleNamespace(pw_uid=1000,pw_gid=1000,pw_name='fixture')), \
             patch.object(matrix.os,'initgroups'),patch.object(matrix.os,'setgid'), \
             patch.object(matrix.os,'setuid',side_effect=lambda n:state.update(uid=n)), \
             patch.object(builtins,'__import__',side_effect=importer),contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as stopped:
                runpy.run_path(str(Path(matrix.__file__)),run_name='__main__')
            self.assertEqual(stopped.exception.code,0)
            self.assertNotIn('XDG_CACHE_HOME',os.environ)
            self.assertEqual(os.environ['USER'],'fixture')
        self.assertTrue(imports)
        self.assertTrue(all(uid==1000 for _,uid in imports))

    def test_accidental_sudo_drops_all_user_ids_before_creating_evidence(self):
        calls=[]; state=dict(uid=0)
        with patch.object(matrix.os,'geteuid',side_effect=lambda:state['uid']), \
             patch.object(matrix.os,'getuid',side_effect=lambda:state['uid']), \
             patch.object(matrix.os,'getresuid',side_effect=lambda:(state['uid'],)*3,create=True), \
             patch.object(matrix.os,'getresgid',return_value=(1000,)*3,create=True), \
             patch.dict(os.environ,SUDO_UID='1000',SUDO_USER='fixture'), \
             patch.object(matrix.pwd,'getpwnam',return_value=SimpleNamespace(pw_uid=1000,pw_gid=1000,pw_name='fixture')), \
             patch.object(matrix.os,'initgroups',side_effect=lambda *a:calls.append(('groups',a))), \
             patch.object(matrix.os,'setgid',side_effect=lambda n:calls.append(('gid',n))), \
             patch.object(matrix.os,'setuid',side_effect=lambda n:(calls.append(('uid',n)),state.update(uid=n))), \
             patch.object(matrix.os,'chown') as change,tempfile.TemporaryDirectory() as directory:
            matrix.benchmark_account()
            out=matrix.evidence_directory(Path(directory),'matrix-')
            self.assertTrue(out.is_dir())
            self.assertEqual(calls,[('groups',('fixture',1000)),('gid',1000),('uid',1000)])
            change.assert_not_called()

    def test_root_cannot_create_run_directory_or_use_a_forged_invoker(self):
        with patch.object(matrix.os,'geteuid',return_value=0), \
             patch.object(matrix.tempfile,'mkdtemp') as create:
            with self.assertRaisesRegex(RuntimeError,'root-owned benchmark evidence'):
                matrix.evidence_directory(Path('/unused'),'matrix-')
            with patch.dict(os.environ,SUDO_UID='1000',SUDO_USER='fixture'), \
                 patch.object(matrix.pwd,'getpwnam',return_value=SimpleNamespace(pw_uid=2000)), \
                 patch.object(matrix.os,'setuid') as drop:
                with self.assertRaisesRegex(RuntimeError,'invalid sudo invoking account'):
                    matrix.benchmark_account()
                drop.assert_not_called()
            create.assert_not_called()

    def test_cli_rejects_direct_root_before_configuration_or_run_creation(self):
        with patch.object(sys,'argv',['matrix','init','--config','unused.json','--node','a1']), \
             patch.object(matrix.os,'geteuid',return_value=0),patch.dict(os.environ,{},clear=True), \
             patch.object(matrix,'apply_session_profile') as read_config, \
             patch.object(matrix.tempfile,'mkdtemp') as create,contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(matrix.main(),1)
            read_config.assert_not_called(); create.assert_not_called()

    def test_receiver_exit_reports_real_error_and_redacts_control_token(self):
        with tempfile.TemporaryDirectory() as directory:
            log=Path(directory)/'native.log'
            log.write_text('ImportError: missing native dependency\nprivate-token-value\n')
            child=SimpleNamespace(poll=lambda:1)
            with self.assertRaisesRegex(RuntimeError,'ImportError: missing native dependency') as failure:
                matrix.receiver_exit_error(child,log,'native',dict(CALLOSUM_CONTROL_TOKEN='private-token-value'))
            self.assertIn('exit=1',str(failure.exception))
            self.assertNotIn('private-token-value',str(failure.exception))
            self.assertIn('[REDACTED]',str(failure.exception))
            matrix.receiver_exit_error(SimpleNamespace(poll=lambda:None),log,'native',{})

    def test_receive_start_failure_surfaces_child_log_without_restarting_or_killing(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=self.fixture(root)
            args.node='a1'; args.action='receive'; args.page_modes=['native']
            spec,c,topology,receipt,private,env,_=matrix.load(path,args)
            out=root/'receive'; runner=Runner(out)
            child=SimpleNamespace(pid=os.getpid(),poll=lambda:1)
            def start(command,**kw):
                kw['stdout'].write('ImportError: test receiver dependency missing\n')
                kw['stdout'].flush()
                return child
            try:
                with patch.object(matrix,'receiver_health',side_effect=RuntimeError('not running')), \
                     patch.object(matrix.socket,'socket'), \
                     patch.object(matrix.resource,'getrlimit',return_value=(matrix.resource.RLIM_INFINITY,)*2), \
                     patch.object(matrix.subprocess,'Popen',side_effect=start) as starts, \
                     contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError,'test receiver dependency missing'):
                        matrix.receive(spec,c,args,topology,receipt,private,env,out,runner)
                starts.assert_called_once()
                self.assertEqual(runner.stage,'receiver-start-native')
                self.assertTrue((out/'owned-pids.json').is_file())
            finally: runner.log.close()

    def fixture(self,root):
        mini_tests.MiniTest().fixture(root)
        topology=json.loads((root/'topology.json').read_text())
        topology['max_inflight_bytes']=128*1024**2
        topology['pools']['A']['capacity']=2*1024**3
        (root/'topology.json').write_text(json.dumps(topology))
        (root/'setup.json').write_text(json.dumps(dict(status='PASS',build=str(root.resolve()),env=dict(PYTHON_BIN=sys.executable,
            CALLOSUM_NATIVE_BUILD=str(root),PYTHONPATH=str(root),LD_LIBRARY_PATH=''))))
        spec=dict(schema_version=1,topology_file='topology.json',token_file='control.token',output_root='results',
                  runtimes={n:dict(setup_run=str(root)) for n in ('a1','b1')},
                  pair=dict(source='b1',receiver='a1'),numa_nodes={'a1':0,'b1':0},
                  control_ports=dict(native=19201,huge2MB=19202),te_ports=dict(native=19401,huge2MB=19402))
        path=root/'matrix.json'; path.write_text(json.dumps(spec))
        args=SimpleNamespace(action='run',node='b1',sizes=[1,2,4,8,16,32,64,128],unit='MiB',repetitions=3,
                             backings=['cxl','local_numa'],page_modes=['native','huge2MB'])
        return path,args

    def test_preflight_default_sweep_capacity_runtime_and_role(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=self.fixture(root)
            spec,c,topology,receipt,private,env,_=matrix.load(path,args)
            self.assertEqual(topology,root.resolve()/'topology.json')
            self.assertEqual(env['PYTHON_BIN'],sys.executable)
            with patch.dict(os.environ,MC_MTU='4096',MC_GID_INDEX='99',MC_NUM_QP_PER_EP='7'):
                fresh=matrix.load(path,args)[5]
            self.assertEqual(fresh['MC_NUM_QP_PER_EP'],'16')
            self.assertNotIn('MC_MTU',fresh)
            self.assertNotIn('MC_GID_INDEX',fresh)
            command=matrix.dram_command(spec,args,topology,receipt,private,root/'huge',
                                       'huge2MB','sender',env)
            self.assertEqual(command[command.index('--page-mode')+1],'huge2MB')
            self.assertEqual(command[command.index('--control-port')+1],'19202')
            start=command.index('--sizes')+1
            self.assertEqual(command[start:start+8],['1','2','4','8','16','32','64','128'])
            args.node='a1'
            with self.assertRaisesRegex(RuntimeError,'node does not match'): matrix.load(path,args)
            args.node='b1'
            c['pools']['A']['capacity']=1024**3
            (root/'topology.json').write_text(json.dumps({k:v for k,v in c.items() if k!='fingerprint'}))
            with self.assertRaisesRegex(RuntimeError,'arena too small'): matrix.load(path,args)

    def test_function_hugetlb_geometry_flags_and_no_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            node=Path(directory)/'devices/system/node/node0/hugepages/hugepages-2048kB'
            node.mkdir(parents=True); (node/'free_hugepages').write_text('1')
            self.assertEqual(dram.hugepage_capacity(0,1024**2,Path(directory))['required_pages'],1)
            with self.assertRaisesRegex(RuntimeError,'needs 2'):
                dram.hugepage_capacity(0,2**21+1,Path(directory))
        real_mmap=mmap.mmap; flags=[]
        def model_map(fd,length,**kwargs):
            flags.append(kwargs['flags'])
            return real_mmap(-1,length)  # Portable model; not HugeTLB evidence.
        with patch.object(dram,'hugepage_capacity',return_value={}), \
             patch.object(dram.mmap,'mmap',side_effect=model_map), \
             patch.object(dram.ctypes,'addressof',new=lambda _:2**21), \
             patch.object(dram.ctypes,'memset'),patch.object(dram,'bind_memory'), \
             patch.object(dram,'resident_pages',return_value=dict(kernel_page_bytes=2**21)):
            memory=dram.Memory(2**21+1,0,'huge2MB')
            try:
                self.assertEqual(memory.capacity,2*2**21)
                self.assertEqual(flags[0] & (0x3f<<26),21<<26)
                self.assertTrue(flags[0]&0x40000)
                self.assertTrue(memory.page_evidence['hugetlb'])
            finally: memory.memory.close()
        with patch.object(dram,'hugepage_capacity',return_value={}), \
             patch.object(dram.mmap,'mmap',side_effect=OSError('ENOMEM')) as allocate:
            with self.assertRaisesRegex(OSError,'ENOMEM'): dram.Memory(1024**2,0,'huge2MB')
            self.assertEqual(allocate.call_count,1)

    def test_function_four_cases_reuse_children_and_keep_case_reports_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=self.fixture(root)
            spec,c,topology,receipt,private,env,_=matrix.load(path,args)
            args.sizes=[1]; args.repetitions=1
            out=root/'output'; out.mkdir(); runner=Runner(out)
            commands=[]; stages=[]
            def call(stage,command,*a,**kw):
                stages.append(stage)
                if stage=='cxl-status-preflight':
                    return json.dumps(dict(fingerprint=c['fingerprint'],topology_verified=True,
                        mr_pages={n:dict(device_alignment_bytes=2**21,kernel_page_size_bytes=2**21,
                                         virtual_alignment_2MiB=True) for n in ('a1','b1')}))
                if stage.startswith('smoke-'):
                    destination=Path(command[command.index('--output')+1]); destination.mkdir()
                    (destination/'summary.json').write_text(json.dumps(dict(status='PASS')))
                    return
                commands.append(command)
                if stage.startswith('cxl'):
                    mini=json.loads(Path(command[-1]).read_text()); destination=Path(mini['output'])
                    self.assertEqual(mini['require_dax_2m'],stage.endswith('huge2MB'))
                    page_mode='native'; path_mode='direct_dax_write'
                else:
                    destination=Path(command[command.index('--output')+1]); path_mode='dram_rdma_write'
                    page_mode=command[command.index('--page-mode')+1]
                destination.mkdir()
                row=write_loop(lambda s,d,n:n,4096,8192,1024**2,2*1024**2,1024**2)
                row.update(status='PASS',actual_transport='rdma',path_mode=path_mode,mr_page_mode=page_mode,
                           direction='b1->a1',transfer_id=stage)
                (destination/'summary.json').write_text(json.dumps(dict(status='PASS',results=[row])))
            try:
                with patch.object(Runner,'call',side_effect=call), \
                     patch.object(matrix,'receiver_health',return_value={}), \
                     patch.object(matrix,'hugepage_capacity',return_value={}), \
                     patch.object(matrix.resource,'getrlimit',return_value=(matrix.resource.RLIM_INFINITY,)*2), \
                     patch.object(matrix,'TOTAL_BYTES',2*1024**2), \
                     contextlib.redirect_stdout(io.StringIO()):
                    matrix.run(spec,c,args,topology,receipt,private,env,out,runner)
                result=json.loads((out/'summary.json').read_text())
                self.assertEqual(len(commands),4)
                self.assertEqual(stages,['cxl-status-preflight','smoke-local_numa-native',
                    'smoke-local_numa-huge2MB','smoke-cxl','local_numa-native',
                    'local_numa-huge2MB','cxl-native','cxl-huge2MB'])
                self.assertEqual(len(result['report']['aggregates']),4)
                self.assertEqual(result['metric_applicability'],dict(gpu_ready_latency='NOT_APPLICABLE',
                    helper_selection='NOT_APPLICABLE',recomputation='NOT_APPLICABLE'))
                checked=next(r for r in result['results'] if r['experiment_case']=='cxl-huge2MB')
                self.assertEqual(checked['mr_page_mode'],'native')
                self.assertEqual(checked['requested_page_mode'],'huge2MB')
                self.assertTrue((out/'smoke-results.json').exists())
                self.assertTrue((out/'requests.csv').exists())
                self.assertIn('not a forced 4 KiB/2 MiB A/B',(out/'report.md').read_text())
            finally: runner.log.close()

    def test_repeat_receive_reuses_compatible_owners_without_second_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=self.fixture(root); args.action='receive'; args.node='a1'
            spec,c,topology,receipt,private,env,_=matrix.load(path,args)
            out=root/'receive'; out.mkdir(); runner=Runner(out)
            try:
                with patch.object(matrix,'receiver_health',return_value={}), \
                     patch.object(matrix.subprocess,'Popen') as start, \
                     contextlib.redirect_stdout(io.StringIO()):
                    matrix.receive(spec,c,args,topology,receipt,private,env,out,runner)
                start.assert_not_called(); self.assertFalse((out/'owned-pids.json').exists())
            finally: runner.log.close()

    def test_receiver_seal_refuses_active_or_unknown_then_blocks_new_target(self):
        receiver=dram.Receiver(SimpleNamespace(capacity=4096),'model-segment','a1','b1','fp',{},Path('/unused'))
        for state in ('ACTIVE','QUARANTINED'):
            receiver.state=state
            with self.assertRaisesRegex(RuntimeError,'active/quarantined'):
                receiver.handle('seal',{})
            self.assertFalse(receiver.sealed)
        receiver.state='IDLE'
        sealed=receiver.handle('seal',{})
        self.assertTrue(receiver.handle('health',{})['sealed'])
        self.assertEqual(sealed['epoch'],receiver.description['epoch'])
        with self.assertRaisesRegex(RuntimeError,'busy or quarantined'):
            receiver.handle('prepare',{})

    def test_not_ready_receiver_or_failed_smoke_starts_no_performance(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=self.fixture(root)
            spec,c,topology,receipt,private,env,_=matrix.load(path,args)
            args.backings=['local_numa']; args.page_modes=['native']; args.sizes=[1]
            out=root/'test'; out.mkdir(); runner=Runner(out)
            try:
                with patch.object(matrix,'receiver_health',side_effect=RuntimeError('receiver not ready')), \
                     patch.object(Runner,'call') as invoke:
                    with self.assertRaisesRegex(RuntimeError,'receiver not ready'):
                        matrix.run(spec,c,args,topology,receipt,private,env,out,runner)
                    invoke.assert_not_called()
                with patch.object(matrix,'receiver_health',return_value={}), \
                     patch.object(matrix.resource,'getrlimit',return_value=(matrix.resource.RLIM_INFINITY,)*2), \
                     patch.object(Runner,'call',side_effect=RuntimeError('smoke failed')) as invoke:
                    with self.assertRaisesRegex(RuntimeError,'smoke failed'):
                        matrix.run(spec,c,args,topology,receipt,private,env,out,runner)
                    self.assertEqual(invoke.call_count,1)
                    self.assertEqual(invoke.call_args.args[0],'smoke-local_numa-native')
                self.assertFalse((out/'summary.json').exists())
            finally: runner.log.close()

    def test_incomplete_trial_never_produces_matrix_performance_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=self.fixture(root)
            spec,c,topology,receipt,private,env,_=matrix.load(path,args)
            args.backings=['local_numa']; args.page_modes=['native']; args.sizes=[1]; args.repetitions=1
            out=root/'test'; out.mkdir(); runner=Runner(out)
            def call(stage,command,*a,**kw):
                destination=Path(command[command.index('--output')+1]); destination.mkdir()
                result=dict(status='PASS')
                if not stage.startswith('smoke-'):
                    result['results']=[dict(block_bytes=1024**2,repetition=0,bytes=1)]
                (destination/'summary.json').write_text(json.dumps(result))
            try:
                with patch.object(matrix,'receiver_health',return_value={}), \
                     patch.object(matrix.resource,'getrlimit',return_value=(matrix.resource.RLIM_INFINITY,)*2), \
                     patch.object(Runner,'call',side_effect=call):
                    with self.assertRaisesRegex(RuntimeError,'incomplete or smoke-only'):
                        matrix.run(spec,c,args,topology,receipt,private,env,out,runner)
                self.assertFalse((out/'summary.json').exists()); self.assertFalse((out/'report.md').exists())
            finally: runner.log.close()

    def test_status_shell_help_and_missing_config_need_no_native(self):
        entry=Path(__file__).resolve().parents[2]/'scripts/rdma-write-matrix.sh'
        for argv,expected in [(['--help'],0),(['run','--node','b1','--config','/absent/matrix.json'],1)]:
            result=subprocess.run(['bash',str(entry),*argv],cwd='/tmp',env={'PATH':os.environ['PATH']},
                                  capture_output=True,text=True)
            self.assertEqual(result.returncode,expected,result.stderr)
            if expected: self.assertEqual(json.loads(result.stderr)['stage'],'configuration')

    def test_failure_mixed_dram_modes_rejected_before_target_prepare(self):
        args=SimpleNamespace(control_port=19201,page_mode='huge2MB')
        health=dict(node_id='a1',peer_id='b1',fingerprint='fp',memory='dram',configured_qps=16,
                    source_identity=dram.source_identity(),mr_pages=dict(mode='native'))
        safety={}
        with patch.object(dram,'rpc',return_value=health) as control:
            with self.assertRaisesRegex(RuntimeError,'page mode mismatch'):
                dram.sweep(None,None,dict(fingerprint='fp'),dict(id='b1'),
                           dict(id='a1',url='http://127.0.0.1:1'),{},args,Path('.'),safety)
            self.assertEqual(control.call_count,1)
            self.assertFalse(safety.get('exposed',False))

    def test_function_huge_mode_full_sweep_preserves_existing_dma_and_report_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            source=dram_tests.FakeMemory(2**21); destination=dram_tests.FakeMemory(2**21)
            pages=dict(mode='huge2MB',hugetlb=True,kernel_page_bytes=2**21)
            source.page_evidence=destination.page_evidence=pages
            receiver=dram.Receiver(destination,'model-segment','a1','b1','fixture',{},root)
            args=SimpleNamespace(control_port=19201,page_mode='huge2MB',sizes=[1],unit='MiB',
                                 repetitions=1,warmup_blocks=0)
            def control(url,method,data=None,**kwargs): return receiver.handle(method,data or {})
            def dma(segments,sources,targets,lengths):
                ctypes.memmove(targets[0],sources[0],lengths[0]); return lengths
            try:
                with patch.object(dram,'rpc',side_effect=control),patch.object(dram,'snapshot',return_value={}), \
                     patch.object(dram,'resident_pages',return_value=dict(kernel_page_bytes=2**21)), \
                     patch.object(dram,'TOTAL_BYTES',2**21),contextlib.redirect_stdout(io.StringIO()):
                    rows=dram.sweep(SimpleNamespace(relay_write_batch_drained=dma),source,
                        dict(fingerprint='fixture'),dict(id='b1'),dict(id='a1',url='http://model:1'),
                        {},args,root,{})
                self.assertEqual(rows[0]['mr_page_mode'],'huge2MB')
                self.assertEqual(rows[0]['native_calls'],2)
                self.assertEqual(len(rows[0]['requests']),2)
                self.assertEqual(receiver.state,'IDLE')
                self.assertEqual(json.loads((root/'block-1MiB-rep-0.json').read_text())['status'],'PASS')
            finally:
                source.memory.close(); destination.memory.close()


if __name__=='__main__': unittest.main()
