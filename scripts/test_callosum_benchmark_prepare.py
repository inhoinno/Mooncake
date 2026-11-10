"""First-run and recovery fixtures. No DAX, HugeTLB or RDMA hardware acceptance."""
import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import callosum_benchmark_prepare as prep
import callosum_write_matrix as matrix
import test_callosum_write_matrix as matrix_tests


class PreparationTest(unittest.TestCase):
    def test_launcher_refresh_requires_matching_current_sources_and_stable_runtime_native(self):
        old=dict(controller='a1',profile=dict(sizes=[1]),software={
            'callosum_dram_benchmark.py':'old','callosum_runtime.py':'runtime',
            'mooncake-integration/pool_relay/relay_batch.h':'native'})
        actual=dict(old['software'],**{'callosum_dram_benchmark.py':'fixed'})
        self.assertEqual(matrix.peer_session_differences(old,old,actual,actual),{})
        mismatch=dict(actual,**{'callosum_dram_benchmark.py':'other-fix'})
        self.assertIn('peer_checkout:callosum_dram_benchmark.py',
            matrix.peer_session_differences(old,old,actual,mismatch))
        for field in ('callosum_runtime.py','mooncake-integration/pool_relay/relay_batch.h'):
            changed=dict(actual); changed[field]='incompatible'
            self.assertIn('software:'+field,matrix.peer_session_differences(old,old,changed,changed))
        # Without actual peer-source evidence, the old strict check remains.
        self.assertIn('software:callosum_dram_benchmark.py',matrix.peer_session_differences(old,old,actual))

    def test_prepare_allocation_executes_real_probe_program_before_any_rdma(self):
        import callosum_lab as lab
        import callosum_dram_benchmark as dram
        from unittest.mock import MagicMock
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=matrix_tests.MatrixTest().fixture(root)
            args.node='b1'; args.page_modes=['native','huge2MB']; args.unit='MB'
            spec,c,_,_,_,env,_=matrix.load(path,args)
            runner=lab.Runner(root/'work'); memories=[]
            def memory(capacity,node,mode):
                self.assertEqual((capacity,node),(128000000,0))
                value=SimpleNamespace(placement=dict(local=True),page_evidence=dict(mode=mode),memory=MagicMock())
                memories.append(value); return value
            def run(stage,argv,**kw):
                self.assertEqual(stage,'prepare-dram-allocation')
                self.assertEqual(argv[0],env['PYTHON_BIN'])
                with patch.object(sys,'argv',['-c',argv[-1]]),contextlib.redirect_stdout(io.StringIO()) as output:
                    exec(argv[2],{})
                return output.getvalue()
            try:
                with patch.object(dram,'nic_placement',return_value=dict(memory_numa_node=0)), \
                     patch.object(dram,'Memory',side_effect=memory),patch.object(runner,'call',side_effect=run):
                    matrix.check_dram_allocation(spec,c,args,env,runner,root/'work')
                self.assertEqual(len(memories),2)
                for value in memories: value.memory.close.assert_called_once()
                proof=json.loads((root/'work/dram-allocation.json').read_text())
                self.assertFalse(proof['rdma_registered'])
                self.assertFalse(runner.children)
            finally: runner.log.close()

    def test_prepare_allocation_failure_stops_before_ssh_or_service_start(self):
        import callosum_lab as lab
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=matrix_tests.MatrixTest().fixture(root)
            args.action='prepare'; runner=lab.Runner(root/'work')
            try:
                cold=dict(status='INPUTS_CHECKED_NOT_STARTED',memlock=dict(required=0))
                with patch.object(prep,'inspect_resources',return_value=cold), \
                     patch.object(matrix,'check_dram_allocation',side_effect=RuntimeError('mbind EINVAL')), \
                     patch.object(matrix,'bind_peer') as handoff,patch.object(matrix,'launch_fresh') as start, \
                     patch.object(matrix.resource,'getrlimit',return_value=(prep.resource.RLIM_INFINITY,)*2):
                    with self.assertRaisesRegex(RuntimeError,'mbind EINVAL'):
                        matrix.prepare_session(path,args,root/'work',runner)
                handoff.assert_not_called(); start.assert_not_called()
                self.assertFalse(runner.services_may_be_live)
            finally: runner.log.close()

    def test_session_freshness_uses_origin_clock_for_initial_and_bound_peer(self):
        session=dict(machine_id='controller',created_unix_ns=1000)
        peer=dict(machine_id='controller',host_time_unix_ns=2000)
        # The requester's local clock can be ahead or behind by days.
        for other_clock in (0,10**18):
            with patch.object(matrix.time,'time_ns',return_value=other_clock):
                self.assertEqual(matrix.validate_session_age(session,peer,'requester'),1000)
        # A bound peer carries the controller session; use the controller's own clock.
        with patch.object(matrix.time,'time_ns',return_value=3000):
            self.assertEqual(matrix.validate_session_age(session,
                dict(machine_id='requester',host_time_unix_ns=10**18),'controller'),2000)
        for observed in (999,1000+24*3600*10**9):
            with self.assertRaisesRegex(RuntimeError,'origin host'):
                matrix.validate_session_age(session,dict(peer,host_time_unix_ns=observed),'requester')
        with self.assertRaisesRegex(RuntimeError,'origin is not either verified host'):
            matrix.validate_session_age(dict(session,machine_id='unknown'),peer,'requester')

    def test_full_resource_preparation_configures_hugetlb_access_and_reuses_it(self):
        from callosum_lab import Runner
        for initial in (0,2000):
            with self.subTest(initial=initial),tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
                root=Path(directory); path,args=self.inspect_fixture(root,stack)
                group=dict(value=initial); original=Path.read_text
                def read(p,*a,**kw):
                    if str(p)=='/proc/sys/vm/hugetlb_shm_group': return str(group['value'])
                    return original(p,*a,**kw)
                runner=Runner(root/'work'); commands=[]
                def command(stage,argv,**kw):
                    commands.append(argv)
                    if 'tee' in argv:
                        self.assertEqual(argv,['sudo','-n','tee','--','/proc/sys/vm/hugetlb_shm_group'])
                        group['value']=int(kw['input_text'])
                try:
                    with patch.object(Path,'read_text',new=read), \
                         patch.object(prep.os,'geteuid',return_value=1000), \
                         patch.object(prep.os,'getegid',return_value=1000), \
                         patch.object(prep.os,'getgroups',return_value=[1000]), \
                         patch.object(runner,'call',side_effect=command),contextlib.redirect_stdout(io.StringIO()):
                        result=prep.inspect_resources(path,args,root/'work',matrix.load,runner,live=False)
                        if initial==0:
                            self.assertEqual(result['status'],'INPUTS_CHECKED_NOT_STARTED',result['issues'])
                            self.assertEqual(group['value'],1000)
                            count=len(commands)
                            again=prep.inspect_resources(path,args,root/'work',matrix.load,runner,live=False)
                            self.assertEqual(len(commands),count)
                            self.assertEqual(again['status'],'INPUTS_CHECKED_NOT_STARTED')
                        else:
                            self.assertEqual(result['status'],'BLOCKED')
                            self.assertEqual(group['value'],2000); self.assertEqual(commands,[])
                    record=json.loads((root/'work/hugetlb-permission.json').read_text())
                    self.assertEqual(record['change_possible'],initial==0)
                    self.assertFalse(record['automatic_restore'])
                finally: runner.log.close()

    def test_memlock_sudo_targets_only_current_process_and_is_rechecked(self):
        from callosum_lab import Runner
        for denied in (False,True):
            with self.subTest(denied=denied),tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
                root=Path(directory); path,args=self.inspect_fixture(root,stack)
                limits=dict(value=(8388608,8388608)); runner=Runner(root/'work')
                def command(stage,argv,**kw):
                    if denied: raise RuntimeError('sudo denied fixture')
                    if 'prlimit' in argv:
                        self.assertEqual(argv[:5],['sudo','-n','prlimit','--pid',str(os.getpid())])
                        limits['value']=(2**31,2**31)
                        self.assertEqual(argv[-1],'--memlock=2147483648:2147483648')
                try:
                    with patch.object(prep.resource,'getrlimit',side_effect=lambda _:limits['value']), \
                         patch.object(runner,'call',side_effect=command),contextlib.redirect_stdout(io.StringIO()):
                        initial=prep.inspect(path,args,root/'work',matrix.load,live=False)
                        result=prep.grant_process_memlock(initial,path,args,root/'work',matrix.load,runner,live=False)
                    self.assertEqual(result['status'],'BLOCKED' if denied else 'INPUTS_CHECKED_NOT_STARTED')
                    record=json.loads((root/'work/memlock-permission.json').read_text())
                    self.assertEqual(record['pid'],os.getpid()); self.assertTrue(record['session_only'])
                    self.assertFalse(runner.children)
                finally: runner.log.close()

    def test_hugetlb_sudo_denial_and_partial_change_preserve_evidence_without_fallback(self):
        from callosum_lab import Runner
        for partial in (False,True):
            with self.subTest(partial=partial),tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
                root=Path(directory); path,args=self.inspect_fixture(root,stack)
                group=dict(value=0); original=Path.read_text; runner=Runner(root/'work')
                def read(p,*a,**kw):
                    if str(p)=='/proc/sys/vm/hugetlb_shm_group': return str(group['value'])
                    return original(p,*a,**kw)
                def command(stage,argv,**kw):
                    if not partial: raise RuntimeError('sudo denied')
                    if 'tee' in argv:
                        group['value']=1000
                        raise RuntimeError('write returned failure after possible change')
                try:
                    with patch.object(Path,'read_text',new=read), \
                         patch.object(prep.os,'geteuid',return_value=1000), \
                         patch.object(prep.os,'getegid',return_value=1000), \
                         patch.object(prep.os,'getgroups',return_value=[1000]), \
                         patch.object(runner,'call',side_effect=command),contextlib.redirect_stdout(io.StringIO()):
                        result=prep.inspect_resources(path,args,root/'work',matrix.load,runner,live=False)
                    self.assertEqual(result['status'],'BLOCKED')
                    record=json.loads((root/'work/hugetlb-permission.json').read_text())
                    self.assertEqual(record['change_possible'],partial)
                    self.assertEqual(group['value'],1000 if partial else 0)
                    self.assertFalse(runner.children)
                finally: runner.log.close()

    def test_peer_export_resolves_private_inputs_relative_to_config_not_ssh_cwd(self):
        import shlex
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); inputs=root/'inputs'; inputs.mkdir()
            private=inputs/'token'; private.write_text('private-controller-token-'*3); private.chmod(0o600)
            (inputs/'topology.json').write_text('{"fixture":true}')
            target=root/'matrix.json'
            target.write_text(json.dumps(dict(workflow=dict(node='a1'),
                fresh_session=dict(controller='a1'),token_file='inputs/token',topology_file='inputs/topology.json')))
            args=SimpleNamespace(peer='operator@host')
            def ssh(argv,**kwargs):
                command=shlex.split(argv[-1])
                command[2]=command[2].replace("Path('/etc/machine-id').read_text().strip()",repr('remote-machine'))
                return subprocess.run([sys.executable,*command[1:]],cwd=inputs,check=True,
                    capture_output=True,text=True).stdout
            with patch.object(matrix.subprocess,'check_output',side_effect=ssh):
                value=matrix.peer_inputs(args,target,('-p','2022'))
            self.assertEqual(value['topology'],dict(fixture=True))
            self.assertEqual(value['token'],private.read_text().strip())

    def test_peer_session_reports_init_checkout_and_profile_differences_without_secrets(self):
        local=dict(controller='a1',profile=dict(sizes=[1,2]),software={'launcher.py':'old'})
        peer=dict(controller='a1',profile=dict(sizes=[1,4]),software={'launcher.py':'new'})
        result=matrix.peer_session_differences(local,peer,{'launcher.py':'new'})
        self.assertEqual(set(result),{'profile','software:launcher.py'})
        self.assertEqual(result['software:launcher.py'],
            dict(local_init='old',peer_init='new',local_checkout='new'))
        self.assertEqual(matrix.peer_session_differences(peer,peer,peer['software']),{})
        peer['controller']='other'
        self.assertIn('controller',matrix.peer_session_differences(local,peer,peer['software']))

    def test_skip_build_init_reuses_exact_local_receipt_and_creates_new_sessions(self):
        import callosum_lab as lab
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); topology=self.fresh_fixture(root)
            target=root/'benchmark.local.json'
            target.write_text(json.dumps(dict(runtimes={'a1':dict(setup_run=str(root))})))
            receipt=json.loads((root/'setup.json').read_text())
            source=Path(prep.__file__).resolve().parents[1]
            receipt['source_hashes']={f:prep.digest(source/f) for f in prep.SOURCE_FILES}
            (root/'setup.json').write_text(json.dumps(receipt))
            argv=['matrix','init','--config',str(target),'--node','a1',
                  '--topology',str(topology),'--skip-build']
            campaigns=[]; tokens=[]
            read=Path.read_text
            def machine(path,*a,**kw):
                return 'machine-a1' if str(path)=='/etc/machine-id' else read(path,*a,**kw)
            with patch.object(sys,'argv',argv),patch.object(prep.sys,'platform','linux'), \
                 patch.object(Path,'read_text',new=machine), \
                 patch.object(lab,'prepare') as compile_build, \
                 patch.object(prep.subprocess,'check_output',return_value='{"engine.so":"hash"}'), \
                 patch.object(prep,'inspect_resources',return_value=dict(status='INPUTS_CHECKED_NOT_STARTED')) as inspect, \
                 contextlib.redirect_stdout(io.StringIO()):
                for attempt in range(3):
                    # Explicit selection also enters fresh initialization, not legacy init.
                    status='BLOCKED' if attempt==2 else 'INPUTS_CHECKED_NOT_STARTED'
                    inspect.return_value=dict(status=status,issues=[dict(check='fixture-blocker')] if attempt==2 else [],
                        software=dict(detail='full-software-evidence'),execution_account=dict(uid=1000))
                    output=io.StringIO()
                    with patch.object(sys,'argv',argv+(['--setup-run',str(root)] if attempt==0 else [])+
                                      (['--verbose'] if attempt==1 else [])),contextlib.redirect_stdout(output):
                        self.assertEqual(matrix.main(),1 if attempt==2 else 0)
                    text=output.getvalue()
                    final=json.loads(text[text.rfind('\n{')+1:])
                    self.assertEqual(final['status'],'BUILD_REUSED_NOT_REBUILT')
                    self.assertEqual(final['preparation_status'],status)
                    self.assertEqual('full-software-evidence' in text,attempt==1)
                    if attempt==2: self.assertIn('fixture-blocker',text)
                    spec=json.loads(target.read_text()); run=Path(spec['runtimes']['a1']['setup_run'])
                    adopted=json.loads((run/'setup.json').read_text())
                    self.assertIn('full-software-evidence',(run/'commands.log').read_text())
                    self.assertEqual(adopted['build'],receipt['build'])
                    self.assertEqual(adopted['adoption_scope'],'VERIFIED_EXISTING_ABI2_NOT_REBUILT')
                    self.assertEqual(json.loads((run/'fresh-state.json').read_text())['status'],'REUSED_BUILD_NOT_STARTED')
                    campaigns.append(run); tokens.append(Path(spec['token_file']).read_text())
            compile_build.assert_not_called(); self.assertEqual(inspect.call_count,3)
            self.assertNotEqual(campaigns[0],campaigns[1]); self.assertNotEqual(tokens[0],tokens[1])

    def test_skip_build_first_run_needs_receipt_and_never_falls_back_to_compile(self):
        import callosum_lab as lab
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); topology=self.fresh_fixture(root)
            target=root/'benchmark.local.json'
            base=['matrix','init','--config',str(target),'--node','a1',
                  '--topology',str(topology),'--skip-build']
            with patch.object(prep.sys,'platform','linux'),patch.object(lab,'prepare') as compile_build, \
                 patch.object(prep,'inspect_resources') as inspect,contextlib.redirect_stdout(io.StringIO()):
                with patch.object(sys,'argv',base): self.assertEqual(matrix.main(),1)
                with patch.object(sys,'argv',base+['--setup-run',str(root/'missing')]):
                    self.assertEqual(matrix.main(),1)
            compile_build.assert_not_called(); inspect.assert_not_called(); self.assertFalse(target.exists())

    def fresh_fixture(self,root):
        _,args=matrix_tests.MatrixTest().fixture(root)
        raw=json.loads((root/'topology.json').read_text())
        raw.update(evidence_class='TRANSFER_ONLY',lab_run=dict(controller='a1',source='b1',
            requester='a1',profile='two_worker_smoke',wait_seconds=1800))
        (root/'approved.json').write_text(json.dumps(raw))
        return root/'approved.json'

    def test_fresh_cli_recreates_inputs_despite_existing_campaign_and_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); topology=self.fresh_fixture(root)
            old=root/'old-campaign'; old.mkdir()
            (old/'topology.json').write_text('old topology evidence')
            (old/'control.token').write_text('old private token evidence')
            target=root/'benchmark.local.json'
            previous=json.dumps(dict(runtimes={'a1':dict(setup_run=str(old))},token_file=str(old/'control.token')))
            target.write_text(previous)
            command=['bash',str(Path(__file__).resolve().parents[2]/'scripts/rdma-write-matrix.sh'),
                'init','--config',str(target),'--node','a1','--topology',str(topology),'--generate-only']
            sessions=[]
            for _ in range(2):
                result=subprocess.run(command,cwd=root,text=True,capture_output=True)
                self.assertEqual(result.returncode,0,result.stderr)
                fresh=json.loads(target.read_text()); campaign=Path(fresh['runtimes']['a1']['setup_run'])
                self.assertNotEqual(campaign,old)
                self.assertTrue(campaign.is_dir())
                self.assertFalse((campaign/'setup.json').exists())
                self.assertFalse((campaign/'topology.json').exists())
                self.assertEqual(json.loads((campaign/'fresh-state.json').read_text())['status'],'GENERATED_NOT_BUILT')
                secret=Path(fresh['token_file']).read_text()
                self.assertNotIn(secret.strip(),result.stdout+result.stderr)
                self.assertEqual(stat.S_IMODE(Path(fresh['token_file']).stat().st_mode),0o600)
                self.assertTrue(Path(fresh['topology_file']).is_file())
                sessions.append((fresh['fresh_session']['id'],campaign,secret))
            self.assertNotEqual(sessions[0],sessions[1])
            self.assertNotEqual(sessions[0][2],sessions[1][2])
            self.assertIn(previous,[p.read_text() for p in root.glob('benchmark.local.json.*.previous')])
            self.assertEqual((old/'control.token').read_text(),'old private token evidence')
            self.assertEqual((old/'topology.json').read_text(),'old topology evidence')

    def test_fresh_build_then_canonical_launch_uses_only_new_artifacts(self):
        import callosum_lab as lab
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); topology=self.fresh_fixture(root)
            receipt=json.loads((root/'setup.json').read_text())
            args=SimpleNamespace(node='a1',topology=str(topology),generate_only=False,jobs=4,
                skip_apt=True,sizes=[1,2],unit='MB',repetitions=3,
                page_modes=['native'],backings=['local_numa','cxl'],action='fresh')
            campaign=root/'fresh-run'; runner=lab.Runner(campaign)
            read=Path.read_text
            def machine(path,*a,**kw):
                return 'machine-a1' if str(path)=='/etc/machine-id' else read(path,*a,**kw)
            def build(source,run,options,log):
                self.assertFalse((run/'setup.json').exists())
                (run/'setup.json').write_text(json.dumps(receipt))
            try:
                with patch.object(prep.sys,'platform','linux'),patch.object(Path,'read_text',new=machine), \
                     patch.object(lab,'prepare',side_effect=build) as producer, \
                     contextlib.redirect_stdout(io.StringIO()):
                    spec=prep.fresh_configuration(root/'benchmark.local.json',args,runner)
                producer.assert_called_once()
                with patch.dict(os.environ,{'CALLOSUM_DESTRUCTIVE_ACK':'1'}), \
                     patch.object(lab,'main',return_value=0) as start,contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(matrix.launch_fresh(root/'benchmark.local.json',args,runner),0)
                argv=start.call_args.args[0]
                self.assertEqual(argv[argv.index('--run-dir')+1],str(campaign))
                self.assertEqual(argv[argv.index('--config')+1],spec['topology_file'])
                self.assertEqual(argv[argv.index('--token-file')+1],spec['token_file'])
                # A partial startup is not erased or restarted into the same arena.
                (campaign/'topology.json').write_text('retained partial startup')
                with self.assertRaisesRegex(RuntimeError,'already attempted startup'):
                    matrix.launch_fresh(root/'benchmark.local.json',args,runner)
            finally: runner.log.close()

    def test_init_is_local_and_prepare_explicitly_binds_controller_without_overwriting_probe(self):
        import callosum_lab as lab
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); topology=self.fresh_fixture(root)
            args=SimpleNamespace(node='a1',topology=str(topology),generate_only=True,jobs=4,
                skip_apt=True,sizes=[1,2],unit='MB',repetitions=3,peer=None,
                remote_root=None,peer_config=None,page_modes=['native'],backings=['cxl'],action='prepare')
            leader=lab.Runner(root/'leader'); peer=lab.Runner(root/'peer')
            original_read=Path.read_text
            def machine(path,*a,**kw):
                return 'machine-'+args.node if str(path)=='/etc/machine-id' else original_read(path,*a,**kw)
            try:
                with patch.object(prep.sys,'platform','linux'),patch.object(Path,'read_text',new=machine), \
                     contextlib.redirect_stdout(io.StringIO()):
                    first=prep.fresh_configuration(root/'benchmark.local.json',args,leader)
                    args.node='b1'
                    with patch.object(prep.subprocess,'check_output',side_effect=AssertionError('init must not use SSH')):
                        provisional=prep.fresh_configuration(root/'peer-matrix.json',args,peer)
                    # A fixture for the Linux build producer; not build evidence.
                    (peer.run_dir/'setup.json').write_bytes((root/'setup.json').read_bytes())
                    probe=Path(provisional['token_file']).read_bytes()
                    self.assertNotEqual(first['fresh_session']['id'],provisional['fresh_session']['id'])
                    self.assertFalse(provisional['workflow']['bound'])
                    # Exercise the real export program over an isolated fixture,
                    # not SSH or a Linux host. Substitute only the machine probe.
                    import shlex
                    def private_handoff(argv,**kwargs):
                        command=shlex.split(argv[-1]); self.assertEqual(command[:2],['python3','-c'])
                        command[2]=command[2].replace("Path('/etc/machine-id').read_text().strip()",repr('machine-a1'))
                        return subprocess.run([sys.executable,*command[1:]],check=True,text=True,
                            capture_output=True).stdout
                    args.peer='authorized-controller'; args.peer_config=str(root/'benchmark.local.json')
                    with patch.object(prep.subprocess,'check_output',side_effect=private_handoff):
                        second=matrix.bind_peer(root/'peer-matrix.json',args,())[0]
                    # Rebinding must not repair a wrong host, topology or token
                    # by replacing a retained credential.
                    bundle=dict(session=first['fresh_session'],workflow=first['workflow'],
                        host_time_unix_ns=first['fresh_session']['created_unix_ns']+1,
                        topology=json.loads(Path(first['topology_file']).read_text()),
                        machine_id='machine-a1',token=Path(first['token_file']).read_text().strip())
                    active=(root/'peer-matrix.json').read_bytes()
                    token=Path(second['token_file']).read_bytes()
                    (peer.run_dir/'control.token').write_bytes(token)
                    for mutation in ('same_host','wrong_node','topology','profile','software','session','token'):
                        bad=json.loads(json.dumps(bundle))
                        if mutation=='same_host': bad['machine_id']='machine-b1'
                        elif mutation=='wrong_node': bad['workflow']['node']='b1'
                        elif mutation=='topology': bad['topology']['pools']['A']['owner']='someone-else'
                        elif mutation=='profile': bad['session']['profile']['sizes']=[999]
                        elif mutation=='software': bad['session']['software']['callosum_write_matrix.py']='different-checkout'
                        elif mutation=='session': bad['session']['id']='different-session'
                        elif mutation=='token': bad['token']='wrong-token-'*5
                        with self.subTest(mutation=mutation),patch.object(matrix,'peer_inputs',return_value=bad):
                            with self.assertRaises(RuntimeError) as failure:
                                matrix.bind_peer(root/'peer-matrix.json',args,())
                            if mutation=='software':
                                self.assertIn('software:callosum_write_matrix.py',str(failure.exception))
                                self.assertIn('local_init',str(failure.exception))
                                self.assertNotIn(token.decode().strip(),str(failure.exception))
                        self.assertEqual((root/'peer-matrix.json').read_bytes(),active)
                        self.assertEqual(Path(second['token_file']).read_bytes(),token)
                self.assertEqual(first['fresh_session'],second['fresh_session'])
                self.assertEqual(Path(first['token_file']).read_bytes(),Path(second['token_file']).read_bytes())
                self.assertEqual(Path(provisional['token_file']).read_bytes(),probe)
                self.assertTrue(second['workflow']['bound'])
                self.assertEqual(set(second['runtimes']),{'b1'})
                self.assertEqual(second['runtimes']['b1']['setup_run'],str(root/'peer'))
            finally: leader.log.close(); peer.log.close()

    def test_saved_profile_ignores_inherited_defaults_but_rejects_explicit_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/'matrix.json'
            profile=dict(sizes=[1,2],unit='MB',repetitions=1,
                         backings=['local_numa','cxl'],page_modes=['native'])
            target.write_text(json.dumps(dict(fresh_session=dict(profile=profile))))
            for action in ('prepare','run'):
                for sizes,unit in [('1 2 4 8 16 32 64 128','MiB'),('not a size','invalid-unit')]:
                    with self.subTest(action=action,sizes=sizes),patch.dict(os.environ,IO_SIZES=sizes,IO_UNIT=unit):
                        args=SimpleNamespace(action=action,config=str(target),**{key:None for key in profile})
                        matrix.apply_session_profile(args)
                        self.assertEqual({key:getattr(args,key) for key in profile},profile)
                        for field,value in [('sizes',[4]),('unit','MiB')]:
                            changed=SimpleNamespace(action=action,config=str(target),**{key:None for key in profile})
                            setattr(changed,field,value)
                            with self.assertRaisesRegex(RuntimeError,field+' differs from init'):
                                matrix.apply_session_profile(changed)

    def test_init_cli_environment_and_legacy_default_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            target=Path(directory)/'matrix.json'
            profile=dict(sizes=[1,2],unit='MB',repetitions=1,
                         backings=['local_numa','cxl'],page_modes=['native'])
            # Even an old config must not supply defaults to a new init.
            target.write_text(json.dumps(dict(fresh_session=dict(profile=profile))))
            with patch.dict(os.environ,IO_SIZES='4 8',IO_UNIT='MiB'):
                args=SimpleNamespace(action='init',config=str(target),**{key:None for key in profile})
                matrix.apply_session_profile(args)
                self.assertEqual((args.sizes,args.unit),([4,8],'MiB'))
                cli=SimpleNamespace(action='init',config=str(target),**dict(profile,sizes=[16],unit='MB'))
                matrix.apply_session_profile(cli)
                self.assertEqual((cli.sizes,cli.unit),([16],'MB'))
                target.write_text(json.dumps(dict(schema_version=1)))
                legacy=SimpleNamespace(action='run',config=str(target),**{key:None for key in profile})
                matrix.apply_session_profile(legacy)
                self.assertEqual((legacy.sizes,legacy.unit),([4,8],'MiB'))

    def test_three_manual_stages_with_real_file_handoff_and_node_selected_run(self):
        """T0 workflow: real files/SSH export programs; mocked builds/RPC/engines.

        This proves orchestration, not Linux builds, NICs or DMA semantics.
        """
        import callosum_lab as lab
        import shlex
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); context=threading.local(); hosts={}; errors=[]
            observed=[]; real_read=Path.read_text
            def read(path,*a,**kw):
                if str(path)=='/etc/machine-id': return 'machine-'+context.node
                return real_read(path,*a,**kw)
            def build(source,run,args,runner):
                (run/'setup.json').write_bytes((root/context.node/'setup.json').read_bytes())
            def inspect(path,args,work,loader,live=True):
                spec,c,_,_,_,env,_=loader(path,args)
                report=dict(status='PREPARED_NOT_TESTED' if live else 'INPUTS_CHECKED_NOT_STARTED',
                    node=args.node,issues=[],plan=prep.plan(c,spec,args),software=prep.software(),
                    machine=dict(machine_id='machine-'+args.node),created_unix_ns=prep.time.time_ns(),
                    memlock=dict(required=0))
                return prep.save_readiness(work,report,env['CALLOSUM_CONTROL_TOKEN'])
            def ssh(argv,**kwargs):
                self.assertEqual(argv[0],'ssh')
                target='a1' if 'authorized-a' in argv else 'b1'
                command=shlex.split(argv[-1]); self.assertEqual(command[:2],['python3','-c'])
                command[2]=command[2].replace("Path('/etc/machine-id').read_text().strip()",repr('machine-'+target))
                return subprocess.run([sys.executable,*command[1:]],check=True,text=True,capture_output=True).stdout
            def startup(argv):
                run=Path(argv[argv.index('--run-dir')+1]); node=argv[argv.index('--node-id')+1]
                topology=Path(argv[argv.index('--config')+1]); private=Path(argv[argv.index('--token-file')+1])
                (run/'topology.json').write_bytes(topology.read_bytes())
                (run/'control.token').write_bytes(private.read_bytes())
                if node=='a1': (run/'summary.json').write_text(json.dumps(dict(status='PASS',combined_trace_gate='PASS')))
                return 0
            def allocation(stage,argv,**kw):
                self.assertEqual(stage,'prepare-dram-allocation')
                return json.dumps(dict(status='ALLOCATION_ONLY_PASS',rdma_registered=False,results=[]))
            with patch.object(prep.sys,'platform','linux'),patch.object(Path,'read_text',new=read), \
                 patch.object(lab,'prepare',side_effect=build),patch.object(prep,'inspect',side_effect=inspect), \
                 patch.object(lab,'main',side_effect=startup) as starts, \
                 patch.object(lab.Runner,'call',side_effect=allocation), \
                 patch.object(matrix,'rpc',return_value=dict(topology_verified=True,
                     operations=[dict(state='COMMITTED'),dict(state='COMMITTED')])), \
                 patch.object(matrix.resource,'getrlimit',return_value=(prep.resource.RLIM_INFINITY,)*2), \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                for node in ('a1','b1'):
                    context.node=node; host=root/node; host.mkdir(); topology=self.fresh_fixture(host)
                    raw=json.loads(topology.read_text()); raw['lab_run']['wait_seconds']=2
                    topology.write_text(json.dumps(raw))
                    args=SimpleNamespace(node=node,action='init',topology=str(topology),generate_only=False,
                        jobs=1,skip_apt=True,sizes=[1,2],unit='MB',repetitions=1,
                        backings=['local_numa','cxl'],page_modes=['native'],peer=None,remote_root=None,peer_config=None)
                    path=host/'benchmark.local.json'
                    command=['matrix','init','--config',str(path),'--node',node,'--topology',str(topology),
                             '--sizes','1','2','--unit','MB','--repetitions','1','--page-modes','native','--skip-apt']
                    with patch.object(sys,'argv',command), \
                         patch.object(prep.subprocess,'check_output',side_effect=AssertionError('no init network')):
                        self.assertEqual(matrix.main(),0)
                    hosts[node]=(path,args,Path(json.loads(path.read_text())['preparation_dir']))
                for node,(path,args,work) in hosts.items():
                    # Model a session created before the DRAM-only mbind repair.
                    # Real checkout hashes still come from the actual SSH export program.
                    existing=json.loads(path.read_text())
                    existing['fresh_session']['software']['callosum_dram_benchmark.py']='prior-dram-version'
                    path.write_text(json.dumps(existing))
                    args.action='prepare'; other='b1' if node=='a1' else 'a1'
                    args.peer='authorized-b' if node=='a1' else 'authorized-a'
                    args.peer_config=str(hosts[other][0])
                def prepare(node):
                    context.node=node; path,args,work=hosts[node]; runner=lab.Runner(work)
                    try: matrix.prepare_session(path,args,work,runner)
                    except Exception as error: errors.append((node,str(error)))
                    finally: runner.log.close()
                with patch.object(prep.subprocess,'check_output',side_effect=ssh), \
                     patch.dict(os.environ,{'CALLOSUM_DESTRUCTIVE_ACK':'1'}):
                    threads=[threading.Thread(target=prepare,args=(n,),name=n) for n in hosts]
                    for thread in threads: thread.start()
                    for thread in threads: thread.join(timeout=8)
                    self.assertTrue(all(not t.is_alive() for t in threads),'prepare handoff did not converge')
                    self.assertEqual(errors,[])
                    self.assertEqual(starts.call_count,2)
                    for path,args,work in hosts.values():
                        proof=json.loads((work/'launcher-refresh.json').read_text())
                        self.assertTrue(proof['native_runtime_contract_unchanged'])
                        self.assertEqual(proof['session_id'],json.loads(path.read_text())['fresh_session']['id'])
                    # Refresh uses success markers, not another service owner.
                    context.node='a1'; prepare('a1')
                    self.assertEqual(starts.call_count,2)
                def selected(label):
                    def record(spec,c,args,*unused): observed.append((label,args.node,args.sizes,args.unit,args.repetitions))
                    return record
                for node,(path,args,work) in hosts.items():
                    self.assertEqual(json.loads((work/'prepare.json').read_text())['status'],'PREPARED_NOT_TESTED')
                    context.node=node
                    with patch.object(sys,'argv',['matrix','run','--config',str(path),'--node',node]), \
                         patch.dict(os.environ,IO_SIZES='1 2 4 8 16 32 64 128',IO_UNIT='MiB'), \
                         patch.object(matrix,'receive',side_effect=selected('receiver')), \
                         patch.object(matrix,'run',side_effect=selected('sender')), \
                         patch.object(matrix,'receiver_health',return_value={}), \
                         patch.object(prep.subprocess,'check_output',side_effect=AssertionError('no run SSH')):
                        self.assertEqual(matrix.main(),0)
                self.assertEqual(observed,[('receiver','a1',[1,2],'MB',1),('sender','b1',[1,2],'MB',1)])
                for path,_,_ in hosts.values():
                    token=Path(json.loads(path.read_text())['token_file']).read_text().strip()
                    self.assertNotIn(token,output.getvalue())

    def test_init_rejects_ssh_options_and_prepare_requires_explicit_peer(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); topology=self.fresh_fixture(root); target=root/'new.json'
            command=['matrix','init','--config',str(target),'--node','a1','--topology',str(topology),
                     '--generate-only','--peer','not-implicitly-used']
            with patch.object(sys,'argv',command),contextlib.redirect_stderr(io.StringIO()) as errors:
                self.assertEqual(matrix.main(),1)
            self.assertIn('SSH options belong only to prepare',errors.getvalue())
            self.assertFalse(target.exists())
            args=SimpleNamespace(peer=None)
            with patch.object(matrix,'prepare_session') as start:
                with self.assertRaisesRegex(RuntimeError,'requires --peer'):
                    matrix.prepare_connected(target,args,root/'work',None)
                start.assert_not_called()

    def test_prepare_peer_port_applies_to_session_and_owned_connection_cleanup(self):
        import callosum_lab as lab
        for port in (None,2222):
            with self.subTest(port=port),tempfile.TemporaryDirectory() as directory:
                root=Path(directory); args=SimpleNamespace(peer='labuser@192.0.2.2',peer_port=port)
                commands=[]
                def session(path,options,work,runner,ssh_options):
                    commands.append(lab.remote_command(options.peer,['python3','-c','pass'],options=ssh_options))
                    control=next(o.split('=',1)[1] for o in ssh_options if o.startswith('ControlPath='))
                    Path(control).touch()
                with patch.object(matrix,'prepare_session',side_effect=session), \
                     patch.object(matrix.subprocess,'run') as cleanup:
                    matrix.prepare_connected(root/'config.json',args,root,None)
                commands.append(cleanup.call_args.args[0])
                for command in commands:
                    if port is None: self.assertNotIn('-p',command)
                    else: self.assertEqual(command[command.index('-p')+1],str(port))
                    self.assertIn(args.peer,command)

    def test_peer_port_cli_rejects_invalid_ports_and_non_prepare_actions(self):
        for action,port in [('prepare','0'),('prepare','65536'),('init','2222'),('run','2222')]:
            with self.subTest(action=action,port=port), \
                 patch.object(sys,'argv',['matrix',action,'--config','unused.json','--node','b1','--peer-port',port]), \
                 patch.object(matrix,'prepare_connected') as start,contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(matrix.main(),1)
                start.assert_not_called()

    def test_cold_preflight_does_not_query_cxl_peer_or_claim_ready(self):
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args=self.inspect_fixture(root,stack)
            args.action='init'
            with patch.object(prep,'rpc',side_effect=AssertionError('cold inspection must not contact CXL services')), \
                 patch.object(matrix,'receiver_health',side_effect=AssertionError('init must not send receiver RPC')) as receiver:
                report=prep.inspect(path,args,root/'work',matrix.load,live=False)
            receiver.assert_not_called()
            self.assertEqual(report['status'],'INPUTS_CHECKED_NOT_STARTED',report['issues'])
            self.assertFalse(report['live_services_checked'])

    def test_prepare_preserves_four_local_blockers_and_starts_no_ssh_or_service(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=matrix_tests.MatrixTest().fixture(root)
            spec=json.loads(path.read_text()); work=root/'preparation'
            profile={key:getattr(args,key) for key in ('sizes','unit','repetitions','backings','page_modes')}
            spec.update(workflow=dict(node='a1',bound=True),fresh_session=dict(id='fixture-session',profile=profile),
                        preparation_dir=str(work))
            path.write_text(json.dumps(spec)); original=path.read_bytes()
            issues=[dict(check=name,error='fixture '+name) for name in
                    ('hugetlb_permission','dax_access','cxl_budget','cxl_arena')]
            def blocked(*unused,**kwargs):
                return prep.save_readiness(work,dict(status='BLOCKED',node='a1',issues=issues),
                                           (root/'control.token').read_text().strip())
            command=['matrix','prepare','--config',str(path),'--node','a1','--peer','operator-supplied-peer']
            with patch.object(sys,'argv',command),patch.object(prep,'inspect',side_effect=blocked), \
                 patch.object(matrix,'bind_peer',side_effect=AssertionError('no peer handoff')) as handoff, \
                 patch.object(matrix,'launch_fresh',side_effect=AssertionError('no service startup')) as startup, \
                 patch.object(prep.subprocess,'check_output',side_effect=AssertionError('no SSH')), \
                 contextlib.redirect_stderr(io.StringIO()) as errors:
                self.assertEqual(matrix.main(),1)
            failure=json.loads(errors.getvalue())
            self.assertEqual(failure['stage'],'prepare-local-prerequisites')
            self.assertEqual(failure['issues'],issues)
            self.assertFalse(failure['rdma_work_possible'])
            self.assertFalse(failure['owned_service_transition_possible'])
            self.assertEqual(failure['owned_children_started'],[])
            self.assertEqual(json.loads((work/'readiness.json').read_text())['issues'],issues)
            self.assertEqual(path.read_bytes(),original)
            handoff.assert_not_called(); startup.assert_not_called()

    def test_cold_budget_and_arena_errors_identify_exact_config_fields(self):
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args=self.inspect_fixture(root,stack)
            topology=json.loads((root/'topology.json').read_text())
            topology['max_inflight_bytes']=64*1024**2
            topology['pools']['A']['capacity']=1024**3
            (root/'topology.json').write_text(json.dumps(topology))
            report=prep.inspect(path,args,root/'work',matrix.load,live=False)
            issues={item['check']:item for item in report['issues']}
            budget=issues['cxl_budget']; arena=issues['cxl_arena']
            self.assertEqual((budget['config_field'],budget['configured_bytes'],budget['required_bytes']),
                             ('max_inflight_bytes',67108864,128000000))
            self.assertEqual((arena['config_field'],arena['configured_bytes'],arena['required_bytes']),
                             ('pools.A.capacity',1073741824,1532203008))
            self.assertIn('owner approval',arena['next_action'])
            self.assertFalse(report['services_started']); self.assertFalse(report['devices_mapped'])
            self.assertEqual(json.loads((root/'topology.json').read_text()),topology)

    def test_first_run_cli_handoff_then_repeat_without_historical_directories(self):
        """Exercise the shipped Bash entry, real JSON/token handoff and read-only retry.

        Build receipts model a successful Linux build. This is not native-build
        or hardware evidence. No hidden checkout-local config is consumed.
        """
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); hosts={}
            wrapper=Path(__file__).resolve().parents[2]/'scripts/rdma-write-matrix.sh'
            for node in ('a1','b1'):
                host=root/node; host.mkdir()
                _,args=matrix_tests.MatrixTest().fixture(host)
                topology=json.loads((host/'topology.json').read_text())
                topology['lab_run']=dict(controller='a1',source='b1',requester='a1',
                                         profile='two_worker_smoke',wait_seconds=1800)
                (host/'topology.json').write_text(json.dumps(topology))
                # Each host has a different freshly generated build receipt path.
                build=host/'fresh-local-build'; build.mkdir()
                (host/'setup.json').rename(build/'setup.json')
                token=host/'new-control.token'
                command=['bash',str(wrapper),'init','--config',str(host/'benchmark.local.json'),
                    '--node',node,'--topology',str(host/'topology.json'),
                    '--setup-run',str(build),'--token-file',str(token)]
                if node=='a1': command+=['--create-token']
                else:
                    token.write_bytes(hosts['a1']['token'].read_bytes()); token.chmod(0o600)
                missing=list(command); missing[missing.index('--setup-run')+1]=str(host/'missing-build')
                failed=subprocess.run(missing,cwd=host,text=True,capture_output=True)
                self.assertNotEqual(failed.returncode,0)
                failure=json.loads(failed.stderr)
                self.assertEqual(failure['issues'],[dict(input='local setup receipt',
                    path=str(host/'missing-build/setup.json'),reason='MISSING')])
                self.assertFalse(failure['rdma_work_possible'])
                self.assertFalse((host/'benchmark.local.json').exists())
                if node=='a1': self.assertFalse(token.exists())
                run=subprocess.run(command,cwd=host,text=True,capture_output=True)
                self.assertEqual(run.returncode,0,run.stderr)
                result=json.loads(run.stdout)
                self.assertEqual(result['setup_receipt'],str(build/'setup.json'))
                config=host/'benchmark.local.json'; original=config.read_bytes()
                self.assertEqual(set(json.loads(original)['runtimes']),{node})
                self.assertEqual(stat.S_IMODE(token.stat().st_mode),0o600)
                self.assertNotIn(token.read_text().strip(),run.stdout+run.stderr)
                # Re-running init does not overwrite a valid config or token.
                repeat=subprocess.run(command,cwd=host,text=True,capture_output=True)
                self.assertNotEqual(repeat.returncode,0)
                self.assertEqual(config.read_bytes(),original)
                self.assertFalse(result['services_started']); self.assertFalse(result['devices_mapped'])
                args.node=node; args.action='prepare'; args.unit='MB'
                loaded=matrix.load(config,args)
                self.assertEqual(loaded[3],build)
                # A pre-service prepare retains a useful report, not a fake PASS.
                # Explicitly model the inaccessible-lab boundary on every OS.
                with patch.object(prep.sys,'platform','darwin'):
                    report=prep.inspect(config,args,host/'work',matrix.load)
                    again=prep.inspect(config,args,host/'work',matrix.load)
                self.assertEqual(again['status'],'BLOCKED')
                self.assertTrue(report['evidence_saved'])
                self.assertIn('platform',{i['check'] for i in report['issues']})
                self.assertEqual(report['setup_receipt']['path'],str(build/'setup.json'))
                self.assertFalse(again['services_started']); self.assertFalse(again['devices_mapped'])
                self.assertTrue(list((host/'work').glob('readiness.json.*.previous')))
                hosts[node]=dict(token=token,plan=report['plan'])
            self.assertEqual(hosts['a1']['plan'],hosts['b1']['plan'])

    def test_invalid_receipt_token_node_and_retained_token_do_not_create_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); _,args=matrix_tests.MatrixTest().fixture(root)
            args.topology=str(root/'topology.json'); args.setup_run=str(root)
            args.peer_setup_run=None; args.token_file=str(root/'different.token')
            args.source='b1'; args.receiver='a1'; args.node='a1'; args.create_token=False
            private=Path(args.token_file); private.write_text('short'); private.chmod(0o600)
            target=root/'new.json'
            with self.assertRaisesRegex(RuntimeError,'32 characters'): prep.initialize(target,args)
            self.assertFalse(target.exists())
            private.write_text('different-private-token-'*3)
            with self.assertRaisesRegex(RuntimeError,'token differs'): prep.initialize(target,args)
            self.assertFalse(target.exists())
            args.token_file=str(root/'control.token'); args.node='unknown'
            with self.assertRaisesRegex(RuntimeError,'select --source'): prep.initialize(target,args)
            args.node='a1'; receipt=root/'setup.json'; old=receipt.read_text()
            receipt.write_text(json.dumps(dict(status='FAIL')))
            with self.assertRaisesRegex(RuntimeError,'must be PASS'): prep.initialize(target,args)
            self.assertFalse(target.exists()); receipt.write_text(old)
            args.node='b1'; args.create_token=True
            with self.assertRaisesRegex(RuntimeError,'controller-only'): prep.initialize(target,args)
            self.assertFalse(target.exists())

    def test_new_receipt_from_a_different_machine_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); matrix_tests.MatrixTest().fixture(root)
            path=root/'setup.json'; value=json.loads(path.read_text())
            value['host_identity']=dict(machine_id='other-machine')
            path.write_text(json.dumps(value))
            with patch.object(prep.sys,'platform','linux'), \
                 patch.object(Path,'read_text',side_effect=[json.dumps(value),'this-machine']):
                with self.assertRaisesRegex(RuntimeError,'different host'): prep.runtime_receipt(root)

    def test_invalid_inputs_collected_and_created_token_survives_output_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); _,args=matrix_tests.MatrixTest().fixture(root)
            topology=json.loads((root/'topology.json').read_text())
            topology['lab_run']=dict(controller='a1',source='b1',requester='a1')
            (root/'topology.json').write_text(json.dumps(topology))
            args.node='a1'; args.topology=str(root/'topology.json'); args.setup_run=str(root)
            args.token_file=str(root/'new-control.token'); args.create_token=True
            args.source='b1'; args.receiver='a1'; args.peer_setup_run=None
            private=Path(args.token_file); private.write_text('short'); private.chmod(0o600)
            receipt=root/'setup.json'; original=receipt.read_bytes()
            receipt.write_text(json.dumps(dict(status='FAIL')))
            with self.assertRaises(RuntimeError) as caught: prep.initialize(root/'new.json',args)
            self.assertEqual({i['input'] for i in caught.exception.issues},{'local setup receipt','shared token'})
            self.assertEqual(private.read_text(),'short')
            receipt.write_bytes(original)
            # Use a separate fresh build to avoid changing the retained fixture token.
            fresh=root/'fresh'; fresh.mkdir(); (fresh/'setup.json').write_bytes(original)
            args.setup_run=str(fresh); args.token_file=str(root/'fresh-control.token')
            target=root/'denied.json'; original_open=Path.open
            def deny_output(path,*a,**kw):
                if path==target: raise PermissionError('fixture config directory denied')
                return original_open(path,*a,**kw)
            with patch.object(Path,'open',new=deny_output):
                with self.assertRaises(PermissionError) as caught: prep.initialize(target,args)
            side=caught.exception.initialization_side_effects
            self.assertTrue(side['token_created']); self.assertFalse(side['configuration_exists'])
            token=Path(args.token_file).read_bytes()
            prep.initialize(target,args)
            self.assertEqual(Path(args.token_file).read_bytes(),token)

    def test_canonical_build_rerun_rejects_failed_receipt_then_reuses_valid_receipt(self):
        from callosum_lab import main as lab_main
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); matrix_tests.MatrixTest().fixture(root)
            receipt=root/'setup.json'; original=receipt.read_bytes()
            topology=(root/'topology.json').read_bytes()
            receipt.write_text(json.dumps(dict(status='FAIL')))
            command=['--root',str(Path(__file__).resolve().parents[2]),'--run-dir',str(root),'--action','prepare']
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(lab_main(command),1)
            receipt.write_bytes(original)
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(lab_main(command),0)
            self.assertIn('[REUSED]',output.getvalue())
            self.assertEqual(receipt.read_bytes(),original)
            self.assertEqual((root/'topology.json').read_bytes(),topology)

    def test_signed_ready_handoff_checks_host_plan_source_and_intended_token(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with contextlib.ExitStack() as stack:
                path,args=self.inspect_fixture(root,stack)
                local=prep.inspect(path,args,root/'work',matrix.load)
                self.assertEqual(local['status'],'PREPARED_NOT_TESTED',local['issues'])
                spec,c,_,_,private,_,_=matrix.load(path,args)
                secret=private.read_text().strip()
                peer=dict(local,node='b1',machine=dict(machine_id='host-b1',boot_id='boot-b1'))
                handed=root/'peer-b1.json'; handed.write_text(json.dumps(prep.signed(peer,secret)))
                self.assertEqual(prep.verify_peer(handed,secret,c,spec,args)['node'],'b1')
                with self.assertRaisesRegex(RuntimeError,'signature differs'):
                    prep.verify_peer(handed,'wrong-setup-token-'*3,c,spec,args)
                peer['machine']=local['machine']; handed.write_text(json.dumps(prep.signed(peer,secret)))
                with self.assertRaisesRegex(RuntimeError,'same physical host'):
                    prep.verify_peer(handed,secret,c,spec,args)

    def test_init_reports_all_missing_inputs_before_creating_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); target=root/'benchmark.local.json'
            args=SimpleNamespace(topology=str(root/'lab.json'),setup_run=str(root/'old-run'),
                peer_setup_run=str(root/'peer-run'),token_file=str(root/'control.token'))
            with self.assertRaises(RuntimeError) as caught: prep.initialize(target,args)
            self.assertEqual(len(caught.exception.issues),3)
            self.assertTrue(all(i['reason']=='MISSING' for i in caught.exception.issues))
            self.assertIn(str(root/'old-run/setup.json'),str(caught.exception))
            self.assertFalse(target.exists())

    def test_init_distinguishes_denied_receipt_from_missing_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); target=root/'benchmark.local.json'
            args=SimpleNamespace(topology=str(root/'lab.json'),setup_run=str(root/'old-run'),
                peer_setup_run=str(root/'peer-run'),token_file=str(root/'control.token'))
            real_open=Path.open
            def open_input(path,*a,**kw):
                if path==root/'old-run/setup.json': raise PermissionError(13,'fixture denied',str(path))
                return real_open(path,*a,**kw)
            with patch.object(Path,'open',new=open_input):
                with self.assertRaises(RuntimeError) as caught: prep.initialize(target,args)
            issue=next(i for i in caught.exception.issues if i['input']=='local setup receipt')
            self.assertEqual(issue['reason'],'ACCESS_DENIED')
            self.assertFalse(target.exists())

    def test_permission_failure_preserves_all_diagnostics_and_old_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory); old=work/'readiness.json'
            old.write_text('previous evidence')
            secret='private-fixture-token-'*3
            real_write=Path.write_bytes
            def deny_backup(path,data):
                if path.name.endswith('.previous'): raise PermissionError(13,'Permission denied',str(path))
                return real_write(path,data)
            with patch.object(Path,'write_bytes',new=deny_backup):
                report=prep.save_readiness(work,dict(status='PREPARED_NOT_TESTED',issues=[],
                    services_started=False,devices_mapped=False),secret)
            self.assertEqual(report['status'],'BLOCKED')
            self.assertFalse(report['evidence_saved'])
            self.assertEqual(report['issues'][-1]['check'],'evidence_storage')
            self.assertEqual(old.read_text(),'previous evidence')
            self.assertEqual(report['signature'],prep.signed(report,secret)['signature'])
            self.assertNotIn(secret,json.dumps(report))
            # Correcting storage access permits repetition without deleting evidence.
            recovered=prep.save_readiness(work,dict(status='PREPARED_NOT_TESTED',issues=[]),secret)
            self.assertTrue(recovered['evidence_saved'])
            self.assertEqual(json.loads(old.read_text())['status'],'PREPARED_NOT_TESTED')
            self.assertEqual(next(work.glob('*.previous')).read_text(),'previous evidence')

    def test_storage_failure_does_not_hide_independent_configuration_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory)
            with patch.object(prep,'archive',side_effect=PermissionError('fixture evidence directory denied')):
                report=prep.inspect(work/'missing.json',SimpleNamespace(node='a1',action='prepare'),work,matrix.load)
            self.assertFalse(report['evidence_saved'])
            checks={issue['check'] for issue in report['issues']}
            self.assertTrue({'matrix_config','configuration','evidence_storage'}<=checks)
            self.assertFalse(report['services_started']); self.assertFalse(report['devices_mapped'])

    def test_preflight_missing_files_are_collected_without_hidden_local_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'matrix.json').write_text(json.dumps(dict(topology_file='missing-topology.json',
                token_file='missing.token',runtimes={'a1':dict(setup_run='missing-setup')})))
            args=SimpleNamespace(node='a1',action='prepare')
            report=prep.inspect(root/'matrix.json',args,root/'work',matrix.load)
            checks={i['check'] for i in report['issues']}
            self.assertTrue({'topology_path','token_path','setup_path','configuration'}<=checks)
            self.assertFalse(report['services_started']); self.assertFalse(report['devices_mapped'])
            self.assertEqual(report['status'],'BLOCKED')

    def inspect_fixture(self,root,stack,stale=False,free=62):
        path,args=matrix_tests.MatrixTest().fixture(root)
        args.action='prepare'; args.node='a1'; args.unit='MB'
        setup=json.loads((root/'setup.json').read_text()); setup['build']=str(root/'build')
        (root/'setup.json').write_text(json.dumps(setup))
        c=matrix.load(path,args)[1]
        real_read=Path.read_text; real_iter=Path.iterdir; real_stat=os.stat; real_access=os.access
        def read(p,*a,**kw):
            name=str(p)
            if name=='/etc/machine-id': return 'host-a1'
            if name=='/proc/sys/kernel/random/boot_id': return 'boot-a1'
            if name=='/proc/self/status': return 'Mems_allowed_list:\t0\nCapEff:\t0000\n'
            if name=='/proc/sys/vm/hugetlb_shm_group': return '0'
            if '/cpulist' in name: return '0'
            if name.endswith('/node0/meminfo'): return 'Node 0 MemFree: 1048576 kB\n'
            if name=='/fake-port/state': return '4: ACTIVE'
            if name.startswith('/sys/dev/char/') and name.endswith('/size'): return str(2**41)
            if name.startswith('/sys/dev/char/') and name.endswith('/align'): return str(2**21)
            return real_read(p,*a,**kw)
        def dirs(p):
            if str(p).endswith('/ports'): return iter([Path('/fake-port')])
            if str(p)=='/proc': return iter([])
            return real_iter(p)
        def st(path,*a,**kw):
            if str(path)=='/dev/not-opened': return SimpleNamespace(st_mode=stat.S_IFCHR,st_rdev=0)
            return real_stat(path,*a,**kw)
        def native(argv,**kw):
            if argv[:3]==['ip','-j','addr']: return json.dumps([dict(addr_info=[dict(local='127.0.0.1')])])
            return json.dumps({'engine-test.so':'test-hash'})
        def network(url,method,**kw):
            if method=='status': return dict(fingerprint=c['fingerprint'],topology_verified=True,
                benchmark_request_timing_versions={} if stale else {'b1':1},operations=[],
                remaining_capacity_bytes={'A':2*1024**3,'B':1024**3})
            member=next(n for n in c['nodes'] if n['url']==url)
            return dict(id=member['id'],abi_version=2,configured_qps='16',
                transport_env=dict(MC_MTU=None,MC_GID_INDEX=None),
                runtime_sha256=prep.RUNTIME_SHA256,implementation_sha256=prep.IMPLEMENTATION_SHA256,
                transport_request_timing_version=0 if stale else 1,
                mr_pages=dict(device_alignment_bytes=2**21,kernel_page_size_bytes=2**21,virtual_alignment_2MiB=True))
        for manager in [patch.object(Path,'read_text',new=read),patch.object(Path,'iterdir',new=dirs),
            patch.object(prep.os,'stat',side_effect=st),
            patch.object(prep.os,'access',side_effect=lambda p,m:True if str(p)=='/dev/not-opened' else real_access(p,m)),
            patch.object(prep.os,'geteuid',return_value=0),patch.object(prep.sys,'platform','linux'),
            patch.object(prep.os,'sched_getaffinity',return_value={0},create=True),
            patch.object(prep.resource,'getrlimit',return_value=(prep.resource.RLIM_INFINITY,)*2),
            patch.object(prep.subprocess,'check_output',side_effect=native),patch.object(prep,'rpc',side_effect=network),
            patch.object(prep,'huge_snapshot',return_value=dict(node=0,total_pages=free,free_pages=free,
                available_pages=free,global_reserved_pages=0,global_free_pages=free)),
            patch.object(matrix,'receiver_health',side_effect=RuntimeError('not started'))]:
            stack.enter_context(manager)
        # Binds are modelled, not listeners. These fixtures issue no network traffic.
        stack.enter_context(patch.object(prep.socket,'socket'))
        return path,args

    def test_hugetlb_permission_accepts_configured_primary_group_without_root(self):
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args=self.inspect_fixture(root,stack)
            with patch.object(prep.os,'geteuid',return_value=1000), \
                 patch.object(prep.os,'getegid',return_value=0),patch.object(prep.os,'getgroups',return_value=[]):
                report=prep.inspect(path,args,root/'work',matrix.load,live=False)
            self.assertFalse(any(i['check']=='hugetlb_permission' for i in report['issues']))
            self.assertEqual(report['execution_account']['effective_uid'],1000)
            self.assertEqual(report['hugetlb_permission']['uid'],1000)

    def test_failure_stale_agent_and_zero_pages_reported_together_then_repeat_is_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with contextlib.ExitStack() as stack:
                path,args=self.inspect_fixture(root,stack,stale=True,free=0)
                report=prep.inspect(path,args,root/'work',matrix.load)
                checks={i['check'] for i in report['issues']}
                self.assertTrue({'hugepages_capacity','timing_b1','coordinator_timing'}<=checks,report['issues'])
                self.assertEqual(report['hugepages_required_for_new_buffer'],62)
                private=(root/'control.token').read_text().strip()
                self.assertNotIn(private,json.dumps(report))
            with tempfile.TemporaryDirectory() as other:
                other=Path(other)
                with contextlib.ExitStack() as stack:
                    path,args=self.inspect_fixture(other,stack,stale=False,free=62)
                    first=prep.inspect(path,args,other/'work',matrix.load)
                    second=prep.inspect(path,args,other/'work',matrix.load)
                    self.assertEqual(first['status'],'PREPARED_NOT_TESTED',first['issues'])
                    self.assertEqual(second['status'],'PREPARED_NOT_TESTED')
                    self.assertTrue(list((other/'work').glob('*.previous')))

    def test_old_live_schema_is_not_reported_as_wrong_settings_or_zero_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with contextlib.ExitStack() as stack:
                path,args=self.inspect_fixture(root,stack)
                original=prep.rpc.side_effect
                def old(url,method,**kwargs):
                    value=original(url,method,**kwargs)
                    fields=(['benchmark_request_timing_versions','remaining_capacity_bytes']
                            if method=='status' else ['transport_env','transport_request_timing_version','mr_pages'])
                    for field in fields: value.pop(field,None)
                    return value
                stack.enter_context(patch.object(prep,'rpc',side_effect=old))
                report=prep.inspect(path,args,root/'work',matrix.load)
                checks={issue['check'] for issue in report['issues']}
                self.assertTrue({'live_agent_a1','live_agent_b1','live_coordinator_schema'}<=checks)
                self.assertFalse({'transport_settings_a1','dax_geometry_a1','remaining_arena',
                                  'remaining_source_arena'}&checks)
                self.assertEqual(report['status'],'BLOCKED')

    def test_dax_permission_failure_identifies_existing_device_not_missing_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with contextlib.ExitStack() as stack:
                path,args=self.inspect_fixture(root,stack)
                access=prep.os.access.side_effect
                stack.enter_context(patch.object(prep.os,'access',side_effect=lambda p,m:
                    False if str(p)=='/dev/not-opened' else access(p,m)))
                report=prep.inspect(path,args,root/'work',matrix.load)
                issue=next(i for i in report['issues'] if i['check']=='dax_access')
                self.assertIn('device exists',issue['error'])
                self.assertTrue(report['dax_inventory']['exists'])
                self.assertFalse(report['dax_inventory']['read_write_access'])

    def test_dax_acl_change_preserves_other_principals_and_rejects_mask_expansion(self):
        base='user::rw-\ngroup::---\nother::---\n'
        self.assertEqual(prep.dax_acl_change(base,1000,0),'u:1000:rw-,m::rw-')
        self.assertEqual(prep.dax_acl_change('user::r--\ngroup::---\nother::---\n',1000,1000),'u::rw-')
        masked='user::rw-\nuser:2000:rw-\ngroup::---\nmask::r--\nother::---\n'
        with self.assertRaisesRegex(RuntimeError,'another principal'):
            prep.dax_acl_change(masked,1000,0)
        existing='user::rw-\nuser:1000:rw-\ngroup::---\nmask::rw-\nother::---\n'
        with self.assertRaisesRegex(RuntimeError,'already allows rw'):
            prep.dax_acl_change(existing,1000,0)

    def dax_access_fixture(self,root,stack):
        """Mock device metadata, ACL tools and opens. Never touch a real DAX."""
        path,args=matrix_tests.MatrixTest().fixture(root); args.action='prepare'; args.node='a1'
        device='/dev/dax1.0'; state=dict(granted=False,tools=True,extent=True,addresses=True,open_ok=True)
        before='user::rw-\ngroup::---\nother::---\n'
        after='user::rw-\nuser:1000:rw-\ngroup::---\nmask::rw-\nother::---\n'
        def st(*unused,**kwargs):
            return SimpleNamespace(st_mode=stat.S_IFCHR|(0o660 if state['granted'] else 0o600),
                                   st_rdev=123,st_ino=42,st_uid=0,st_gid=0)
        def report(path,args,work,loader,live=False):
            return dict(status='INPUTS_CHECKED_NOT_STARTED' if state['granted'] else 'BLOCKED',node='a1',
                issues=[] if state['granted'] else [dict(check='dax_access',error='fixture access denied')],
                local_addresses=['fixture-local'] if state['addresses'] else [],
                dax_inventory=dict(path=device,exists=True,character_device=True,device_number=123,inode=42,
                    extent_validated=state['extent'],read_write_access=state['granted']),
                services_started=False,devices_mapped=False)
        real_file=Path.is_file; real_access=os.access; real_open=os.open; real_close=os.close
        tools={'/usr/bin/getfacl','/usr/bin/setfacl'}
        def open_device(p,*a,**kw):
            if str(p)!=device: return real_open(p,*a,**kw)
            if not state['open_ok']: raise PermissionError('fixture driver access denied')
            return 999999
        for manager in [patch.object(prep,'inspect',side_effect=report),
            patch.object(prep.os,'lstat',side_effect=st),patch.object(prep.os,'getuid',return_value=1000),
            patch.object(prep.os,'geteuid',return_value=1000),patch.object(prep.sys,'platform','linux'),
            patch.object(Path,'is_file',new=lambda p:state['tools'] if str(p) in tools else real_file(p)),
            patch.object(prep.os,'access',side_effect=lambda p,m:state['tools'] if str(p) in tools else real_access(p,m)),
            patch.object(prep.os,'open',side_effect=open_device),
            patch.object(prep.os,'close',side_effect=lambda fd:None if fd==999999 else real_close(fd))]:
            stack.enter_context(manager)
        commands=[]
        def command(stage,argv,*a,**kw):
            commands.append(argv)
            if argv==['sudo','-v']: return
            if argv==['sudo','-n','apt-get','install','-y','acl']:
                state['tools']=True; return
            if argv[0]=='/usr/bin/getfacl': return after if state['granted'] else before
            self.assertEqual(argv,['sudo','-n','/usr/bin/setfacl','--no-mask','-m','u:1000:rw-,m::rw-','--',device])
            state['granted']=True
        return path,args,state,commands,command

    def test_dax_access_sudo_grants_one_uid_and_rechecks_without_root_benchmark(self):
        from callosum_lab import Runner
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args,state,commands,command=self.dax_access_fixture(root,stack)
            runner=Runner(root/'work')
            try:
                with patch.object(runner,'call',side_effect=command):
                    first=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                    again=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                self.assertEqual(first['status'],'INPUTS_CHECKED_NOT_STARTED')
                self.assertEqual(again['status'],'INPUTS_CHECKED_NOT_STARTED')
                self.assertEqual(first['dax_access_preparation']['status'],'GRANTED')
                self.assertEqual(len(commands),4)  # sudo, ACL read, UID grant, ACL read; no duplicate grant.
                self.assertTrue(runner.device_access_change_possible)
                self.assertFalse(first['services_started']); self.assertFalse(first['devices_mapped'])
                self.assertEqual(os.geteuid(),1000)
                record=json.loads((root/'work/dax-access.json').read_text())
                self.assertEqual((record['owner_uid'],record['owner_gid']),(0,0))
                self.assertIn('DEVICE_WIDE',record['protection_scope'])
            finally: runner.log.close()

    def test_dax_access_installs_only_missing_acl_tools_with_existing_package_policy(self):
        from callosum_lab import Runner
        for skip in (False,True):
            with self.subTest(skip_apt=skip),tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
                root=Path(directory); path,args,state,commands,command=self.dax_access_fixture(root,stack)
                args.skip_apt=skip; state['tools']=False; runner=Runner(root/'work')
                try:
                    with patch.object(runner,'call',side_effect=command):
                        report=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                    install=['sudo','-n','apt-get','install','-y','acl']
                    if skip:
                        self.assertEqual(report['status'],'BLOCKED')
                        self.assertNotIn(install,commands); self.assertFalse(state['granted'])
                    else:
                        self.assertEqual(report['status'],'INPUTS_CHECKED_NOT_STARTED')
                        self.assertIn(install,commands)
                finally: runner.log.close()

    def test_dax_access_does_not_repair_unverified_extent_or_wrong_local_node(self):
        from callosum_lab import Runner
        for field in ('extent','addresses'):
            with self.subTest(field=field),tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
                root=Path(directory); path,args,state,commands,command=self.dax_access_fixture(root,stack)
                state[field]=False; runner=Runner(root/'work')
                try:
                    with patch.object(runner,'call',side_effect=AssertionError('no privilege request')):
                        report=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                    self.assertEqual(report['status'],'BLOCKED'); self.assertFalse(state['granted'])
                finally: runner.log.close()

    def test_dax_access_denied_sudo_or_failed_caller_open_retains_evidence_without_fallback(self):
        from callosum_lab import Runner
        for case in ('sudo','caller_open'):
            with self.subTest(case=case),tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
                root=Path(directory); path,args,state,commands,command=self.dax_access_fixture(root,stack)
                runner=Runner(root/'work'); state['open_ok']=case!='caller_open'
                def operate(*a,**kw):
                    if case=='sudo': raise RuntimeError('fixture sudo denied')
                    return command(*a,**kw)
                try:
                    with patch.object(runner,'call',side_effect=operate):
                        report=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                    self.assertEqual(report['status'],'BLOCKED')
                    attempt=report['dax_access_preparation']
                    self.assertEqual(attempt['status'],'FAILED')
                    self.assertEqual(attempt['change_possible'],case=='caller_open')
                    self.assertEqual(state['granted'],case=='caller_open')
                    self.assertTrue((root/'work/dax-access.json').is_file())
                    self.assertFalse(report['services_started']); self.assertFalse(report['devices_mapped'])
                finally: runner.log.close()

    def auto_hugepage_fixture(self,root,stack):
        """Model sysfs/sudo only. This fixture never performs a privileged write."""
        path,args=matrix_tests.MatrixTest().fixture(root)
        args.action='init'; args.node='a1'; args.unit='MB'
        state=dict(total_pages=10,free_pages=4,available_pages=4,global_free_pages=4,
            global_reserved_pages=0,node=1,
            control_file='/sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages')
        extra=[]; reused=[False]; needed=[62]
        def inspect(path,args,work,loader,live=False):
            issues=list(extra)
            if needed[0] and state['available_pages']<needed[0] and not reused[0]:
                issues.append(dict(check='hugepages_capacity',error='fixture capacity shortage'))
            return dict(status='BLOCKED' if issues else 'INPUTS_CHECKED_NOT_STARTED',node='a1',issues=issues,
                local_addresses=['fixture-local'],numa=dict(node=1),hugepages=dict(state),
                hugepages_required_for_new_buffer=needed[0],reused_huge_receiver=reused[0],
                services_started=False,devices_mapped=False)
        stack.enter_context(patch.object(prep,'inspect',side_effect=inspect))
        stack.enter_context(patch.object(prep,'huge_snapshot',side_effect=lambda *a,**k:dict(state)))
        stack.enter_context(patch.object(prep.sys,'platform','linux'))
        stack.enter_context(patch.object(prep.os,'geteuid',return_value=1000))
        return path,args,state,extra,reused,needed

    def test_auto_hugepages_sudo_writes_only_numa_deficit_then_rechecks_and_reuses(self):
        from callosum_lab import Runner
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args,state,extra,reused,needed=self.auto_hugepage_fixture(root,stack)
            runner=Runner(root/'work')
            def command(stage,argv,*a,**kw):
                if argv==['sudo','-v']: return
                self.assertEqual(argv,['sudo','-n','tee','--',state['control_file']])
                self.assertEqual(kw['input_text'],'68\n')  # 10 total + (62 required - 4 free).
                state.update(total_pages=68,free_pages=62,available_pages=62,global_free_pages=62)
                return '68\n'
            try:
                with patch.object(runner,'call',side_effect=command) as calls,contextlib.redirect_stdout(io.StringIO()):
                    first=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                    second=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                self.assertEqual(calls.call_count,2)  # One authentication, one bounded privileged write.
                self.assertEqual(first['status'],'INPUTS_CHECKED_NOT_STARTED')
                self.assertEqual(first['hugepage_auto_provision']['status'],'PROVISIONED')
                self.assertEqual(second['status'],'INPUTS_CHECKED_NOT_STARTED')
                self.assertFalse(first['services_started']); self.assertFalse(first['devices_mapped'])
                self.assertTrue(runner.hugepage_pool_change_possible)
                record=json.loads((root/'work/hugepage-provision.json').read_text())
                self.assertEqual(record['target_total_pages'],68)
                self.assertFalse(record['reclaimed'])
            finally: runner.log.close()

    def test_init_entry_automatically_provisions_without_extra_operator_action(self):
        import callosum_lab as lab
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); _,args,state,extra,reused,needed=self.auto_hugepage_fixture(root,stack)
            raw=json.loads((root/'topology.json').read_text())
            raw.update(evidence_class='TRANSFER_ONLY',lab_run=dict(controller='a1',source='b1',requester='a1',
                profile='two_worker_smoke',wait_seconds=1800))
            (root/'approved.json').write_text(json.dumps(raw))
            template=(root/'setup.json').read_bytes(); real_read=Path.read_text; commands=[]
            def read(path,*a,**kw):
                return 'fixture-a1' if str(path)=='/etc/machine-id' else real_read(path,*a,**kw)
            def build(source,run,options,runner): (run/'setup.json').write_bytes(template)
            def call(runner,stage,argv,*a,**kw):
                commands.append(argv)
                if argv==['sudo','-v']: return
                self.assertEqual(argv,['sudo','-n','tee','--',state['control_file']])
                self.assertEqual(kw['input_text'],'68\n')
                state.update(total_pages=68,free_pages=62,available_pages=62,global_free_pages=62)
            config=root/'fresh-matrix.json'
            command=['matrix','init','--config',str(config),'--node','a1',
                     '--topology',str(root/'approved.json'),'--skip-apt']
            with patch.object(Path,'read_text',new=read),patch.object(lab,'prepare',side_effect=build), \
                 patch.object(lab.Runner,'call',autospec=True,side_effect=call), \
                 patch.object(sys,'argv',command),contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(matrix.main(),0)
            spec=json.loads(config.read_text()); work=Path(spec['preparation_dir'])
            report=json.loads((work/'readiness.json').read_text())
            self.assertEqual(report['status'],'INPUTS_CHECKED_NOT_STARTED')
            self.assertEqual(report['hugepage_auto_provision']['status'],'PROVISIONED')
            self.assertEqual(len(commands),2)
            self.assertFalse(report['services_started']); self.assertFalse(report['devices_mapped'])

    def test_auto_hugepages_skips_native_ready_reused_and_independently_blocked_cases(self):
        from callosum_lab import Runner
        for case in ('native','already_free','reused','permission','other_blocker'):
            with self.subTest(case=case),tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
                root=Path(directory); path,args,state,extra,reused,needed=self.auto_hugepage_fixture(root,stack)
                runner=Runner(root/'work')
                if case=='native': needed[0]=0
                if case=='already_free': state['available_pages']=62
                if case=='reused': reused[0]=True
                if case=='permission': extra.append(dict(check='hugetlb_permission',error='fixture permission denied'))
                if case=='other_blocker': extra.append(dict(check='dax_access',error='fixture device access denied'))
                try:
                    with patch.object(runner,'call',side_effect=AssertionError('must not request sudo')) as calls, \
                         contextlib.redirect_stdout(io.StringIO()):
                        report=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                    calls.assert_not_called()
                    self.assertFalse(getattr(runner,'hugepage_pool_change_possible',False))
                    self.assertEqual(report['status'],'BLOCKED' if extra else 'INPUTS_CHECKED_NOT_STARTED')
                finally: runner.log.close()

    def test_auto_hugepages_auth_denial_does_not_claim_a_host_change_or_fallback(self):
        from callosum_lab import Runner
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args,state,extra,reused,needed=self.auto_hugepage_fixture(root,stack)
            runner=Runner(root/'work')
            try:
                with patch.object(runner,'call',side_effect=RuntimeError('sudo authorization denied')) as calls, \
                     contextlib.redirect_stdout(io.StringIO()):
                    report=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                self.assertEqual(calls.call_count,1)
                self.assertEqual(report['status'],'BLOCKED')
                self.assertEqual({i['check'] for i in report['issues']},{'hugepages_capacity','hugepages_provision'})
                self.assertFalse(getattr(runner,'hugepage_pool_change_possible',False))
                self.assertFalse((root/'work/hugepage-provision.json').exists())
                self.assertFalse(report['hugepage_auto_provision']['change_possible'])
            finally: runner.log.close()

    def test_auto_hugepages_partial_allocation_retains_evidence_and_retry_adds_no_duplicate_pages(self):
        from callosum_lab import Runner
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args,state,extra,reused,needed=self.auto_hugepage_fixture(root,stack)
            args.action='prepare'; runner=Runner(root/'work'); writes=[]
            def command(stage,argv,*a,**kw):
                if argv==['sudo','-v']: return
                writes.append(int(kw['input_text']))
                total=20 if len(writes)==1 else 68
                state.update(total_pages=total,free_pages=total-6,available_pages=total-6,global_free_pages=total-6)
            try:
                with patch.object(runner,'call',side_effect=command),contextlib.redirect_stdout(io.StringIO()):
                    first=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                    partial=json.loads((root/'work/hugepage-provision.json').read_text())
                    second=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                self.assertEqual(first['status'],'BLOCKED')
                self.assertEqual(partial['status'],'PARTIAL')
                self.assertTrue(first['hugepage_auto_provision']['change_possible'])
                self.assertEqual(second['status'],'INPUTS_CHECKED_NOT_STARTED')
                self.assertEqual(writes,[68,68])
                self.assertTrue(list((root/'work').glob('hugepage-provision.json.*.previous')))
            finally: runner.log.close()

    def test_auto_hugepages_failed_privileged_write_records_possible_partial_change(self):
        from callosum_lab import Runner
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args,state,extra,reused,needed=self.auto_hugepage_fixture(root,stack)
            runner=Runner(root/'work')
            def command(stage,argv,*a,**kw):
                if argv==['sudo','-v']: return
                state.update(total_pages=20,free_pages=14,available_pages=14,global_free_pages=14)
                raise RuntimeError('fixture tee failed after possible partial change')
            try:
                with patch.object(runner,'call',side_effect=command),contextlib.redirect_stdout(io.StringIO()):
                    report=prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                record=json.loads((root/'work/hugepage-provision.json').read_text())
                self.assertEqual(report['status'],'BLOCKED')
                self.assertEqual(record['status'],'WRITE_FAILED')
                self.assertTrue(record['change_possible'])
                self.assertEqual(record['after']['total_pages'],20)
                self.assertTrue(runner.hugepage_pool_change_possible)
                self.assertEqual(state['total_pages'],20)  # No rollback or shrink.
            finally: runner.log.close()

    def test_auto_hugepages_rejects_unexpected_privileged_control_path(self):
        from callosum_lab import Runner
        with tempfile.TemporaryDirectory() as directory,contextlib.ExitStack() as stack:
            root=Path(directory); path,args,state,extra,reused,needed=self.auto_hugepage_fixture(root,stack)
            state['control_file']='/proc/sys/vm/hugetlb_shm_group'
            runner=Runner(root/'work')
            try:
                with patch.object(runner,'call') as calls:
                    with self.assertRaisesRegex(RuntimeError,'unexpected HugeTLB control file'):
                        prep.inspect_resources(path,args,root/'work',matrix.load,runner)
                calls.assert_not_called()
            finally: runner.log.close()

    def test_function_numa_provision_delta_partial_failure_and_idempotence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); control=root/'nr_hugepages'; control.write_text('10')
            before=dict(total_pages=10,available_pages=0,free_pages=0,control_file=str(control))
            partial=dict(before,total_pages=50,available_pages=40)
            with patch.object(prep,'huge_snapshot',side_effect=[before,partial]):
                with self.assertRaisesRegex(RuntimeError,'partial NUMA'):
                    prep.provision(1,62,root/'work')
            self.assertEqual(control.read_text(),'72')
            record=json.loads((root/'work/hugepage-provision.json').read_text())
            self.assertEqual(record['status'],'PARTIAL'); self.assertFalse(record['reclaimed'])
            ready=dict(before,total_pages=72,available_pages=62)
            control.write_text('72')
            with patch.object(prep,'huge_snapshot',return_value=ready):
                self.assertFalse(prep.provision(1,62,root/'work')['changed'])
            self.assertEqual(control.read_text(),'72')

    def test_failure_unknown_journal_never_supplies_drain_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'master.sqlite'
            db=sqlite3.connect(path)
            db.executescript('CREATE TABLE operations(id,state); CREATE TABLE members(id,descriptor);')
            db.execute('INSERT INTO operations VALUES (?,?)',('attempt','QUARANTINED')); db.commit(); db.close()
            with self.assertRaisesRegex(RuntimeError,'keep MR owners'): prep.frozen_journal(path)

    def test_status_peer_signature_and_failed_preparation_cannot_authorize_run(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'peer.json'; secret='private-fixture-token-'*3
            path.write_text(json.dumps(prep.signed(dict(status='BLOCKED'),secret)))
            args=SimpleNamespace(node='a1')
            with self.assertRaisesRegex(RuntimeError,'not prepared'):
                prep.verify_peer(path,secret,{}, {},args)
            path.write_text(json.dumps(dict(status='PREPARED_NOT_TESTED',signature='forged')))
            with self.assertRaisesRegex(RuntimeError,'signature differs'): prep.read_proof(path,secret)

    def test_first_init_preserves_topology_token_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); path,args=matrix_tests.MatrixTest().fixture(root)
            topology=root/'topology.json'; private=root/'control.token'
            before=(topology.read_bytes(),private.read_bytes())
            args.topology=str(topology); args.setup_run=args.peer_setup_run=str(root)
            args.token_file=str(private); args.source='b1'; args.receiver='a1'
            target=root/'new'/'benchmark.local.json'
            result=prep.initialize(target,args)
            self.assertEqual(result['status'],'CONFIG_CREATED_NOT_VALIDATED')
            self.assertEqual(matrix.load(target,args)[0]['pair'],dict(source='b1',receiver='a1'))
            with self.assertRaisesRegex(RuntimeError,'preserve'):
                prep.initialize(target,args)
            self.assertEqual((topology.read_bytes(),private.read_bytes()),before)

    def retirement_fixture(self,root):
        path,args=matrix_tests.MatrixTest().fixture(root)
        old=root/'old'; old.mkdir(); (old/'state').mkdir()
        old.joinpath('topology.json').write_bytes((root/'topology.json').read_bytes())
        old.joinpath('control.token').write_bytes((root/'control.token').read_bytes())
        topology=prep.config(old/'topology.json')
        topology['lab_run']=dict(controller='a1',source='b1',requester='a1',profile='two_worker_smoke')
        topology.pop('fingerprint'); (old/'topology.json').write_text(json.dumps(topology))
        topology=prep.config(old/'topology.json')
        for filename,value in [('authorization.json',dict(confirmed=True,config_sha256=prep.digest(old/'topology.json'))),
            ('summary.json',dict(status='PASS',config_sha256=prep.digest(old/'topology.json'))),
            ('setup.json',dict(source_hashes=prep.software()))]:
            (old/filename).write_text(json.dumps(value))
        descriptor=dict(id='a1',epoch='old-a1',abi_version=2)
        db=sqlite3.connect(old/'state/master.sqlite')
        db.executescript('CREATE TABLE operations(id,state); CREATE TABLE members(id,descriptor);')
        db.execute('INSERT INTO operations VALUES (?,?)',('attempt','COMMITTED'))
        for n in ('a1','b1'):
            db.execute('INSERT INTO members VALUES (?,?)',(n,json.dumps(dict(descriptor,id=n,epoch='old-'+n))))
        db.commit(); db.close()
        for name,pid in [('master',11),('a1',12)]:
            (old/f'{name}.process.json').write_text(json.dumps(dict(pid=pid,stamp=str(pid))))
        return old,topology,(old/'control.token').read_text().strip(),descriptor

    def test_retirement_seals_control_before_agents_and_resumes_positive_approval(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); old,c,secret,descriptor=self.retirement_fixture(root)
            work=root/'work'; events=[]; states={11:'11',12:'12'}
            def rpc(url,method,**kwargs):
                events.append(method)
                if method=='status': return dict(fingerprint=c['fingerprint'],operations=[dict(state='COMMITTED')])
                if method=='health': return descriptor
                if method=='dispatch_status': return {'attempt':'DRAINED'}
                self.fail(method)
            def stop(marker):
                pid=json.loads(marker.read_text())['pid']; events.append('stop-'+str(pid))
                states[pid]=None
                if pid==12: raise RuntimeError('simulated disconnect after positive approval and exit')
                return dict(pid=pid,stamp=str(pid))
            with patch.object(prep,'rpc',side_effect=rpc),patch.object(prep,'signal_owned',side_effect=stop), \
                 patch.object(prep,'process_stamp',side_effect=lambda pid:states.get(pid)):
                with self.assertRaisesRegex(RuntimeError,'simulated disconnect'):
                    prep.retire(old,'a1',secret,work,exclusive=True)
                self.assertLess(events.index('stop-11'),events.index('health'))
                self.assertTrue((work/'agent-drain-approved.json').exists())
                result=prep.retire(old,'a1',secret,work,exclusive=True)
                self.assertEqual(result['status'],'RETIRED_NOT_RECLAIMED')
                before=list(events)
                self.assertEqual(prep.retire(old,'a1',secret,work,exclusive=True),result)
                self.assertEqual(events,before)
                self.assertTrue((old/'state/master.sqlite').exists())

    def test_retirement_busy_campaign_stops_no_process(self):
        with tempfile.TemporaryDirectory() as directory:
            old,c,secret,_=self.retirement_fixture(Path(directory))
            with patch.object(prep,'rpc',return_value=dict(fingerprint=c['fingerprint'],operations=[dict(state='QUARANTINED')])), \
                 patch.object(prep,'signal_owned') as stop:
                with self.assertRaisesRegex(RuntimeError,'busy/unknown'):
                    prep.retire(old,'a1',secret,Path(directory)/'work',exclusive=True)
                stop.assert_not_called()

    def test_adopt_checks_native_contract_and_records_real_origin_without_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); old=root/'old'; old.mkdir()
            source=Path(prep.__file__).resolve().parents[1]
            receipt=dict(status='PASS',build=str(root/'existing-build'),
                source_hashes={f:prep.digest(source/f) for f in prep.SOURCE_FILES})
            (old/'setup.json').write_text(json.dumps(receipt))
            with patch.object(prep.subprocess,'check_output',return_value='{"existing-engine.so":"actual-hash"}') as probe:
                adopted=prep.adopt_build(source,old,root/'new',dict(PYTHON_BIN='selected-python'))
                self.assertEqual(adopted['adoption_scope'],'VERIFIED_EXISTING_ABI2_NOT_REBUILT')
                self.assertEqual(adopted['adopted_modules'],{'existing-engine.so':'actual-hash'})
                self.assertNotIn('initialize(',probe.call_args.args[0][-1])
            receipt['source_hashes']['mooncake-integration/pool_relay/relay_batch.h']='different-native-contract'
            (old/'setup.json').write_text(json.dumps(receipt))
            with patch.object(prep.subprocess,'check_output') as probe:
                with self.assertRaisesRegex(RuntimeError,'native contract source differs'):
                    prep.adopt_build(source,old,root/'not-adopted',{})
                probe.assert_not_called()
                self.assertFalse((root/'not-adopted').exists())


if __name__=='__main__': unittest.main()
