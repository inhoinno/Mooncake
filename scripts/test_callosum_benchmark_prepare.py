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
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import callosum_benchmark_prepare as prep
import callosum_write_matrix as matrix
import test_callosum_write_matrix as matrix_tests


class PreparationTest(unittest.TestCase):
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
