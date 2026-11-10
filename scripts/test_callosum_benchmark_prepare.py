"""First-run and recovery fixtures. No DAX, HugeTLB or RDMA hardware acceptance."""
import contextlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import callosum_benchmark_prepare as prep
import callosum_write_matrix as matrix
import test_callosum_write_matrix as matrix_tests


class PreparationTest(unittest.TestCase):
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
