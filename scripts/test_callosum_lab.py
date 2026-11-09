"""Portable orchestration tests. These are not glog/RDMA hardware validation."""
import base64
import hashlib
import io
import json
import os
import shlex
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import callosum_lab as lab


class LabTest(unittest.TestCase):
    def test_private_configuration_pair_is_exclusive_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            config,token=root/'lab.json',root/'control.token'
            secret='fixture-shared-random-token-'*2
            data=self.node_manifest()
            result=lab.install_configuration(str(config),str(token),data,secret)
            self.assertEqual(result['status'],'CREATED')
            self.assertEqual(config.stat().st_mode & 0o777,0o600)
            self.assertEqual(token.stat().st_mode & 0o777,0o600)
            before=(config.read_bytes(),token.read_bytes())
            self.assertEqual(lab.install_configuration(str(config),str(token),data,secret)['status'],'EXISTS_MATCHED')
            with self.assertRaisesRegex(RuntimeError,'conflict'):
                lab.install_configuration(str(config),str(token),dict(data,master='changed'),secret)
            self.assertEqual(before,(config.read_bytes(),token.read_bytes()))

    def test_partial_or_insecure_configuration_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            config,token=root/'lab.json',root/'control.token'
            data,secret=self.node_manifest(),'fixture-private-shared-token-'*2
            config.write_text(json.dumps(data))
            with self.assertRaisesRegex(RuntimeError,'Partial'):
                lab.install_configuration(str(config),str(token),data,secret)
            self.assertFalse(token.exists())
            token.write_text(secret)
            token.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError,'conflict'):
                lab.install_configuration(str(config),str(token),data,secret)
            self.assertEqual(token.stat().st_mode & 0o777,0o644)

    def exercise_initializer(self,authorize=True,conflict=False):
        # SSH is a fake boundary here. Execute the actual peer installer with
        # private stdin against a second temporary checkout, never a lab host.
        with tempfile.TemporaryDirectory() as directory,patch('sys.stdout',new=io.StringIO()):
            base=Path(directory)
            root,peer_root=base/'local checkout',base/'peer checkout'
            root.mkdir()
            peer_root.mkdir()
            template=root/'experiments/two_worker_metadata_lookup/lab.example.json'
            template.parent.mkdir(parents=True)
            data=self.node_manifest()
            data['nodes'][0]['id']='consumer-17'
            data['nodes'][1]['id']='producer-91'
            data['lab_run'].update(controller='consumer-17',requester='consumer-17',source='producer-91')
            template.write_text(json.dumps(data))
            run=root/'evidence'
            runner=lab.Runner(run)
            peer_inventory=dict(self.inventoried_host(),host='fixture-peer')
            original_call=runner.call
            stages=[]
            def call(stage,command,env=None,capture=False,input_text=None):
                stages.append(stage)
                if stage=='peer-read-only-inventory':
                    return json.dumps(peer_inventory)
                self.assertEqual(stage,'peer-private-config-install')
                remote=shlex.split(command[-1])
                self.assertEqual(remote[0],'python3')
                return original_call(stage,[sys.executable]+remote[1:],env,capture,input_text)
            runner.call=call
            args=SimpleNamespace(config=str(root/'lab.json'),token_file=str(root/'control.token'),
                node_id='consumer-17',peer='operator@fixture-peer',remote_root=str(peer_root))
            fields={}
            for node in data['nodes']:
                ip=lab.urlparse(node['url']).hostname
                fields[node['id']]=dict(ip=ip,engine_ip=ip,path=node['path'],hca=node['rdma_device'],
                                       pool=data['pools'][node['pool']])
            if conflict:
                (peer_root/'lab.json').write_text('{"preserve":"existing-config"}')
                (peer_root/'control.token').write_text('preserve-existing-token-'*2)
                (peer_root/'control.token').chmod(0o600)
            try:
                with patch.object(lab.sys,'platform','linux'), \
                        patch.object(lab,'inventory',return_value=dict(self.inventoried_host(),host='fixture-local')), \
                        patch.object(lab,'host_fields',side_effect=lambda label,inv:fields[label]), \
                        patch.object(lab,'ask',return_value='INITIALIZE CONFIG' if authorize else 'CANCEL'), \
                        patch.object(lab,'prepare') as prepare,patch.object(lab,'start_service') as start:
                    if not authorize or conflict:
                        with self.assertRaises(RuntimeError):
                            lab.initialize_configuration(root,run,args,runner)
                    else:
                        lab.initialize_configuration(root,run,args,runner)
                        secret=(root/'control.token').read_text()
                        self.assertEqual(secret,(peer_root/'control.token').read_text())
                        self.assertEqual(json.loads((root/'lab.json').read_text()),
                                         json.loads((peer_root/'lab.json').read_text()))
                        self.assertEqual(json.loads((root/'lab.json').read_text())['lab_run']['controller'],'consumer-17')
                        self.assertNotIn(secret.strip(),(run/'commands.log').read_text())
                        self.assertEqual((peer_root/'control.token').stat().st_mode & 0o777,0o600)
                    prepare.assert_not_called()
                    start.assert_not_called()
                if not authorize:
                    self.assertFalse((root/'lab.json').exists())
                    self.assertFalse((peer_root/'lab.json').exists())
                if conflict:
                    self.assertEqual((peer_root/'lab.json').read_text(),'{"preserve":"existing-config"}')
                    self.assertEqual((peer_root/'control.token').read_text(),'preserve-existing-token-'*2)
                return stages
            finally:
                runner.log.close()

    def test_initializer_collects_data_and_installs_identical_private_pair(self):
        self.assertEqual(self.exercise_initializer(),['peer-read-only-inventory','peer-private-config-install'])

    def test_initializer_cancellation_does_not_create_config_or_start_services(self):
        self.assertEqual(self.exercise_initializer(authorize=False),['peer-read-only-inventory'])

    def test_initializer_preserves_conflicting_peer_configuration(self):
        self.assertEqual(self.exercise_initializer(conflict=True),['peer-read-only-inventory','peer-private-config-install'])

    def test_missing_manifest_reports_initialization_not_transport_failure(self):
        with tempfile.TemporaryDirectory() as directory,patch('sys.stdout',new=io.StringIO()):
            root=Path(directory)
            (root/'Mooncake-dev/scripts').mkdir(parents=True)
            (root/'Mooncake-dev/scripts/callosum_runtime.py').write_text('# fixture')
            for node in ('a1','b1'):
                run=root/('evidence-'+node)
                with patch.object(lab.sys,'platform','linux'), \
                        patch.dict(os.environ,{'CALLOSUM_DESTRUCTIVE_ACK':'1'}), \
                        patch.object(lab,'prepare') as prepare,patch.object(lab,'start_service') as start:
                    code=lab.main(['--root',str(root),'--run-dir',str(run),'--action','node',
                        '--node-id',node,'--config',str(root/'absent.json'),'--token-file',str(root/'absent.token')])
                    self.assertEqual(code,1)
                    failure=json.loads((run/'failure.json').read_text())
                    self.assertEqual(failure['stage'],'configuration-validation')
                    self.assertIn('--initialize',failure['error'])
                    self.assertFalse(failure['local_services_started_this_run'])
                    prepare.assert_not_called()
                    start.assert_not_called()

    def test_canonical_entrypoint_supplies_default_shared_file_paths(self):
        root=Path(lab.__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            stub=Path(directory)/'python-stub'
            stub.write_text('#!'+sys.executable+'\nimport json,sys; print(json.dumps(sys.argv[1:]))\n')
            stub.chmod(0o755)
            env=dict(lab.clean_env(),PYTHON_BIN=str(stub),NODE_ID='arbitrary-id')
            env.pop('CONFIG',None)
            env.pop('TOKEN_FILE',None)
            env.pop('CALLOSUM_ROOT',None)
            result=subprocess.run(['bash',str(root/'scripts/callosum_lab.sh'),'--initialize'],
                                  env=env,cwd=directory,text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            argv=json.loads(result.stdout)
            self.assertEqual(Path(argv[argv.index('--config')+1]).resolve(),root/'lab.json')
            self.assertEqual(Path(argv[argv.index('--token-file')+1]).resolve(),root/'control.token')
            self.assertEqual(argv[argv.index('--node-id')+1],'arbitrary-id')
            self.assertIn('--initialize',argv)

    def test_initialize_flag_precedes_normal_node_workflow(self):
        with tempfile.TemporaryDirectory() as directory,patch('sys.stdout',new=io.StringIO()):
            root=Path(directory)
            (root/'Mooncake-dev/scripts').mkdir(parents=True)
            (root/'Mooncake-dev/scripts/callosum_runtime.py').write_text('# fixture')
            order=[]
            with patch.object(lab,'initialize_configuration',side_effect=lambda *args:order.append('initialize')), \
                    patch.object(lab,'node_run',side_effect=lambda *args:order.append('run')):
                self.assertEqual(lab.main(['--root',str(root),'--run-dir',str(root/'evidence'),
                    '--action','node','--node-id','some-controller','--initialize']),0)
            self.assertEqual(order,['initialize','run'])

    def test_embedded_patch_is_complete(self):
        payload=base64.b64decode(lab.PATCH_B64)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), lab.PATCH_SHA256)
        self.assertTrue(payload.endswith(b'\n'))
        self.assertEqual(payload.count(b'diff --git '),5)

    def test_embedded_patch_applies_to_base_and_is_idempotent(self):
        source=Path(lab.__file__).resolve().parent.parent
        files=('mooncake-integration/pool_relay/CMakeLists.txt','scripts/callosum_initiation.py',
               'scripts/run_callosum_relay.sh','scripts/test_callosum_initiation.py',
               'scripts/test_callosum_runtime.py','scripts/callosum_runtime.py')
        with tempfile.TemporaryDirectory() as directory,patch('sys.stdout',new=io.StringIO()):
            root=Path(directory)
            checkout=root/'checkout'
            checkout.mkdir()
            for file in files:
                data=subprocess.check_output(['git','-C',str(source),'show','996d08c0:'+file])
                target=checkout/file
                target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes(data)
            runner=lab.Runner(root/'evidence')
            try:
                lab.apply_remaining_patch(checkout,root/'evidence',runner)
                expected={p:lab.digest(checkout/p) for p in files}
                lab.apply_remaining_patch(checkout,root/'evidence',runner)
                self.assertEqual(expected,{p:lab.digest(checkout/p) for p in files})
                self.assertIn("entry['error'] = None",(checkout/'scripts/callosum_initiation.py').read_text())
                self.assertIn("if method == 'payload_trace':",(checkout/'scripts/callosum_runtime.py').read_text())
            finally:
                runner.log.close()

    def test_environment_excludes_stale_libraries_and_headers(self):
        with patch.dict(os.environ, {'LD_PRELOAD':'old.so','PYTHONPATH':'old',
                'LD_LIBRARY_PATH':'old','CPATH':'old','CPLUS_INCLUDE_PATH':'old',
                'CALLOSUM_CONTROL_TOKEN':'do-not-print-this',
                'MC_CXL_DEV_PATH':'/dev/unapproved','CALLOSUM_DESTRUCTIVE_ACK':'1'}):
            env=lab.clean_env()
        for key in ('LD_PRELOAD','PYTHONPATH','LD_LIBRARY_PATH','CPATH','CPLUS_INCLUDE_PATH'):
            self.assertNotIn(key,env)
        for key in ('CALLOSUM_CONTROL_TOKEN','MC_CXL_DEV_PATH','CALLOSUM_DESTRUCTIVE_ACK'):
            self.assertNotIn(key,env)
        self.assertEqual(env['CTEST_PARALLEL_LEVEL'],'1')

    def test_engine_rejects_wrong_or_unresolved_shared_libraries(self):
        with tempfile.TemporaryDirectory() as directory:
            library=Path(directory)/'libglog.so.1'
            library.write_bytes(b'fixture-library')
            lab.check_glog_dependencies(f'libglog.so.1 => {library} (0x1234)',library)
            with self.assertRaisesRegex(RuntimeError,'selected package-owned'):
                lab.check_glog_dependencies('libglog.so.0 => /old/libglog.so.0 (0x1234)',library)
            with self.assertRaisesRegex(RuntimeError,'unresolved'):
                lab.check_glog_dependencies(f'libglog.so.1 => {library} (0x1234)\nlibother.so => not found',library)
            with self.assertRaises(RuntimeError):
                lab.check_glog_dependencies('libglog.so.1 => not found',library)

    def test_command_failure_preserves_stage_and_log(self):
        with tempfile.TemporaryDirectory() as directory, patch('sys.stdout',new=io.StringIO()):
            runner=lab.Runner(directory)
            try:
                with self.assertRaisesRegex(RuntimeError,'exit=7'):
                    runner.call('compiler', [sys.executable,'-c','print("compiler evidence");raise SystemExit(7)'])
                self.assertEqual(runner.stage,'compiler')
            finally:
                runner.log.close()
            self.assertIn('compiler evidence',(Path(directory)/'commands.log').read_text())

    def test_capture_returns_json_without_mixing_command_banner(self):
        with tempfile.TemporaryDirectory() as directory, patch('sys.stdout',new=io.StringIO()):
            runner=lab.Runner(directory)
            try:
                value=runner.call('remote-info',[sys.executable,'-c','print("{\\"ok\\": true}")'],capture=True)
                self.assertEqual(json.loads(value),{'ok':True})
            finally:
                runner.log.close()

    def test_ssh_quoting_and_option_rejection(self):
        command=lab.remote_command('labuser@192.168.5.44',['python3','/safe/script.py','--root','a b;echo x'])
        self.assertEqual(command[:2],['ssh','labuser@192.168.5.44'])
        self.assertIn("'a b;echo x'",command[-1])
        for peer in ('-oProxyCommand=bad','host other','host;command',''):
            with self.assertRaises(RuntimeError):
                lab.remote_command(peer,['true'])

    @staticmethod
    def inventoried_host():
        return dict(devices=['/dev/dax0.0'],hcas=['mlx5_0'],
                    geometry={'/dev/dax0.0':dict(size=8*1024**2,align=2*1024**2,mode='devdax')},
                    network=dict(status='PASS',output='fixture UP 192.0.2.2/24 192.0.2.3/24 192.0.2.6/24 192.0.2.7/24'))

    def test_manifest_separates_control_and_engine_addresses(self):
        answers=['192.168.3.42','192.168.5.42','','','0','4194304','labuser','physical-M2']
        with patch('builtins.input',side_effect=answers),patch('sys.stdout',new=io.StringIO()):
            a=lab.host_fields('A',self.inventoried_host())
        b=dict(a,ip='192.168.3.46',engine_ip='192.168.5.46',pool=dict(a['pool'],physical_pool_id='physical-M6'))
        c=lab.manifest(a,b)
        self.assertEqual(c['master'],'http://192.168.3.42:19100')
        self.assertEqual(c['nodes'][0]['te_endpoint'],'192.168.5.42:19301')
        self.assertEqual(c['mode'],'rdma')
        self.assertEqual(c['path_mode'],'direct_dax_write')
        with self.assertRaisesRegex(RuntimeError,'independent'):
            lab.manifest(a,dict(b,pool=a['pool']))

    def test_extent_geometry_rejects_overflow_and_system_ram(self):
        answers=['192.168.3.42','','','','6291456','4194304','owner','device']
        with patch('builtins.input',side_effect=answers),patch('sys.stdout',new=io.StringIO()):
            with self.assertRaisesRegex(RuntimeError,'exceeds'):
                lab.host_fields('A',self.inventoried_host())
        bad=self.inventoried_host()
        bad['geometry']['/dev/dax0.0']['mode']='system-ram'
        with patch('builtins.input',side_effect=['192.168.3.42','','']),patch('sys.stdout',new=io.StringIO()):
            with self.assertRaisesRegex(RuntimeError,'already be devdax'):
                lab.host_fields('A',bad)

    @staticmethod
    def node_manifest():
        a=dict(ip='192.0.2.2',engine_ip='192.0.2.2',path='/dev/dax0.0',hca='mlx5_0',
               pool=dict(capacity=4194304,map_offset=0,mapping_alignment=2097152,
                         owner='test-fixture',physical_pool_id='fixture-A'))
        b=dict(a,ip='192.0.2.6',engine_ip='192.0.2.6',pool=dict(a['pool'],physical_pool_id='fixture-B'))
        config=lab.manifest(a,b)
        config['lab_run']=dict(controller='a1',profile='two_worker_smoke',source='b1',requester='a1')
        return config

    def test_node_selection_checks_actual_local_extent(self):
        config=self.node_manifest()
        self.assertEqual(lab.validate_node_extent(config,'a1',self.inventoried_host())['id'],'a1')
        self.assertEqual(lab.validate_node_extent(config,'b1',self.inventoried_host())['id'],'b1')
        config['pools']['B']['map_offset']=6291456
        with self.assertRaisesRegex(RuntimeError,'geometry'):
            lab.validate_node_extent(config,'b1',self.inventoried_host())
        config=self.node_manifest()
        config['pools']['B']['physical_pool_id']='fixture-A'
        with self.assertRaisesRegex(RuntimeError,'Independent'):
            lab.validate_node_extent(config,'a1',self.inventoried_host())
        with self.assertRaisesRegex(RuntimeError,'NODE_ID'):
            lab.validate_node_extent(self.node_manifest(),'unknown',self.inventoried_host())

    def test_node_without_explicit_write_ack_does_not_prepare_or_start(self):
        with patch.object(lab.sys,'platform','linux'),patch.dict(os.environ,{},clear=True), \
                patch.object(lab,'prepare') as prepare,patch.object(lab,'start_service') as start:
            with self.assertRaisesRegex(RuntimeError,'CALLOSUM_DESTRUCTIVE_ACK'):
                lab.node_run(Path('/fixture'),Path('/fixture'),
                    SimpleNamespace(node_id='a1',config='/fixture.json',token_file='/fixture.token'),Mock())
            prepare.assert_not_called()
            start.assert_not_called()

    def exercise_node_fixture(self,node_id,configured=None):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'Mooncake-dev/scripts').mkdir(parents=True)
            (root/'Mooncake-dev/scripts/callosum_runtime.py').write_text('# control-client fixture')
            run=root/'run'
            run.mkdir()
            (run/'setup.json').write_text(json.dumps(dict(status='PASS',env={},source_hashes={})))
            config=root/'approved.json'
            configured=configured or self.node_manifest()
            config.write_text(json.dumps(configured))
            token=root/'private.token'
            token.write_text('fixture-only-token-not-a-real-secret')
            token.chmod(0o600)
            args=SimpleNamespace(node_id=node_id,config=str(config),token_file=str(token),python=sys.executable)
            status=dict(topology_verified=True,operations=[dict(state='COMMITTED')]*2)
            def rpc(url,method,*args,**kwargs):
                if method=='health':
                    return dict(id=next(n['id'] for n in configured['nodes'] if n['url']==url))
                return {'status':status,'register':dict(registered=True),
                        'dispatch_status':{'fixture-transfer':'DRAINED'}}[method]
            runtime=SimpleNamespace(rpc=Mock(side_effect=rpc),config=Mock())
            runner=Mock()
            with patch.object(lab.sys,'platform','linux'), \
                    patch.dict(os.environ,{'CALLOSUM_DESTRUCTIVE_ACK':'1'}), \
                    patch.object(lab,'inventory',return_value=self.inventoried_host()), \
                    patch.object(lab,'start_service') as start, \
                    patch.object(lab,'validate_node_traces',return_value=[dict(status='PASS',fixture=True)]), \
                    patch.object(lab.importlib,'import_module',return_value=runtime):
                lab.node_run(root,run,args,runner)
            summary=json.loads((run/'summary.json').read_text()) if (run/'summary.json').exists() else None
            return start.call_args_list,[c.args[0] for c in runner.call.call_args_list],summary

    def test_configured_controller_owns_master_and_tests_both_directions(self):
        starts,stages,summary=self.exercise_node_fixture('a1')
        self.assertEqual([(c.args[2],c.args[3]) for c in starts],[('master',None),('agent','a1')])
        self.assertEqual(stages,['host-preflight','effective-config','authorized-topology-preflight','forward','reverse'])
        self.assertEqual(summary['status'],'PASS')  # Fixture; not real RDMA evidence.
        self.assertFalse(summary['serving_proven'])

    def test_member_starts_only_its_configured_agent(self):
        starts,stages,summary=self.exercise_node_fixture('b1')
        self.assertEqual([(c.args[2],c.args[3]) for c in starts],[('agent','b1')])
        self.assertEqual(stages,['host-preflight','effective-config'])
        self.assertIsNone(summary)

    def test_controller_and_ids_change_by_data_without_host_branches(self):
        config=self.node_manifest()
        config['nodes'][0]['id']='member-17'
        config['nodes'][1]['id']='member-91'
        config['master']='http://192.0.2.6:19100'
        config['lab_run'].update(controller='member-91',source='member-17',requester='member-91')
        starts,stages,summary=self.exercise_node_fixture('member-91',config)
        self.assertEqual([(c.args[2],c.args[3]) for c in starts],
                         [('master',None),('agent','member-91')])
        self.assertEqual(stages[-2:],['forward','reverse'])
        self.assertEqual(summary['scope'],'TRANSFER_ONLY')

    def test_four_node_services_profile_needs_only_more_records(self):
        config=self.node_manifest()
        config['lab_run']['profile']='services_only'
        for original,new_id,ip in ((config['nodes'][0],'extra-123','192.0.2.3'),
                                   (config['nodes'][1],'extra-987','192.0.2.7')):
            config['nodes'].append(dict(original,id=new_id,url=f'http://{ip}:19101',
                                        te_endpoint=f'{ip}:19301'))
        selected,launch=lab.validate_launch_config(config,'extra-987')
        self.assertEqual(selected['pool'],'B')
        self.assertEqual(launch['controller'],'a1')
        starts,stages,summary=self.exercise_node_fixture('extra-987',config)
        self.assertEqual([(c.args[2],c.args[3]) for c in starts],[('agent','extra-987')])
        self.assertIsNone(summary)  # Services only is not a four-worker transfer PASS.

    def test_launch_settings_fail_closed_on_types_and_bounds(self):
        mutations=[
            lambda c:c.update(nodes=None),
            lambda c:c['nodes'][0].update(id='../other'),
            lambda c:c['pools']['A'].update(capacity='4194304'),
            lambda c:c['pools']['A'].update(map_offset=True),
            lambda c:c['pools']['B'].update(owner=''),
            lambda c:c['lab_run'].update(wait_seconds=True),
            lambda c:c['lab_run'].update(profile='invented'),
            lambda c:c['lab_run'].update(controller='missing'),
            lambda c:c['lab_run'].update(requester='b1'),
            lambda c:c.update(max_inflight_bytes=False),
            lambda c:c.update(master='http://192.0.2.6:19100'),
        ]
        for mutate in mutations:
            config=self.node_manifest()
            mutate(config)
            with self.subTest(config=config):
                with self.assertRaises(RuntimeError):
                    lab.validate_launch_config(config,'a1')

    def test_invalid_launch_config_fails_before_prepare_or_start(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            config=root/'config.json'
            invalid=self.node_manifest()
            invalid['lab_run']['controller']='missing'
            config.write_text(json.dumps(invalid))
            args=SimpleNamespace(node_id='a1',config=str(config),token_file=str(root/'absent'))
            with patch.object(lab.sys,'platform','linux'), \
                    patch.dict(os.environ,{'CALLOSUM_DESTRUCTIVE_ACK':'1'}), \
                    patch.object(lab,'prepare') as prepare,patch.object(lab,'start_service') as start:
                runner=Mock()
                with self.assertRaisesRegex(RuntimeError,'controller'):
                    lab.node_run(root,root,args,runner)
                self.assertEqual(runner.stage,'configuration-validation')
                prepare.assert_not_called()
                start.assert_not_called()

    def test_payload_trace_is_filtered_validated_and_bounded(self):
        from callosum_runtime import Log
        with tempfile.TemporaryDirectory() as directory:
            log=Log(Path(directory)/'agent.jsonl','endpoint-72')
            try:
                identity='a'*32
                log.emit('batch_complete',transfer_id=identity,bytes=4096,
                         token='secret',rkey=123,remote_address=456,key='private-key')
                log.emit('prepare',transfer_id=identity,bytes=4096)
                log.emit('chunk_complete',transfer_id='b'*32,bytes=4096)
                events=log.payload_trace(identity)
                self.assertEqual(len(events),1)
                self.assertEqual(events[0]['bytes'],4096)
                for key in ('token','rkey','remote_address','key'):
                    self.assertNotIn(key,events[0])
                for invalid in (None,42,'../file','a'*31,'A'*32):
                    with self.assertRaisesRegex(RuntimeError,'trace transfer ID'):
                        log.payload_trace(invalid)
                for _ in range(2048):
                    log.emit('chunk_complete',transfer_id=identity,bytes=1)
                with self.assertRaisesRegex(RuntimeError,'bounded smoke'):
                    log.payload_trace(identity)
            finally:
                log.file.close()

    def test_readiness_does_not_hide_authentication_failure(self):
        runtime=SimpleNamespace(rpc=Mock(side_effect=RuntimeError('unauthorized')))
        with patch.object(lab.time,'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError,'unauthorized'):
                lab.wait_rpc(runtime,'http://192.0.2.2:19100','status',1800,Mock())
            runtime.rpc.assert_called_once()
            sleep.assert_not_called()

    def test_trace_collection_uses_actual_validator_and_rejects_tcp(self):
        source_root=Path(lab.__file__).resolve().parents[2]
        config=self.node_manifest()
        for transport in ('rdma','tcp'):
            with tempfile.TemporaryDirectory() as directory:
                run=Path(directory)
                (run/'state').mkdir()
                all_events=[]
                payload={}
                for index,direction in enumerate(('forward','reverse')):
                    identity=str(index)*32
                    events=[dict(stage=stage,node='master',monotonic_ns=i,evidence_class='TRANSFER_ONLY',
                                 transfer_id=identity,bytes=4096)
                            for i,stage in enumerate(('request_begin','lookup','reserved','targets_ready',
                                'dispatch_begin','writes_complete','verified','published','handles_released'))]
                    all_events.extend(events)
                    sender='b1' if index==0 else 'a1'
                    common=dict(node=sender,sender=sender,transfer_id=identity,evidence_class='TRANSFER_ONLY',
                                bytes=4096,batch_id=0)
                    payload[identity]=[dict(common,stage='batch_complete',cardinality=1,opcode='WRITE',
                        actual_transport=transport,path_mode='direct_dax_write'),
                        dict(common,stage='chunk_complete',chunk=0,object_byte_start=0)]
                    (run/direction).mkdir()
                    (run/direction/'samples.jsonl').write_text(json.dumps(dict(status='PASS',
                        transfer_id=identity,bytes=4096,consume_rpc_ns=1000000,local_reuse_rpc_ns=500000))+'\n')
                (run/'state/master-None.jsonl').write_text('\n'.join(map(json.dumps,all_events))+'\n')
                def rpc(url,method,data):
                    self.assertEqual(method,'payload_trace')
                    events=payload[data['transfer_id']]
                    sender=events[0]['sender']
                    return events if url==next(n['url'] for n in config['nodes'] if n['id']==sender) else []
                runtime=SimpleNamespace(rpc=rpc)
                if transport=='rdma':
                    result=lab.validate_node_traces(source_root,run,config,runtime)
                    self.assertEqual(len(result),2)
                    self.assertTrue(all(r['status']=='PASS' for r in result))
                else:
                    with self.assertRaisesRegex(ValueError,'transport'):
                        lab.validate_node_traces(source_root,run,config,runtime)

    def test_node_shell_entrypoint_help_needs_no_manifest_or_ssh(self):
        wrapper=Path(lab.__file__).with_name('run_callosum_relay.sh')
        result=subprocess.run(['bash',str(wrapper),'lab','--help'],text=True,capture_output=True,
            env=dict(lab.clean_env(),PYTHON_BIN=sys.executable,NODE_ID='arbitrary-node'))
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('--node-id',result.stdout)

    def test_project_entrypoint_uses_canonical_launcher_outside_cwd(self):
        root=Path(lab.__file__).resolve().parents[2]
        entry=root/'scripts/callosum_lab.sh'
        with tempfile.TemporaryDirectory() as directory:
            result=subprocess.run(['bash',str(entry),'--help'],cwd=directory,
                text=True,capture_output=True,
                env=dict(lab.clean_env(),PYTHON_BIN=sys.executable,NODE_ID='arbitrary-node'))
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('--node-id',result.stdout)
        self.assertNotIn('--node-role',result.stdout)

    def test_missing_authorization_prevents_any_child_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'Mooncake-dev/scripts').mkdir(parents=True)
            (root/'Mooncake-dev/scripts/callosum_runtime.py').write_text('fixture')
            (root/'setup.json').write_text(json.dumps(dict(status='PASS',source_hashes={},env={})))
            (root/'control.token').write_text('fixture-token')
            with patch.object(subprocess,'Popen') as spawn:
                with self.assertRaises(FileNotFoundError):
                    lab.start_service(root,root,'agent','a1',type('Runner',(),{})())
                spawn.assert_not_called()

    def test_changed_authorization_prevents_any_child_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'Mooncake-dev/scripts').mkdir(parents=True)
            (root/'Mooncake-dev/scripts/callosum_runtime.py').write_text('fixture')
            (root/'setup.json').write_text(json.dumps(dict(status='PASS',source_hashes={},env={})))
            (root/'control.token').write_text('fixture-token')
            (root/'topology.json').write_text('{}')
            (root/'authorization.json').write_text(json.dumps(dict(confirmed=True,config_sha256='stale')))
            with patch.object(subprocess,'Popen') as spawn:
                with self.assertRaisesRegex(RuntimeError,'authorization'):
                    lab.start_service(root,root,'agent','a1',type('Runner',(),{})())
                spawn.assert_not_called()

    def test_stop_requires_matching_success_and_terminal_operations(self):
        summary=dict(status='PASS',config_sha256='matched')
        status=dict(operations=[dict(state='COMMITTED'),dict(state='COMMITTED')])
        lab.check_stop_gate(summary,status,'matched')
        for proof in ({},dict(status='PASS',config_sha256='old')):
            with self.assertRaisesRegex(RuntimeError,'successful smoke proof'):
                lab.check_stop_gate(proof,status,'matched')
        for states in ([],['COMMITTED'],['COMMITTED','QUARANTINED']):
            with self.assertRaisesRegex(RuntimeError,'refuse stop'):
                lab.check_stop_gate(summary,dict(operations=[dict(state=s) for s in states]),'matched')

    def test_stop_never_signals_a_reused_or_missing_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            run=Path(directory)
            (run/'a1.process.json').write_text(json.dumps(dict(pid=123,stamp='old')))
            for stamp in (None,'new'):
                with patch.object(lab,'process_stamp',return_value=stamp),patch.object(os,'kill') as kill:
                    result=lab.stop_owned(run)
                    kill.assert_not_called()
                    self.assertFalse(result['reclaimed'])

    def test_stop_only_signals_matching_owned_command(self):
        with tempfile.TemporaryDirectory() as directory:
            run=Path(directory)
            (run/'a1.process.json').write_text(json.dumps(dict(pid=123,stamp='same',
                state_dir='/owned/state',config='/owned/topology.json')))
            with patch.object(lab,'process_stamp',return_value='same'),patch.object(os,'kill') as kill:
                with patch.object(Path,'read_bytes',return_value=b'python\0unrelated-job\0'):
                    with self.assertRaisesRegex(RuntimeError,'refuse signal'):
                        lab.stop_owned(run)
                    kill.assert_not_called()
                with patch.object(Path,'read_bytes',return_value=b'python\0/owned/state\0/owned/topology.json\0'):
                    lab.stop_owned(run)
                    kill.assert_called_once_with(123,lab.signal.SIGTERM)


if __name__=='__main__':
    unittest.main()
