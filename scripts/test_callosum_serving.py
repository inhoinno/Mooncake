import json
import hashlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import callosum_serving as serving


class ServingTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source, self.requester = self.root / 'source', self.root / 'requester'
        self.source.mkdir()
        self.token = self.root / 'control.token'
        self.token.write_text('a' * 64 + '\n')
        self.token.chmod(0o600)
        self.spec = dict(schema=1, campaign='test-campaign', master_node='b1', master_port=50051,
                         metrics_port=9003, model=dict(path='/installed/model', revision='pinned',
                         tokenizer_revision='pinned', dtype='float16', kv_dtype='auto', block_size=16,
                         prefix_blocks=2, token_id=42, max_model_len=64), nodes={})
        for i, role in enumerate(('requester', 'source'), 1):
            node = dict(hostname=socket.gethostname() if i == 1 else 'peer-fixture', ip=f'192.0.2.{i}',
                        role=role, pool_id=f'pool{i}', physical_pool_id=f'physical{i}', dax_path='/dev/dax0.0',
                        owner='fixture', hca='fixture', cuda_devices='0', capacity=2**28, alignment=2**21,
                        owned_size=2**28, owned_offset=0, mapping_offset=0, api_port=18000, te_port=18001,
                        authorization=str(self.root / f'auth{i}.json'))
            evidence = self.root / f'evidence{i}.txt'
            evidence.write_text('fixture only\n')
            serving.save(node['authorization'], dict(node, exclusive=True, physical_evidence=str(evidence),
                                                      challenge_evidence=str(evidence)))
            self.spec['nodes']['a1' if i == 1 else 'b1'] = node
        self.spec_file = self.root / 'spec.json'
        serving.save(self.spec_file, self.spec)

    def init(self):
        return serving.initialize(self.spec_file, 'a1', self.token, self.requester)

    def test_preflight_resolved_environment_and_edit_detection(self):
        setup = self.init()
        self.assertEqual(self.init(), setup)
        self.assertEqual(serving.load(self.requester), setup)
        _, fixed = serving.environment(setup, 'worker')
        self.assertEqual(fixed['PYTHONHASHSEED'], '0')
        for value, allowed in (('16', True), ('4', False), ('', False)):
            with patch.dict(os.environ, {'MC_NUM_QP_PER_EP': value}):
                if allowed:
                    serving.environment(setup, 'worker')
                else:
                    with self.assertRaisesRegex(RuntimeError, 'MC_NUM_QP_PER_EP'):
                        serving.environment(setup, 'worker')
        self.assertEqual(json.loads(Path(setup['connector']).read_text())['metadata_server'], 'P2PHANDSHAKE')
        Path(setup['native']).write_text('{}')
        with self.assertRaisesRegex(RuntimeError, 'native config changed'):
            serving.load(self.requester)

    def test_preflight_canonical_init_missing_inputs_and_rerun(self):
        launcher = serving.ROOT / 'scripts/callosum_lab.sh'
        argv = ['bash', str(launcher), '--serving', 'init', '--run', str(self.requester),
                '--spec', str(self.spec_file), '--node', 'a1', '--token', str(self.token)]
        env = dict(os.environ, PYTHON_BIN=sys.executable)
        result = subprocess.run(argv[:-1] + [str(self.root / 'missing')], env=env, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.requester.exists())
        for _ in range(2):
            result = subprocess.run(argv, env=env, capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(result.stdout)['result']['run'], str(self.requester.resolve()))

    def test_function_instrument_legacy_seam_twice(self):
        source = serving.ROOT.parent / 'hbf/src/vllm-0.23.0/vllm'
        if not (source / serving.WORKER).is_file():
            self.skipTest('audited sibling vLLM reference checkout is not present')
        fixture = self.root / 'vllm'
        self.requester.mkdir()
        for relative in (serving.WORKER, serving.SCHEDULER):
            target = fixture / relative
            target.parent.mkdir(parents=True)
            target.write_text((source / relative).read_text())
        expected = serving.instrument(fixture, self.requester)
        self.assertEqual(serving.instrument(fixture, self.requester), expected)

    def fixture(self, duplex=False, reverse_delay_ns=0):
        if duplex:
            self.spec['profile'] = 'bidirectional'
            self.spec_file.write_text(serving.encoded(self.spec))
        setup = self.init()
        for node, directory in (('a1', self.requester), ('b1', self.source)):
            serving.save(directory / 'preflight.json', dict(
                node_id=node, hostname=self.spec['nodes'][node]['hostname'],
                spec_sha256=hashlib.sha256(serving.encoded(self.spec).encode()).hexdigest(),
                token_sha256=setup['token_sha256'], model_manifest={}, connector_sha256='same', scheduler_sha256='same',
                vllm_version='fixture', native_abi=1, native_mux_abi=1, source_fingerprint={}))
            (directory / 'host-preflight').mkdir()
            serving.save(directory / 'host-preflight/summary.json', {})
        for node_id, directory in (('a1', self.requester), ('b1', self.source)):
            node = self.spec['nodes'][node_id]
            te = f"{node['ip']}:19800"
            self.line(directory / 'worker.log', 'component=callosum_store event=pool_mounted '
                 f"pool={node['pool_id']} segment={serving.endpoint(node)} te_endpoint={te} backend=devdax")
        self.direction('b1', self.source, 'a1', self.requester)
        if duplex:
            self.direction('a1', self.requester, 'b1', self.source, reverse_delay_ns)
        return setup

    @staticmethod
    def line(path, value):
        with path.open('a') as stream:
            stream.write(value + '\n')

    def direction(self, source_id, source_run, target_id, target_run, delay_ns=0):
        duplex = self.spec.get('profile') == 'bidirectional'
        src, dst = (self.spec['nodes'][k] for k in (source_id, target_id))
        src_te, dst_te = (f"{n['ip']}:19800" for n in (src, dst))
        key_prefix = f'{source_id}-fixture-key' if duplex else 'fixture-key'
        phases = ('produce', 'reuse', 'local') if duplex else ('produce', 'reuse', 'local', 'cold')
        for phase in phases:
            directory = source_run if phase == 'produce' else target_run
            node_id = source_id if phase == 'produce' else target_id
            log = directory / ('cold.log' if phase == 'cold' else 'worker.log')
            identity = serving.request_identity(self.spec, phase, node_id)
            api_id = 'cmpl-' + identity
            serving.save(directory / f'{phase}.response.json', dict(phase=phase, request_id=identity,
                         elapsed_ns=100, scheduled_unix_s=12345 if duplex and phase == 'reuse' else None,
                         response=dict(id=api_id, usage=dict(prompt_tokens=33, completion_tokens=1),
                                                       choices=[dict(text='fixture')])) )
            tick = 0

            def trace(event, **fields):
                nonlocal tick
                tick += 1
                self.line(log, '[TTFT_TRACE] ' + json.dumps(dict(event=event, timestamp_ns=tick,
                     request_id=api_id + '-0-1234abcd', **fields)))

            reused = phase in ('reuse', 'local')
            if reused:
                trace('external_kv_wait_start', external_cached_tokens=32, local_cached_tokens=0)
            if phase != 'cold':
                for i in range(2):
                    trace('external_kv_chunk_complete' if reused else 'callosum_connector_export',
                          object_key=f'{key_prefix}-{i}', start_token=16*i, end_token=16*(i+1),
                          status='ok', tp_rank=0, group_index=0, tensor_bytes=[8, 8], bytes=16,
                          replica_endpoints=[src_te], result_code=16)
            if reused:
                trace('external_kv_request_complete', status='ok', token_count=32)
                trace('external_kv_completion_observed')
            trace('callosum_compute', computed_tokens_before=32 if reused else 0,
                  scheduled_tokens=1 if reused else 33)

        for i in range(2):
            key = str(serving.crc64(f'{key_prefix}-{i}'.encode()))
            identity = dict(attempt=f'copy{i}', source_generation=f's{i}', target_generation=f'd{i}')
            if duplex:
                identity['attempt'] += source_id
            gens = {k: v for k, v in identity.items() if k.endswith('generation')}

            def native(directory, event, **fields):
                self.line(directory / 'worker.log', f'component=callosum_store event={event} ' +
                     ' '.join(f'{k}={v}' for k, v in fields.items()))

            clock = {'a1': 10**10, 'b1': 2 * 10**10}
            auth = 10**7 * (1 + i)
            begin = clock[source_id] + auth + 100_000 + delay_ns
            native(source_run, 'store_task_dispatch', key_crc=key, task=f'task{i}{source_id}')
            native(source_run, 'receiver_authorized', **identity, key_crc=key, bytes=16,
                   source_pool=src['pool_id'], destination_pool=dst['pool_id'], receiver=dst_te,
                   source_offset=i * 16, destination_offset=1024 + i * 16, receiver_epoch='epoch',
                   authorize_start_ns=clock[source_id] + auth,
                   authorize_end_ns=clock[source_id] + auth + 100_000,
                   receiver_steady_ns=clock[target_id] + auth + 50_000)
            native(source_run, 'mux_queued', attempt=identity['attempt'], bytes=16)
            native(source_run, 'mux_admitted', attempt=identity['attempt'], bytes=16)
            native(source_run, 'write_drained', **identity, protocol='rdma', opcode='WRITE', sender=src_te,
                   batch=1, batch_bytes=16, completed_end=16, native_start_ns=begin, native_end_ns=begin + 1_000_000)
            native(source_run, 'mux_gathered', attempt=identity['attempt'], bytes=16)
            native(source_run, 'copy_verified', attempt=identity['attempt'], key_crc=key, bytes=16)
            native(source_run, 'replica_published', key_crc=key, **gens)
            native(target_run, 'initial_local_miss', key_crc=key, pool=dst['pool_id'])
            native(target_run, 'remote_materialization', key_crc=key, task=f'task{i}{source_id}')
            native(target_run, 'receiver_validated', attempt=identity['attempt'], bytes=16, content_ok=1, epoch='epoch')
            for phase in ('remote', 'local'):
                native(target_run, 'replica_usable', key_crc=key, phase=phase)
                native(target_run, 'gpu_load_verified', key_crc=key, bytes=16,
                       path='local_cxl_copy', replica_generation=gens['target_generation'])

    def test_status_bidirectional_verifies_both_request_paths(self):
        setup = self.fixture(duplex=True)
        self.assertEqual(json.loads(serving.command(setup, 'worker')[-1])['kv_role'], 'kv_both')
        result = serving.verify(setup, self.source)
        self.assertEqual(result['status'], 'BIDIRECTIONAL_NATIVE_CALL_OVERLAP_PROVEN')
        self.assertGreater(result['native_call_overlap_lower_bound_ns'], 0)
        self.assertEqual(result['physical_device_overlap'], 'UNVERIFIED')
        self.assertEqual([d['requester_node'] for d in result['directions']], ['a1', 'b1'])
        self.assertEqual([d['reused_prefix_tokens'] for d in result['directions']], [32, 32])
        self.assertEqual(serving.verify(setup, self.source), result)

    def test_status_shared_start_with_serial_writes_is_insufficient(self):
        setup = self.fixture(duplex=True, reverse_delay_ns=30_000_000)
        result = serving.verify(setup, self.source)
        self.assertEqual(result['status'], 'BIDIRECTIONAL_REUSE_NO_OVERLAP')
        self.assertEqual(result['native_call_overlap_lower_bound_ns'], 0)

    def test_function_bidirectional_sends_peer_salt_once(self):
        setup = self.fixture(duplex=True)
        (self.requester / 'reuse.response.json').unlink()
        owner = dict(campaign=self.spec['campaign'], role='worker', pid=17, stamp='start17')
        for kind in ('process', 'ready'):
            serving.save(self.requester / f'worker.{kind}.json', owner)
        response = dict(id='cmpl-test-campaign-reuse-a1', choices=[dict(text='fixture')],
                        usage=dict(prompt_tokens=33, completion_tokens=1))
        with patch.object(serving, 'process_stamp', return_value='start17'), \
                patch.object(serving.time, 'time', return_value=12344), \
                patch.object(serving.time, 'sleep') as wait, \
                patch.object(serving, 'http', return_value=response) as http:
            first = serving.request(setup, 'reuse', self.source, 12345)
            self.assertEqual(first, serving.request(setup, 'reuse', self.source, 12345))
            wait.assert_called_once_with(1)
            http.assert_called_once()
            self.assertEqual(http.call_args.args[1]['cache_salt'], 'test-campaign-b1')

    def test_failure_bidirectional_drain_requires_terminal_tasks(self):
        setup = self.fixture(duplex=True)
        (self.requester / 'arena.journal').write_text('IDENTITY test-campaign:pool1\n')
        with self.assertRaisesRegex(RuntimeError, 'terminal status missing'):
            serving.drain(setup)
        for task in ('task0b1', 'task1b1'):
            self.line(self.requester / 'worker.log',
                      f'component=callosum_store event=materialization_terminal task={task}')
        receipt = Path(serving.drain(setup))
        self.assertEqual(json.loads(receipt.read_text())['tasks'], ['task0b1', 'task1b1'])
        self.assertEqual(serving.drain(setup), str(receipt))
        (self.requester / 'local.response.json').unlink()
        with self.assertRaisesRegex(RuntimeError, 'closed request admission'):
            serving.request(setup, 'local', self.source)

    def test_status_joins_real_event_shapes_and_rejects_recompute(self):
        setup = self.fixture()
        result = serving.verify(setup, self.source)
        self.assertEqual(result['reused_prefix_tokens'], 32)
        self.assertEqual(result['source'], '192.0.2.2:19800')
        log = self.requester / 'worker.log'
        log.write_text(log.read_text().replace('"computed_tokens_before": 32', '"computed_tokens_before": 0'))
        with self.assertRaisesRegex(RuntimeError, 'compute overlaps'):
            serving.verify(setup, self.source)

    def test_function_reuse_sends_its_own_request_id_after_checking_source_exports(self):
        setup = self.fixture()
        (self.requester / 'reuse.response.json').unlink()
        owner = dict(campaign=self.spec['campaign'], role='worker', pid=17, stamp='start17')
        serving.save(self.requester / 'worker.process.json', owner)
        serving.save(self.requester / 'worker.ready.json', owner)
        response = dict(id='cmpl-test-campaign-reuse', choices=[dict(text='fixture')],
                        usage=dict(prompt_tokens=33, completion_tokens=1))
        with patch.object(serving, 'process_stamp', return_value='replacement'), \
                patch.object(serving, 'http') as http:
            with self.assertRaisesRegex(RuntimeError, 'service changed after readiness'):
                serving.request(setup, 'reuse', self.source)
            http.assert_not_called()
        with patch.object(serving, 'process_stamp', return_value='start17'), \
                patch.object(serving, 'http', return_value=response) as request:
            report = serving.request(setup, 'reuse', self.source)
            self.assertEqual(serving.request(setup, 'reuse', self.source), report)
            request.assert_called_once()
        self.assertEqual(request.call_args.args[2], 'test-campaign-reuse')
        self.assertEqual(report['request_id'], 'test-campaign-reuse')

    def test_failure_rejects_tcp_and_wrong_gpu_generation(self):
        setup = self.fixture()
        log = self.source / 'worker.log'
        original = log.read_text()
        log.write_text(original.replace('protocol=rdma', 'protocol=tcp'))
        with self.assertRaisesRegex(RuntimeError, 'wrong native opcode'):
            serving.verify(setup, self.source)
        log.write_text(original.replace('te_endpoint=192.0.2.2:19800', 'te_endpoint=192.0.2.1:19800'))
        with self.assertRaisesRegex(RuntimeError, 'wrong interface'):
            serving.verify(setup, self.source)
        log.write_text(original)
        log = self.requester / 'worker.log'
        log.write_text(log.read_text().replace('replica_generation=d0', 'replica_generation=d1'))
        with self.assertRaisesRegex(RuntimeError, 'GPU copy/checksum'):
            serving.verify(setup, self.source)

    def test_failure_pending_dma_cannot_retire(self):
        setup = self.init()
        with self.assertRaisesRegex(RuntimeError, 'missing native arena journal'):
            serving.transfer_snapshot_ready(setup, 'worker')
        journal = self.requester / 'arena.journal'
        journal.write_text('IDENTITY test-campaign:pool1\ncopy-rx PENDING\n')
        with self.assertRaisesRegex(RuntimeError, 'pending DMA'):
            serving.transfer_snapshot_ready(setup, 'worker')
        with journal.open('a') as stream:
            stream.write('copy-rx VALIDATED\n')
        serving.transfer_snapshot_ready(setup, 'worker')


if __name__ == '__main__':
    unittest.main()
