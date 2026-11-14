import json
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

    def fixture(self):
        setup = self.init()
        for node, directory in (('a1', self.requester), ('b1', self.source)):
            serving.save(directory / 'preflight.json', dict(
                node_id=node, hostname=self.spec['nodes'][node]['hostname'], spec_sha256='same',
                token_sha256='same', model_manifest={}, connector_sha256='same', scheduler_sha256='same',
                vllm_version='fixture', native_abi=1, native_mux_abi=1, source_fingerprint={}))
            (directory / 'host-preflight').mkdir()
            serving.save(directory / 'host-preflight/summary.json', {})
        src, dst = (self.spec['nodes'][k] for k in ('b1', 'a1'))

        def line(path, value):
            with path.open('a') as stream:
                stream.write(value + '\n')

        for phase in ('produce', 'reuse', 'local', 'cold'):
            directory = self.source if phase == 'produce' else self.requester
            log = directory / ('cold.log' if phase == 'cold' else 'worker.log')
            identity = self.spec['campaign'] + '-' + phase
            api_id = 'cmpl-' + identity
            serving.save(directory / f'{phase}.response.json', dict(phase=phase, request_id=identity,
                         elapsed_ns=100, response=dict(id=api_id, usage=dict(prompt_tokens=33, completion_tokens=1),
                                                       choices=[dict(text='fixture')])) )
            tick = 0

            def trace(event, **fields):
                nonlocal tick
                tick += 1
                line(log, '[TTFT_TRACE] ' + json.dumps(dict(event=event, timestamp_ns=tick,
                     request_id=api_id + '-0-1234abcd', **fields)))

            reused = phase in ('reuse', 'local')
            if reused:
                trace('external_kv_wait_start', external_cached_tokens=32, local_cached_tokens=0)
            if phase != 'cold':
                for i in range(2):
                    trace('external_kv_chunk_complete' if reused else 'callosum_connector_export',
                          object_key=f'fixture-key-{i}', start_token=16*i, end_token=16*(i+1),
                          status='ok', tp_rank=0, group_index=0, tensor_bytes=[8, 8], bytes=16,
                          replica_endpoints=[serving.endpoint(src)], result_code=16)
            if reused:
                trace('external_kv_request_complete', status='ok', token_count=32)
                trace('external_kv_completion_observed')
            trace('callosum_compute', computed_tokens_before=32 if reused else 0,
                  scheduled_tokens=1 if reused else 33)

        for i in range(2):
            key = str(serving.crc64(f'fixture-key-{i}'.encode()))
            identity = dict(attempt=f'copy{i}', source_generation=f's{i}', target_generation=f'd{i}')
            gens = {k: v for k, v in identity.items() if k.endswith('generation')}

            def native(directory, event, **fields):
                line(directory / 'worker.log', f'component=callosum_store event={event} ' +
                     ' '.join(f'{k}={v}' for k, v in fields.items()))

            native(self.source, 'store_task_dispatch', key_crc=key, task=f'task{i}')
            native(self.source, 'receiver_authorized', **identity, key_crc=key, bytes=16,
                   source_pool=src['pool_id'], destination_pool=dst['pool_id'], receiver=serving.endpoint(dst),
                   source_offset=0, destination_offset=0, receiver_epoch='epoch')
            native(self.source, 'mux_queued', attempt=identity['attempt'], bytes=16)
            native(self.source, 'mux_admitted', attempt=identity['attempt'], bytes=16)
            native(self.source, 'write_drained', **identity, protocol='rdma', opcode='WRITE', sender=serving.endpoint(src),
                   batch=1, batch_bytes=16, completed_end=16)
            native(self.source, 'mux_gathered', attempt=identity['attempt'], bytes=16)
            native(self.source, 'copy_verified', attempt=identity['attempt'], key_crc=key, bytes=16)
            native(self.source, 'replica_published', key_crc=key, **gens)
            native(self.requester, 'initial_local_miss', key_crc=key, pool=dst['pool_id'])
            native(self.requester, 'remote_materialization', key_crc=key, task=f'task{i}')
            native(self.requester, 'receiver_validated', attempt=identity['attempt'], bytes=16, content_ok=1, epoch='epoch')
            for phase in ('remote', 'local'):
                native(self.requester, 'replica_usable', key_crc=key, phase=phase)
                native(self.requester, 'gpu_load_verified', key_crc=key, bytes=16,
                       path='local_cxl_copy', replica_generation=gens['target_generation'])
        return setup

    def test_status_joins_real_event_shapes_and_rejects_recompute(self):
        setup = self.fixture()
        result = serving.verify(setup, self.source)
        self.assertEqual(result['reused_prefix_tokens'], 32)
        log = self.requester / 'worker.log'
        log.write_text(log.read_text().replace('"computed_tokens_before": 32', '"computed_tokens_before": 0'))
        with self.assertRaisesRegex(RuntimeError, 'compute overlaps'):
            serving.verify(setup, self.source)

    def test_failure_rejects_tcp_and_wrong_gpu_generation(self):
        setup = self.fixture()
        log = self.source / 'worker.log'
        original = log.read_text()
        log.write_text(original.replace('protocol=rdma', 'protocol=tcp'))
        with self.assertRaisesRegex(RuntimeError, 'wrong native opcode'):
            serving.verify(setup, self.source)
        log.write_text(original)
        log = self.requester / 'worker.log'
        log.write_text(log.read_text().replace('replica_generation=d0', 'replica_generation=d1'))
        with self.assertRaisesRegex(RuntimeError, 'GPU copy/checksum'):
            serving.verify(setup, self.source)


if __name__ == '__main__':
    unittest.main()
