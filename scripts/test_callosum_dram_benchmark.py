"""Portable control-path tests. Fake DMA is not RDMA/NUMA hardware evidence."""
import ctypes
import contextlib
import io
import json
import mmap
import os
from pathlib import Path
import tempfile
import threading
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import callosum_dram_benchmark as bench
import callosum_runtime as runtime


class FakeMemory:
    def __init__(self, capacity=8192, node=0):
        self.capacity, self.node, self.placement = capacity, node, {}
        self.memory = mmap.mmap(-1, capacity)
        self.address = ctypes.addressof(ctypes.c_char.from_buffer(self.memory))
        ctypes.memset(self.address, 0x5A, capacity)

    digest = bench.Memory.digest


class DramBenchmarkTest(unittest.TestCase):
    def test_dram_sweep_strategies_use_fixed_mr_and_verify_random_holes_and_tail(self):
        for pattern in ('overwrite','sequential','uniform_random'):
            with self.subTest(pattern=pattern),tempfile.TemporaryDirectory() as directory:
                root=Path(directory); source=FakeMemory(2*1024**2); target=FakeMemory(2*1024**2)
                service=bench.Receiver(target,'fixed-segment','a1','b1','fp',{},root)
                writes=[]
                def native(segments,sources,targets,lengths):
                    for src,dst,n in zip(sources,targets,lengths):
                        self.assertTrue(source.address<=src and src+n<=source.address+source.capacity)
                        self.assertTrue(target.address<=dst and dst+n<=target.address+target.capacity)
                        self.assertEqual(src-source.address,dst-target.address)
                        writes.append((src-source.address,n)); ctypes.memmove(dst,src,n)
                    return lengths
                engine=SimpleNamespace(relay_write_batch_drained=native)
                args=SimpleNamespace(control_port=19201,page_mode='native',sizes=[1],unit='MiB',
                    repetitions=1,warmup_blocks=1,access_pattern=pattern,access_window_bytes=source.capacity,random_seed=17)
                def rpc(url,method,data=None,**kw): return service.handle(method,data or {})
                try:
                    with patch.object(bench,'rpc',side_effect=rpc),patch.object(bench,'snapshot',return_value={}), \
                         patch.object(bench,'resident_pages',return_value={}), \
                         patch.object(bench,'TOTAL_BYTES',1024**2+1),contextlib.redirect_stdout(io.StringIO()):
                        rows=bench.sweep(engine,source,dict(fingerprint='fp'),dict(id='b1'),
                            dict(id='a1',url='http://a1'),{},args,root)
                    self.assertEqual(rows[0]['status'],'PASS')
                    self.assertEqual(rows[0]['window_bytes'],source.capacity)
                    self.assertEqual(rows[0]['access_pattern'],pattern)
                    self.assertEqual(rows[0]['bytes'],1024**2+1)
                    self.assertEqual(service.description['epoch'],service.handle('health',{})['epoch'])
                    self.assertEqual(service.state,'IDLE')
                    self.assertEqual(len(writes),3)  # One warmup + two measured requests.
                finally: source.memory.close(); target.memory.close()

    def test_mbind_preserves_selected_node_with_legacy_and_current_kernel_masks(self):
        for legacy in (False,True):
            for node in (0,1,63,64,127,128):
                with self.subTest(legacy=legacy,node=node):
                    def kernel(address,length,mode,mask,maxnode,flags):
                        word=ctypes.sizeof(ctypes.c_ulong)*8
                        value=sum(int(v)<<(i*word) for i,v in enumerate(mask))
                        imported=value & ((1<<(maxnode-int(legacy)))-1)
                        self.assertEqual(imported,1<<node)
                        self.assertEqual(maxnode,len(mask)*word)
                        self.assertGreater(maxnode,node+1)
                        self.assertEqual((address,length,mode,flags),(4096,128000000,2,0))
                        return 0
                    with patch.object(bench.ctypes.util,'find_library',return_value='fixture-numa'), \
                         patch.object(bench.ctypes,'CDLL',return_value=SimpleNamespace(mbind=kernel)):
                        bench.bind_memory(4096,128000000,node)

    def test_mbind_failure_retains_errno_and_reports_parameters_without_fallback(self):
        def kernel(*args):
            ctypes.set_errno(22)
            return -1
        with patch.object(bench.ctypes.util,'find_library',return_value='fixture-numa'), \
             patch.object(bench.ctypes,'CDLL',return_value=SimpleNamespace(mbind=kernel)):
            with self.assertRaises(OSError) as failure: bench.bind_memory(4096,128000000,1)
        self.assertEqual(failure.exception.errno,22)
        self.assertIn('node=1',str(failure.exception))
        self.assertIn('maxnode=',str(failure.exception))
        self.assertIn('no NUMA fallback',str(failure.exception))

    def test_preflight_nic_cpu_intersection_and_unknown_node(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nic = root/'class/infiniband/hca0/device'
            (nic/'net/eth0').mkdir(parents=True)
            (nic/'numa_node').write_text('2')
            node = root/'devices/system/node/node2'
            node.mkdir(parents=True)
            (node/'cpulist').write_text('4-7,12')
            with patch.object(os, 'sched_getaffinity', create=True, side_effect=[{2, 4, 5}, {4, 5}]), \
                 patch.object(os, 'sched_setaffinity', create=True) as bind:
                actual = bench.nic_placement('hca0', sysroot=root)
                bind.assert_called_once_with(0, {4, 5})
                self.assertEqual(actual['memory_numa_node'], 2)
                self.assertEqual(actual['netdevs'], ['eth0'])
            (nic/'numa_node').write_text('-1')
            with self.assertRaisesRegex(RuntimeError, 'unknown'):
                bench.nic_placement('hca0', sysroot=root)
            with patch.object(os, 'sched_getaffinity', create=True, return_value={0}):
                with self.assertRaisesRegex(RuntimeError, 'cpuset'):
                    bench.nic_placement('hca0', override=2, sysroot=root)

    def test_preflight_residency_requires_all_local_pages(self):
        result = bench.resident_pages(4096, 2, 8192,
                                     '1000 bind:2 file=/dev/zero N2=2 kernelpagesize_kB=4')
        self.assertEqual(result['pages_by_node'], {2: 2})
        for line in ('1000 N2=1 N0=1', '1000 N2=1', '2000 N2=2'):
            with self.assertRaises(RuntimeError):
                bench.resident_pages(4096, 2, 8192, line)

    def test_preflight_setup_reexec_uses_local_receipt_and_scrubs_cxl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python = root/'python'
            python.touch()
            values = dict(PYTHON_BIN=str(python), PYTHONPATH='new-modules',
                          LD_LIBRARY_PATH='new-libs', CALLOSUM_NATIVE_BUILD='new-native')
            (root/'setup.json').write_text(json.dumps(dict(status='PASS', env=values)))
            with patch.dict(os.environ, {'MC_CXL_DEV_PATH':'/dev/never-open', 'LD_PRELOAD':'old.so'}), \
                 patch.object(os, 'execve', side_effect=RuntimeError('reexec')) as execute:
                with self.assertRaisesRegex(RuntimeError, 'reexec'):
                    bench.setup_environment(root)
                env = execute.call_args.args[2]
                self.assertNotIn('MC_CXL_DEV_PATH', env)
                self.assertNotIn('LD_PRELOAD', env)
                self.assertEqual(env['LD_LIBRARY_PATH'], 'new-libs')

    def test_function_advertise_complete_replay_and_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            memory = FakeMemory()
            service = bench.Receiver(memory, 'segment', 'a1', 'b1', 'fp', {}, directory)
            request = dict(attempt='a'*32, length=4096, checksum=memory.digest(4096),
                           sender='b1', fingerprint='fp')
            with patch.object(bench, 'snapshot', return_value={}), \
                 patch.object(bench, 'resident_pages', return_value={}):
                target = service.handle('prepare', request)
                self.assertEqual(target['remote_base'], memory.address)
                self.assertFalse(service.seal_if_idle())
                with self.assertRaises(RuntimeError):
                    service.handle('prepare', dict(request, attempt='b'*32))
                ctypes.memset(memory.address, 0x5A, 4096)
                done = dict(attempt='a'*32, sender_drained=True)
                with self.assertRaises(RuntimeError):
                    service.handle('complete', dict(done, sender_drained=False))
                self.assertEqual(service.handle('complete', done)['status'], 'PASS')
                with self.assertRaises(RuntimeError):
                    service.handle('prepare', request)
                service.handle('prepare', dict(request, attempt='b'*32))
                with self.assertRaisesRegex(RuntimeError, 'checksum'):
                    service.handle('complete', dict(done, attempt='b'*32))
                self.assertEqual(service.state, 'QUARANTINED')
                self.assertFalse(service.seal_if_idle())
            memory.memory.close()

    def test_failure_bad_target_before_advertisement(self):
        with tempfile.TemporaryDirectory() as directory:
            memory = FakeMemory()
            service = bench.Receiver(memory, 'segment', 'a1', 'b1', 'fp', {}, directory)
            request = dict(attempt='a'*32, length=4096, checksum='hash', sender='b1', fingerprint='fp')
            for changes in ({'length':8193}, {'length':-1}, {'sender':'other'},
                            {'fingerprint':'old'}, {'attempt':'not-a-transfer'}):
                with self.assertRaises(RuntimeError):
                    service.handle('prepare', dict(request, **changes))
                self.assertEqual(service.state, 'IDLE')
            memory.memory.close()

    def test_function_http_sweep_real_control_fake_dma(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, destination = FakeMemory(1024**2), FakeMemory(1024**2)
            (root/'receiver').mkdir()
            service = bench.Receiver(destination, 'receiver-segment', 'a1', 'b1', 'fp', {}, root/'receiver')
            http = runtime.server('http://127.0.0.1:0', service)
            thread = threading.Thread(target=http.serve_forever, daemon=True)
            thread.start()
            calls = []
            class FakeEngine:
                def relay_write_batch_drained(self, segments, sources, targets, lengths):
                    self_test.assertEqual(segments, ['receiver-segment'])
                    self_test.assertEqual(len(lengths), 1)
                    ctypes.memmove(targets[0], sources[0], lengths[0])
                    calls.append(lengths[0])
                    return lengths
            self_test = self
            args = SimpleNamespace(control_port=http.server_address[1], sizes=[1], unit='MiB',
                                   repetitions=2, warmup_blocks=1)
            try:
                with patch.dict(os.environ, CALLOSUM_CONTROL_TOKEN='portable-test-token-'*3), \
                     patch.object(bench, 'TOTAL_BYTES', 2*1024**2), \
                     patch.object(bench, 'snapshot', return_value={}), \
                     patch.object(bench, 'resident_pages', return_value={}):
                    with contextlib.redirect_stdout(io.StringIO()):
                        result = bench.sweep(FakeEngine(), source, {'fingerprint':'fp'}, {'id':'b1'},
                                             {'id':'a1','url':'http://127.0.0.1:1'}, {}, args, root)
                self.assertEqual(len(calls), 6)  # Two warmup + four measured calls.
                self.assertEqual([r['native_calls'] for r in result], [2, 2])
                self.assertEqual([r['bytes'] for r in result], [2*1024**2]*2)
                self.assertEqual(service.state, 'IDLE')
                self.assertTrue(service.seal_if_idle())
                self.assertEqual(len(list(root.glob('block-*.json'))), 2)
                self.assertTrue(all('remote_base' not in p.read_text() for p in root.rglob('*.json')))
            finally:
                http.shutdown()
                http.server_close()
                thread.join()
                source.memory.close()
                destination.memory.close()

    def test_failure_lost_target_reply_retains_exposure(self):
        memory = FakeMemory(1024**2)
        health = dict(node_id='a1', peer_id='b1', fingerprint='fp', memory='dram',
                      configured_qps=16, source_identity=bench.source_identity(), capacity=1024**2)
        args = SimpleNamespace(control_port=19201, sizes=[1], unit='MiB', repetitions=1)
        safety = {}
        try:
            with patch.object(bench, 'rpc', side_effect=[health, TimeoutError('lost prepare')]):
                with self.assertRaises(TimeoutError):
                    bench.sweep(None, memory, {'fingerprint':'fp'}, {'id':'b1'},
                                {'id':'a1','url':'http://127.0.0.1:1'}, {}, args, Path('.'), safety)
            self.assertTrue(safety['exposed'])
        finally:
            memory.memory.close()

    def test_failure_native_error_does_not_complete_or_reuse_target(self):
        memory = FakeMemory(1024**2)
        health = dict(node_id='a1', peer_id='b1', fingerprint='fp', memory='dram',
                      configured_qps=16, source_identity=bench.source_identity(), epoch='boot',
                      capacity=1024**2,segment='peer')
        target = dict(epoch='boot',length=1024**2,address_kind='classic_va',
                      segment='peer',remote_base=8192,node_id='a1')
        args = SimpleNamespace(control_port=19201, sizes=[1], unit='MiB', repetitions=1, warmup_blocks=0)
        engine = SimpleNamespace(relay_write_batch_drained=lambda *a: (_ for _ in ()).throw(RuntimeError('unknown submit')))
        safety = {}
        def control_rpc(url, method, data=None):
            return health if method == 'health' else dict(target, attempt=data['attempt'])
        try:
            with patch.object(bench,'rpc',side_effect=control_rpc) as control, \
                 patch.object(bench,'snapshot',return_value={}):
                with self.assertRaisesRegex(RuntimeError,'unknown submit'):
                    bench.sweep(engine,memory,{'fingerprint':'fp'},{'id':'b1'},
                                {'id':'a1','url':'http://127.0.0.1:1'},{},args,Path('.'),safety)
                self.assertEqual([call.args[1] for call in control.call_args_list],['health','prepare'])
                self.assertTrue(safety['exposed'])
        finally:
            memory.memory.close()

    def test_failure_evidence_write_cannot_release_quarantine(self):
        with patch.object(bench,'save',side_effect=OSError('disk full')), \
             patch.object(bench.signal,'signal') as handlers, \
             patch.object(bench.threading.Event,'wait',side_effect=SystemExit('parked')), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(SystemExit,'parked'):
                bench.retain_uncertain(RuntimeError('unknown DMA'),Path('.'))
            self.assertEqual(handlers.call_count,3)

    def test_function_sender_main_receipt_to_output_and_unregister(self):
        """Actual CLI parser/main + localhost control; native/NUMA hardware seams are fake."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            c = dict(mode='rdma', path_mode='direct_dax_write', abi_version=2,
                     metadata='P2PHANDSHAKE', master='http://127.0.0.1:19100',
                     pools={p:dict(capacity=1024**3, map_offset=0, mapping_alignment=2097152,
                                   owner=n, physical_pool_id=p) for p,n in [('A','a1'),('B','b1')]},
                     nodes=[dict(id=n,pool=p,url=f'http://127.0.0.1:{19101+i}',path='/dev/never-open',
                                 te_endpoint=f'127.0.0.1:{19301+i}',rdma_device='fake0',
                                 can_send=True,can_receive=True) for i,(p,n) in enumerate([('A','a1'),('B','b1')])])
            config = root/'lab.json'
            config.write_text(json.dumps(c))
            c = runtime.config(config)
            private = root/'control.token'
            private.write_text('test-shared-private-token-'*3)
            private.chmod(0o600)
            (root/'receiver').mkdir()
            memory = FakeMemory(1024**2)
            service = bench.Receiver(memory,'segment','a1','b1',c['fingerprint'],{},root/'receiver')
            http = runtime.server('http://127.0.0.1:0', service)
            thread = threading.Thread(target=http.serve_forever,daemon=True)
            thread.start()
            package, engine_module, native = (types.ModuleType(n) for n in
                                              ('mooncake','mooncake.engine','callosum_relay_native'))
            package.__path__, package.engine = [], engine_module
            engine_path = root/'build/python/mooncake/engine.so'
            engine_path.parent.mkdir(parents=True)
            engine_path.touch()
            engine_module.__file__ = str(engine_path)
            native_path = root/'native/binding.so'
            native_path.parent.mkdir()
            native_path.touch()
            native.__file__, native.ABI_VERSION = str(native_path), 2
            instances = []
            class Engine:
                def __init__(self):
                    instances.append(self)
                    self.unregistered = False
                def relay_abi_version(self): return 2
                def initialize(self, *args): return 0
                def relay_rdma_available(self): return True
                def register_memory(self, address, capacity): return 0
                def get_rpc_port(self): return 19401
                def unregister_memory(self, address):
                    self.unregistered = True
                    return 0
                def relay_write_batch_drained(self, segments, sources, targets, lengths):
                    ctypes.memmove(targets[0], sources[0], lengths[0])
                    return lengths
            engine_module.TransferEngine = Engine
            argv = ['dram-benchmark','--role','sender','--node','b1','--peer','a1',
                    '--config',str(config),'--token-file',str(private),'--setup-run',str(root),
                    '--control-port',str(http.server_address[1]),'--sizes','1','--repetitions','1',
                    '--output',str(root/'output')]
            try:
                with patch.dict(os.environ, CALLOSUM_NATIVE_BUILD=str(native_path.parent)), \
                     patch.dict(bench.sys.modules, {'mooncake':package,'mooncake.engine':engine_module,
                                                  'callosum_relay_native':native}), \
                     patch.object(bench.sys,'argv',argv), patch.object(bench.sys,'platform','linux'), \
                     patch.object(bench,'setup_environment',return_value={'build':str(root/'build')}), \
                     patch.object(bench,'nic_placement',return_value={'memory_numa_node':0}), \
                     patch.object(bench,'Memory',FakeMemory), patch.object(bench,'TOTAL_BYTES',1024**2), \
                     patch.object(bench,'snapshot',return_value={}), \
                     patch.object(bench,'resident_pages',return_value={}), \
                     patch.object(bench.signal,'signal'), \
                     contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(bench.main(), 0)
                    previous = {p.name:p.read_bytes() for p in (root/'output').iterdir()}
                    with contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(bench.main(), 1)
                    self.assertEqual(previous, {p.name:p.read_bytes() for p in (root/'output').iterdir()})
                self.assertTrue(instances[0].unregistered)
                summary = json.loads((root/'output/summary.json').read_text())
                self.assertEqual(summary['results'][0]['native_calls'],1)
                self.assertTrue((root/'output/summary.csv').is_file())
                self.assertNotIn(private.read_text(), (root/'output/environment.json').read_text())
            finally:
                http.shutdown()
                http.server_close()
                thread.join()
                memory.memory.close()

    def test_status_native_cli_entry_help_without_installed_engine(self):
        import subprocess
        entry = Path(__file__).with_name('run_callosum_relay.sh')
        result = subprocess.run(['bash', str(entry), 'dram-benchmark', '--help'],
                                capture_output=True, text=True, env=dict(os.environ, PYTHON_BIN=os.sys.executable))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--setup-run', result.stdout)


if __name__ == '__main__':
    unittest.main()
