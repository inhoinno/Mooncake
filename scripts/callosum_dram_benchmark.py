#!/usr/bin/env python3
"""Matched, serial DRAM RDMA WRITE control. No DAX or object directory access."""
import argparse
import ctypes
import ctypes.util
import hashlib
import json
import mmap
import os
from pathlib import Path
import resource
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from urllib.parse import urlparse

from callosum_runtime import config, require, rpc, server
from callosum_transport_benchmark import SIZES, TOTAL_BYTES, block_bytes, write_loop
from callosum_benchmark_report import write_report, write_request_csv, enrich_trial


def source_identity():
    return {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            for name in ('callosum_dram_benchmark.py', 'callosum_transport_benchmark.py')}


def save(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def setup_environment(directory):
    """Re-exec with the successful host-local build, before loading its .so files."""
    path = Path(directory).resolve() / 'setup.json'
    receipt = json.loads(path.read_text())
    require(receipt['status'] == 'PASS', 'setup receipt is not PASS')
    names = ('PYTHON_BIN', 'PYTHONPATH', 'LD_LIBRARY_PATH', 'CALLOSUM_NATIVE_BUILD')
    values = {name: receipt['env'][name] for name in names}
    require(Path(values['PYTHON_BIN']).is_file(), 'host-local setup Python is absent')
    if os.environ.get('CALLOSUM_DRAM_SETUP') != str(path):
        env = dict(os.environ, **values, CALLOSUM_DRAM_SETUP=str(path))
        # A DRAM control must never auto-install an inherited CXL backend.
        for key in tuple(env):
            if key.startswith(('MC_CXL_', 'FAKETRACT_')) or key in ('LD_PRELOAD', 'PYTHONHOME'):
                env.pop(key, None)
        os.execve(values['PYTHON_BIN'], [values['PYTHON_BIN'], str(Path(__file__).resolve()),
                                      *sys.argv[1:]], env)
    require(all(os.environ.get(k) == v for k, v in values.items()), 'setup environment changed')
    return receipt


def cpulist(value):
    result = set()
    for item in value.strip().split(','):
        if not item:
            continue
        ends = [int(n) for n in item.split('-')]
        require(len(ends) in (1, 2) and ends[0] >= 0 and ends[-1] >= ends[0], 'invalid CPU list')
        result.update(range(ends[0], ends[-1] + 1))
    return result


def nic_placement(device, override=None, sysroot=Path('/sys')):
    require(device and '/' not in device and device not in ('.', '..'), 'invalid RDMA device')
    base = sysroot / 'class/infiniband' / device / 'device'
    nic_node = int((base / 'numa_node').read_text())
    require(override is not None or nic_node >= 0,
            'NIC NUMA node is unknown; supply an explicitly reviewed --numa-node')
    node = nic_node if override is None else override
    require(type(node) is int and node >= 0, 'invalid NUMA node')
    allowed = os.sched_getaffinity(0)
    local = cpulist((sysroot / f'devices/system/node/node{node}/cpulist').read_text()) & allowed
    require(local, 'NIC-local CPUs are excluded by this process cpuset')
    os.sched_setaffinity(0, local)
    require(os.sched_getaffinity(0) == local, 'CPU affinity was not applied')
    netdevs = sorted(p.name for p in (base / 'net').iterdir())
    require(netdevs, 'no netdev found for selected RDMA device')
    return dict(rdma_device=device, nic_numa_node=nic_node, memory_numa_node=node,
                numa_override=override is not None, cpus=sorted(local), netdevs=netdevs)


def bind_memory(address, length, node):
    name = ctypes.util.find_library('numa')
    require(name, 'libnuma is required; install the lab OS libnuma package')
    lib = ctypes.CDLL(name, use_errno=True)
    word = ctypes.sizeof(ctypes.c_ulong) * 8
    mask = (ctypes.c_ulong * (node // word + 1))()
    mask[node // word] = 1 << (node % word)
    lib.mbind.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_int,
                         ctypes.POINTER(ctypes.c_ulong), ctypes.c_ulong, ctypes.c_uint]
    lib.mbind.restype = ctypes.c_long
    # MPOL_BIND, before first touch. Do not migrate unrelated process pages.
    if lib.mbind(address, length, 2, mask, node + 1, 0) != 0:
        raise OSError(ctypes.get_errno(), 'mbind failed; no NUMA fallback')


def resident_pages(address, node, length, maps=None):
    lines = (Path('/proc/self/numa_maps').read_text() if maps is None else maps).splitlines()
    line = next((line for line in lines if int(line.split()[0], 16) == address), None)
    require(line is not None, 'cannot identify buffer VMA in numa_maps')
    fields = dict(x.split('=', 1) for x in line.split()[1:] if '=' in x)
    counts = {int(k[1:]): int(v) for k, v in fields.items() if k[0] == 'N' and k[1:].isdigit()}
    page_size = int(fields.get('kernelpagesize_kB', '4')) * 1024
    require(counts.get(node, 0) * page_size >= length and
            all(n == node or count == 0 for n, count in counts.items()),
            f'DRAM pages are not fully resident on NUMA node {node}: {counts}')
    return dict(pages_by_node=counts, kernel_page_bytes=page_size)


def allocation_geometry(capacity, page_mode):
    require(type(capacity) is int and capacity>0 and page_mode in ('native','huge2MB'),
            'invalid DRAM allocation size/page mode')
    huge=2**21
    return ((capacity+huge-1)//huge)*huge if page_mode=='huge2MB' else capacity


def hugepage_capacity(node, capacity, sysroot=Path('/sys')):
    root=sysroot/f'devices/system/node/node{node}/hugepages/hugepages-2048kB'
    free=int((root/'free_hugepages').read_text())
    required=allocation_geometry(capacity,'huge2MB')//2**21
    require(free>=required,
            f'NUMA node {node} has {free} free 2 MiB HugeTLB pages; needs {required}. '
            'Ask the lab owner to provision this node; no global hugepage settings are changed')
    return dict(free_pages_before=free,required_pages=required,page_bytes=2**21)


class Memory:
    def __init__(self, capacity, node, page_mode='native'):
        self.capacity, self.node = allocation_geometry(capacity,page_mode), node
        self.page_mode=page_mode
        self.page_evidence=dict(mode=page_mode,backing='anonymous_dram',hugetlb=page_mode=='huge2MB',
                               allocation_bytes=self.capacity,rnic_page_size_bytes=None,
                               rnic_page_size_status='UNVERIFIED_DRIVER_SELECTED')
        flags=mmap.MAP_SHARED | mmap.MAP_ANONYMOUS
        if page_mode=='huge2MB':
            self.page_evidence.update(hugepage_capacity(node,self.capacity))
            # Linux MAP_HUGETLB | (log2(2 MiB) << MAP_HUGE_SHIFT). Never use on DAX.
            flags |= 0x40000 | (21 << 26)
        self.memory = mmap.mmap(-1, self.capacity, flags=flags,
                                prot=mmap.PROT_READ | mmap.PROT_WRITE)
        try:
            self.address = ctypes.addressof(ctypes.c_char.from_buffer(self.memory))
            bind_memory(self.address, self.capacity, node)
            ctypes.memset(self.address, 0x5A, self.capacity)  # First touch, outside measurements.
            self.placement = resident_pages(self.address, node, self.capacity)
            if page_mode=='huge2MB':
                require(self.address % 2**21==0 and self.placement['kernel_page_bytes']==2**21,
                        'HugeTLB allocation/page evidence mismatch; no fallback')
            self.page_evidence.update(self.placement)
        except BaseException:
            self.memory.close()  # No engine registration exists yet.
            raise

    def digest(self, length):
        require(0 < length <= self.capacity, 'digest outside DRAM window')
        digest = hashlib.sha256()
        for offset in range(0, length, 1024**2):
            digest.update(self.memory[offset:min(length, offset + 1024**2)])
        return digest.hexdigest()


def snapshot(placement):
    counters = {}
    for device in placement['netdevs']:
        root = Path('/sys/class/net') / device / 'statistics'
        counters[device] = {k: int((root / k).read_text()) for k in ('tx_bytes', 'rx_bytes')}
    try:
        probe = subprocess.run(['rdma', 'resource', 'show', 'qp'], capture_output=True,
                               text=True, timeout=5)
        qps = dict(exit_code=probe.returncode, stdout=probe.stdout, stderr=probe.stderr)
    except (OSError, subprocess.TimeoutExpired) as error:
        qps = dict(status='UNAVAILABLE', error=str(error))
    return dict(monotonic_ns=time.monotonic_ns(), netdev=counters, qp_probe=qps)


class Receiver:
    def __init__(self, memory, segment, node_id, peer_id, fingerprint, placement, output):
        self.memory, self.output = memory, Path(output)
        self.lock = threading.Lock()
        self.state, self.attempt, self.sealed = 'IDLE', None, False
        self.seen = set()
        self.description = dict(node_id=node_id, peer_id=peer_id, fingerprint=fingerprint,
                                epoch=secrets.token_hex(16), segment=segment,
                                capacity=memory.capacity, memory='dram', placement=placement,
                                configured_qps=16, active_qps_verified=False, source_identity=source_identity())
        self.description['mr_pages']=getattr(memory,'page_evidence',dict(mode='native'))

    def handle(self, method, d):
        with self.lock:
            if method == 'health':
                return dict(self.description, state=self.state,sealed=self.sealed)
            if method == 'seal':
                require(self.state=='IDLE','active/quarantined receiver cannot be sealed or stopped')
                self.sealed=True
                return dict(sealed=True,state='IDLE',epoch=self.description['epoch'])
            if method == 'prepare':
                require(not self.sealed and self.state == 'IDLE', 'receiver is busy or quarantined')
                require(d['fingerprint'] == self.description['fingerprint'] and
                        d['sender'] == self.description['peer_id'], 'wrong benchmark peer/config')
                identity, length = d['attempt'], d['length']
                require(isinstance(identity, str) and len(identity) == 32 and
                        all(c in '0123456789abcdef' for c in identity), 'invalid attempt')
                require(identity not in self.seen and len(self.seen) < 4096, 'old attempt or ledger full')
                require(type(length) is int and 0 < length <= self.memory.capacity, 'target out of bounds')
                self.seen.add(identity)
                ctypes.memset(self.memory.address, 0, length)
                self.attempt = dict(attempt=identity, length=length, checksum=d['checksum'])
                self.state = 'ACTIVE'
                self.before = snapshot(self.description['placement'])
                return dict(self.description, attempt=identity, address_kind='classic_va',
                            remote_base=self.memory.address, length=length)
            require(self.attempt and d['attempt'] == self.attempt['attempt'], 'unknown/stale attempt')
            if method == 'complete':
                require(self.state == 'ACTIVE' and d.get('sender_drained') is True,
                        'completion needs current sender drain evidence')
                # One cooperating sender uses this target; native wait completes before this RPC.
                try:
                    checksum = self.memory.digest(self.attempt['length'])
                    require(checksum == self.attempt['checksum'], 'final DRAM checksum mismatch')
                    placement = resident_pages(self.memory.address, self.memory.node, self.memory.capacity)
                    result = dict(status='PASS', attempt=self.attempt['attempt'], checksum=checksum,
                                  placement=placement, before=self.before,
                                  after=snapshot(self.description['placement']))
                    save(self.output / f"receiver-{d['attempt']}.json", result)
                except BaseException:
                    self.state = 'QUARANTINED'
                    raise
                self.state = 'IDLE'
                return result
            if method == 'quarantine':
                self.state = 'QUARANTINED'
                return dict(state=self.state)
            raise RuntimeError('unsupported DRAM benchmark method')

    def seal_if_idle(self):
        with self.lock:
            if self.state != 'IDLE':
                return False
            self.sealed = True
            return True


def sweep(engine, memory, c, node, peer, placement, args, output, safety=None):
    safety = safety if safety is not None else {}
    peer_url = f"http://{urlparse(peer['url']).hostname}:{args.control_port}"
    health = rpc(peer_url, 'health')
    page_mode=getattr(args,'page_mode','native')
    require(health['node_id'] == peer['id'] and health['peer_id'] == node['id'] and
            health['fingerprint'] == c['fingerprint'] and health['memory'] == 'dram' and
            health['configured_qps'] == 16 and health['source_identity'] == source_identity(),
            'wrong DRAM receiver or mismatched benchmark source')
    require(health.get('mr_pages',{}).get('mode','native')==page_mode,
            'DRAM sender/receiver page mode mismatch; no mixed-backing result')
    if page_mode=='huge2MB':
        for pages in (health['mr_pages'],getattr(memory,'page_evidence',{})):
            require(pages.get('hugetlb') is True and pages.get('kernel_page_bytes')==2**21,
                    'HugeTLB sender/receiver allocation evidence missing; no WRITE permitted')
    results = []
    for size in args.sizes:
        length = block_bytes(size, args.unit)
        require(length <= health['capacity'], 'receiver window is smaller than the requested block')
        expected = memory.digest(length)
        for index in range(args.repetitions):
            identity = secrets.token_hex(16)
            protocol_wall_start = time.time_ns()
            protocol_start = time.monotonic_ns()
            # A lost prepare reply might still authorize a target. Do not auto-retry.
            safety['exposed'] = True
            target = rpc(peer_url, 'prepare', dict(attempt=identity, length=length,
                         checksum=expected, sender=node['id'], fingerprint=c['fingerprint']))
            require(target['epoch'] == health['epoch'] and target['length'] == length and
                    target['attempt'] == identity and target['node_id'] == peer['id'] and
                    target['segment'] == health['segment'] and
                    type(target['remote_base']) is int and target['remote_base'] > 0 and
                    target['address_kind'] == 'classic_va', 'stale/invalid advertised target')
            def write(source, destination, count):
                # Match the CXL loop adapter: native preflight guarantees one result per range.
                return engine.relay_write_batch_drained([target['segment']], [source],
                                                       [destination], [count])[0]
            before = snapshot(placement)
            for _ in range(args.warmup_blocks):
                require(write(memory.address, target['remote_base'], length) == length,
                        'short warmup WRITE')
            samples = []
            try:
                result = write_loop(write, memory.address, target['remote_base'], length,
                                    getattr(args,'smoke_bytes',0) or TOTAL_BYTES, length,samples)
                observed=dict(result,status='PENDING_VALIDATION',attempt=identity,path_mode='dram_rdma_write')
                save(output/f'observed-{identity}.json',observed)
                write_request_csv(output/f'observed-{identity}.csv',observed)
            except BaseException:
                try:
                    partial=dict(status='FAIL',attempt=identity,requests=samples,
                                 path_mode='dram_rdma_write',block_bytes=length)
                    save(output/f'failed-{identity}.json',partial)
                    write_request_csv(output/f'failed-{identity}.csv',partial)
                except OSError:
                    pass
                raise
            after = snapshot(placement)
            local_placement = resident_pages(memory.address, memory.node, memory.capacity)
            verified = rpc(peer_url, 'complete', dict(attempt=identity, sender_drained=True), timeout=300)
            require(verified['checksum'] == expected, 'receiver validation failed')
            safety['exposed'] = False
            result['protocol_e2e_ns'] = time.monotonic_ns()-protocol_start
            result['controller_start_unix_ns'] = protocol_wall_start
            result['controller_end_unix_ns'] = time.time_ns()
            result.update(status='PASS', evidence_class='TRANSFER_ONLY', path_mode='dram_rdma_write',
                actual_transport='rdma', opcode='WRITE', direction=f"{node['id']}->{peer['id']}",
                block_unit=args.unit, declared_size=size, repetition=index, attempt=identity,
                warmup_blocks=args.warmup_blocks, warmup_bytes=args.warmup_blocks*length,
                batch_cardinality=1, configured_qps=16, active_qps_verified=False,
                GBps=result['bytes']/result['sender_write_loop_ns'],
                GiBps=result['bytes']/(1024**3)/(result['sender_write_loop_ns']/1e9),
                checksum_scope='final reused block window', receiver=verified,
                sender_before=before, sender_after=after, sender_placement=local_placement,
                store_compatible=False, gpu_delivery_proven=False, serving_proven=False)
            result.update(source=node['id'],receiver_node=peer['id'],mr_page_mode=page_mode,
                          measurement_kind='CORRECTNESS_SMOKE' if getattr(args,'smoke_bytes',0) else 'PERFORMANCE',
                          mr_pages={node['id']:getattr(memory,'page_evidence',None),
                                    peer['id']:health.get('mr_pages')})
            result = enrich_trial(result)
            save(output / f'block-{size}{args.unit}-rep-{index}.json', result)
            write_request_csv(output/f'requests-{size}{args.unit}-rep-{index}.csv',result,len(results))
            results.append(result)
            print(f"[PASS] {size} {args.unit} rep={index}: {result['GBps']:.3f} GB/s", flush=True)
    return results


def retain_uncertain(error, output):
    try:
        save(output / 'QUARANTINED.json', dict(status='FAIL', error=str(error),
             action='Retain both processes, mappings and registrations; no automatic retry or reclaim.'))
    except OSError:
        pass  # A failed evidence write must not release a possibly live DMA buffer.
    try:
        print('[QUARANTINED] Possible remote writers remain. Process and memory retained.', flush=True)
    except OSError:
        pass
    # No signal/timeout is treated as DMA drain. External SIGKILL is not handled.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    threading.Event().wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=['receiver', 'sender'], required=True)
    parser.add_argument('--config', default=os.environ.get('CONFIG'), required=not os.environ.get('CONFIG'))
    parser.add_argument('--node', default=os.environ.get('NODE_ID'), required=not os.environ.get('NODE_ID'))
    parser.add_argument('--peer', required=True)
    parser.add_argument('--setup-run', required=True)
    parser.add_argument('--token-file', default=os.environ.get('TOKEN_FILE', 'control.token'))
    parser.add_argument('--control-port', type=int, default=19201)
    parser.add_argument('--te-port', type=int, default=19401)
    parser.add_argument('--numa-node', type=int)
    parser.add_argument('--page-mode',choices=['native','huge2MB'],default='native')
    parser.add_argument('--sizes', nargs='+', type=int, choices=SIZES, default=list(SIZES))
    parser.add_argument('--unit', choices=['MB', 'MiB'], default='MiB')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--warmup-blocks', type=int, default=0)
    parser.add_argument('--smoke-bytes',type=int,default=0,
                        help='Explicit correctness-only byte count; not a performance trial')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output)
    stage = 'configuration'
    service = None
    output_created = False
    try:
        require(sys.platform == 'linux', 'real DRAM RDMA benchmark requires Linux')
        require(0 < args.repetitions <= 100 and args.warmup_blocks >= 0 and
                len(set(args.sizes)) == len(args.sizes), 'invalid sweep settings')
        require(args.smoke_bytes>=0 and (not args.smoke_bytes or
                (len(args.sizes)==1 and args.repetitions==1 and args.warmup_blocks==0
                 and args.smoke_bytes<=block_bytes(args.sizes[0],args.unit))), 'invalid correctness-only volume')
        require(1 <= args.control_port <= 65535 and 1 <= args.te_port <= 65535,
                'invalid benchmark port')
        receipt = setup_environment(args.setup_run)
        c = config(args.config)
        require(c['mode'] == 'rdma', 'DRAM control requires an RDMA manifest')
        nodes = {n['id']: n for n in c['nodes']}
        node, peer = nodes[args.node], nodes[args.peer]
        require(node['id'] != peer['id'] and node['pool'] != peer['pool'], 'select a cross-pool pair')
        require(node['can_receive' if args.role == 'receiver' else 'can_send'], 'capability disabled')
        control = f"http://{urlparse(node['url']).hostname}:{args.control_port}"
        occupied = {urlparse(c['master']).port, *(urlparse(n['url']).port for n in c['nodes'])}
        te_ports = {int(n['te_endpoint'].rsplit(':', 1)[1]) for n in c['nodes']}
        require(args.control_port not in occupied | te_ports and args.te_port not in occupied | te_ports
                and args.control_port != args.te_port, 'benchmark ports overlap campaign ports')
        secret = Path(args.token_file).read_text().strip()
        require(len(secret) >= 32 and Path(args.token_file).stat().st_mode & 0o077 == 0,
                'shared private token needs >=32 characters and mode 0600')
        os.environ['CALLOSUM_CONTROL_TOKEN'] = secret
        os.environ.setdefault('MC_NUM_QP_PER_EP', '16')
        require(os.environ['MC_NUM_QP_PER_EP'] == '16', 'benchmark requires 16 configured QPs')
        capacity = allocation_geometry(max(block_bytes(n, args.unit) for n in args.sizes),args.page_mode)
        soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        if soft!=resource.RLIM_INFINITY and soft<capacity and (hard==resource.RLIM_INFINITY or hard>=capacity):
            resource.setrlimit(resource.RLIMIT_MEMLOCK,(capacity,hard))
            soft,hard=resource.getrlimit(resource.RLIMIT_MEMLOCK)
        require(soft == resource.RLIM_INFINITY or soft >= capacity, 'raise memlock before registration')
        output.mkdir(parents=True, exist_ok=False)
        output_created = True
        if args.role == 'receiver':
            stage = 'control-port-preflight'
            parsed = urlparse(control)
            with socket.socket() as probe:
                probe.bind((parsed.hostname, parsed.port))
        stage = 'numa-preflight'
        placement = nic_placement(node['rdma_device'], args.numa_node)
        stage = 'native-origin'
        import mooncake.engine as engine_module
        import callosum_relay_native as native
        require(native.ABI_VERSION == 2, 'native relay ABI mismatch')
        require(Path(native.__file__).resolve().parent == Path(os.environ['CALLOSUM_NATIVE_BUILD']).resolve(),
                'stale native module')
        require(Path(engine_module.__file__).resolve() ==
                (Path(receipt['build'])/'python/mooncake/engine.so').resolve(), 'stale engine module')
        stage = 'dram-allocation'
        memory = (Memory(capacity, placement['memory_numa_node']) if args.page_mode=='native' else
                  Memory(capacity, placement['memory_numa_node'],args.page_mode))
        stage = 'engine-registration'
        engine = engine_module.TransferEngine()
        require(engine.relay_abi_version() == 2, 'engine relay ABI mismatch')
        endpoint = node['te_endpoint'].rsplit(':', 1)[0] + ':' + str(args.te_port)
        require(engine.initialize(endpoint, 'P2PHANDSHAKE', 'rdma', node['rdma_device']) == 0,
                'engine initialization failed')
        require(engine.relay_rdma_available(), 'RDMA unavailable; no TCP fallback')
        require(engine.register_memory(memory.address, capacity) == 0, 'DRAM registration failed')
        segment = endpoint.rsplit(':', 1)[0] + ':' + str(engine.get_rpc_port())
        save(output/'environment.json', dict(node=node['id'], peer=peer['id'], role=args.role, pid=os.getpid(),
             fingerprint=c['fingerprint'], host=socket.gethostname(), placement=placement,
             residency=memory.placement, mr_pages=getattr(memory,'page_evidence',None),
             capacity=capacity, memlock=[soft, hard],
             configured_qps=16, active_qps_verified=False, setup_run=str(Path(args.setup_run).resolve()),
             modules={str(Path(m.__file__).resolve()): hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()
                      for m in (engine_module, native)},
             software_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
             loop_sha256=hashlib.sha256(Path(__file__).with_name('callosum_transport_benchmark.py').read_bytes()).hexdigest(),
             transport_env={k:os.environ.get(k) for k in ('MC_MTU','MC_GID_INDEX','MC_NUM_QP_PER_EP')}))
        if args.role == 'receiver':
            service = Receiver(memory, segment, node['id'], peer['id'], c['fingerprint'], placement, output)
            http = server(control, service)
            stopped = threading.Event()
            signal.signal(signal.SIGINT, lambda *_: stopped.set())
            signal.signal(signal.SIGTERM, lambda *_: stopped.set())
            signal.signal(signal.SIGHUP, lambda *_: stopped.set())
            threading.Thread(target=http.serve_forever, daemon=True).start()
            print(f'[READY] DRAM receiver {node["id"]} at {control}; evidence={output}', flush=True)
            while True:
                stopped.wait()
                if service.seal_if_idle():
                    http.shutdown()
                    http.server_close()
                    break
                print('[RETAINED] Receiver has an active/quarantined target; do not kill.', flush=True)
                stopped.clear()
        else:
            stage = 'sender-sweep'
            safety = {}
            def interrupted(*_):
                raise InterruptedError('sender interrupted; inspect target lifetime')
            signal.signal(signal.SIGTERM, interrupted)
            signal.signal(signal.SIGHUP, interrupted)
            try:
                results = sweep(engine, memory, c, node, peer, placement, args, output, safety)
                report = write_report(output,results)
                save(output/'summary.json', dict(status='PASS', evidence_class='TRANSFER_ONLY',
                     path_mode='dram_rdma_write', results=results, active_qps_verified=False,
                     report=report,interpretation='Serial drained WRITE control; no saturation or serving claim.'))
                print((output/'report.md').read_text(),flush=True)
            except BaseException as error:
                if safety.get('exposed'):
                    retain_uncertain(error, output)
                raise
        # Only normal, drained completion permits local unregister/free.
        require(engine.unregister_memory(memory.address) == 0, 'DRAM unregister failed')
        memory.memory.close()
    except Exception as error:
        if service is not None and not service.seal_if_idle():
            retain_uncertain(error, output)
        failure = dict(status='FAIL', stage=stage, error=str(error), evidence=str(output))
        if output_created and not (output/'failure.json').exists():
            save(output/'failure.json', failure)
        print(json.dumps(failure, indent=2), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
