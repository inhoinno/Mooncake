#!/usr/bin/env python3
"""Native NUMA-memory RDMA WRITE controls and bounded pipeline. No DAX mmap or Store access."""
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
from callosum_transport_benchmark import SIZES, TOTAL_BYTES, block_bytes, write_loop, ACCESS_PATTERNS, access_strategy, written_ranges, final_window_digest
from callosum_transport_benchmark import diagnostic_write_loop, rdma_snapshot, rdma_inventory, sample_rdma, rdma_trial_progress, pipeline_write_loop, sampled_slice_latencies
from callosum_benchmark_report import write_report, write_request_csv, enrich_trial

NUMA_BACKINGS = {'dram': 'anonymous_dram', 'cxl-system-ram': 'anonymous_cxl_system_ram'}


def load_profile(path):
    """Shared configuration for the existing launcher's native 1:1 workflow."""
    p = json.loads(Path(path).read_text())
    require(isinstance(p,dict) and all(isinstance(p.get(k),dict)
            for k in ('nodes','benchmark','handoff')),'JSON requires nodes, benchmark and handoff objects')
    require(all(isinstance(n,dict) for n in p['nodes'].values()),'node settings must be objects')
    errors = []
    def check(ok, message):
        if not ok: errors.append(message)
    check(p.get('schema_version') == 1 and p.get('experiment') in ('cxl_ddr_1to1','numa_rdma_1to1'),
          'expected schema_version=1, experiment=numa_rdma_1to1 (or legacy cxl_ddr_1to1)')
    source, receiver = p.get('source'), p.get('receiver')
    nodes = p.get('nodes',{})
    check(source in ('a1','a2','b1','b2') and receiver in ('a1','a2','b1','b2') and
          source != receiver and set(nodes) == {source,receiver}, 'select exactly two distinct endpoint IDs')
    from ipaddress import IPv4Address
    import re
    for name,n in nodes.items():
        try: IPv4Address(n.get('ip',''))
        except (ValueError,TypeError): errors.append(f'{name}.ip must be IPv4')
        check(isinstance(n.get('rdma_device'),str) and bool(re.fullmatch(r'[A-Za-z0-9_.-]+',n['rdma_device'])),
              f'{name}.rdma_device is required')
        check(type(n.get('numa_node')) is int and n['numa_node'] >= 0,
              f'{name}.numa_node must be an inventoried memory NUMA node number')
        backing = n.get('backing','cxl-system-ram' if p.get('experiment')=='cxl_ddr_1to1' else None)
        check(backing in NUMA_BACKINGS,f'{name}.backing must be dram or cxl-system-ram')
        check(p.get('experiment')!='cxl_ddr_1to1' or backing=='cxl-system-ram',
              'legacy cxl_ddr_1to1 cannot label ordinary DRAM as CXL; use numa_rdma_1to1')
    check(len({str(n.get('ip')) for n in nodes.values()}) == 2,'two distinct host IPs required')
    b = p.get('benchmark',{})
    sizes = b.get('sizes_mib',[])
    check(isinstance(sizes,list) and bool(sizes) and all(type(s) is int and s in SIZES for s in sizes) and
          len(set(sizes)) == len(sizes),'sizes_mib must be unique supported powers of two')
    for key,low,high in (('repetitions',1,4),('pipeline_depth',1,4096),
                         ('descriptors_per_submit',1,1024),('warmup_blocks',0,16),
                         ('control_port',1,65535),('te_port',1,65535),('random_seed',0,2**63-1)):
        check(type(b.get(key)) is int and low <= b[key] <= high,f'benchmark.{key} outside bounds')
    for key in ('window_bytes','pipeline_max_bytes'):
        check(type(b.get(key)) is int and 4096 <= b[key] <= 1<<40 and b[key]%4096 == 0,
              f'benchmark.{key} must be positive page-aligned bytes')
    if sizes and all(type(s) is int for s in sizes):
        check(all(type(b.get(k)) is int and b[k] >= max(sizes)*(1<<20)
                  for k in ('window_bytes','pipeline_max_bytes')),'window and byte credit must fit the largest block')
    check(b.get('control_port') != b.get('te_port'),'control and TE ports must differ')
    check(b.get('access_pattern') in ACCESS_PATTERNS,'unsupported access_pattern')
    check(b.get('instrumentation') in ('off','counters','sampled'),'unsupported instrumentation')
    check(type(b.get('sample_queues')) is bool and
          (not b['sample_queues'] or b.get('instrumentation') != 'off'),'queue samples need counters/sampled')
    env = p.get('transport_env',{})
    require(isinstance(env,dict),'transport_env must be an object')
    check(set(env) <= {'MC_MTU','MC_GID_INDEX'},'transport_env supports only explicit MC_MTU/MC_GID_INDEX')
    check(all(isinstance(v,str) and v.isdecimal() for v in env.values()),'transport settings must be decimal strings')
    if 'MC_MTU' in env:
        check(env['MC_MTU'] in ('512','1024','2048','4096'),'MC_MTU must be 512/1024/2048/4096')
    if 'MC_GID_INDEX' in env:
        check(isinstance(env['MC_GID_INDEX'],str) and env['MC_GID_INDEX'].isdecimal() and
              int(env['MC_GID_INDEX']) < 256,'MC_GID_INDEX must be 0..255')
    h = p.get('handoff',{})
    check(isinstance(h.get('ssh_target'),str) and bool(re.fullmatch(r'[A-Za-z0-9_.@-]+',h['ssh_target'])) and
          not h['ssh_target'].startswith('-'),'handoff.ssh_target must be an explicit authorized SSH target')
    check(type(h.get('ssh_port')) is int and 1 <= h['ssh_port'] <= 65535,'handoff.ssh_port outside bounds')
    check(isinstance(h.get('checkout'),str) and bool(re.fullmatch(r'[A-Za-z0-9_./-]+',h['checkout'])) and
          not Path(h['checkout']).is_absolute() and '..' not in Path(h['checkout']).parts,
          'handoff.checkout must be a safe path relative to the peer home')
    check(set(p) <= {'schema_version','experiment','source','receiver','nodes','benchmark','transport_env','handoff'},
          'unknown profile field')
    check(set(b) <= {'sizes_mib','repetitions','window_bytes','access_pattern','random_seed',
                    'pipeline_depth','pipeline_max_bytes','descriptors_per_submit','instrumentation',
                    'sample_queues','warmup_blocks','control_port','te_port'},'unknown benchmark field')
    check(set(h) <= {'ssh_target','ssh_port','checkout'},'unknown handoff field')
    check(all(set(n) <= {'ip','rdma_device','numa_node','backing'} for n in nodes.values()),'unknown node field')
    require(not errors,'Invalid NUMA transport JSON: ' + '; '.join(errors))
    return p


def profile_transport(p):
    """Normalize endpoints only; do not invent DAX paths, pools or a master."""
    b = p['benchmark']
    return dict(mode='rdma',numa_profile=True,
        fingerprint=hashlib.sha256(json.dumps(p,sort_keys=True).encode()).hexdigest(),
        nodes=[dict(id=k,pool=k,url=f"http://{n['ip']}:{b['control_port']}",
                    te_endpoint=f"{n['ip']}:{b['te_port']}",rdma_device=n['rdma_device'],
                    can_send=k==p['source'],can_receive=k==p['receiver']) for k,n in p['nodes'].items()])


def numa_path(sender, receiver):
    return 'dram_rdma_write' if sender==receiver=='dram' else 'cxl_system_ram_rdma_write'


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


def nic_placement(device, override=None, sysroot=Path('/sys'), *, require_remote=False, apply_cpu=True,
                  backing='dram'):
    require(backing in NUMA_BACKINGS and (backing=='dram' or require_remote),
            'CXL system RAM requires an explicit remote NUMA placement')
    require(device and '/' not in device and device not in ('.', '..'), 'invalid RDMA device')
    base = sysroot / 'class/infiniband' / device / 'device'
    nic_node = int((base / 'numa_node').read_text())
    require((override is not None and not require_remote) or nic_node >= 0,
            'RNIC NUMA node is unknown; cannot prove remote DRAM placement' if require_remote else
            'NIC NUMA node is unknown; supply an explicitly reviewed --numa-node')
    node = nic_node if override is None else override
    require(type(node) is int and node >= 0, 'invalid NUMA node')
    targets = []
    if require_remote:
        require(override is not None and node != nic_node,
                f'Remote DRAM requires an explicit memory node different from RNIC NUMA {nic_node}')
        meminfo = (sysroot/f'devices/system/node/node{node}/meminfo').read_text().splitlines()
        total = next((int(line.split()[-2]) for line in meminfo if 'MemTotal:' in line),0)
        require(total>0,f'NUMA {node} has no online memory')
        dax = json.loads(subprocess.check_output(['daxctl','list'],text=True))
        def dax_targets(value):
            if isinstance(value,list):
                for child in value:
                    yield from dax_targets(child)
            elif isinstance(value,dict):
                if 'chardev' in value and value.get('target_node')==node:
                    yield {k:value[k] for k in ('chardev','target_node','mode') if k in value}
                for child in value.values():
                    yield from dax_targets(child)
        targets = list(dax_targets(dax))
        if backing=='dram':
            require(not targets, f'NUMA {node} is CXL/DAX-backed; select ordinary DRAM or --backing cxl-system-ram')
        else:
            require(targets and all(t.get('mode')=='system-ram' for t in targets),
                    f'NUMA {node} is not a verified system-ram DAX target; do not reconfigure the device from this benchmark')
    allowed = os.sched_getaffinity(0)
    cpu_node = nic_node if require_remote else node
    local = cpulist((sysroot / f'devices/system/node/node{cpu_node}/cpulist').read_text()) & allowed
    require(local, 'NIC-local CPUs are excluded by this process cpuset')
    if apply_cpu:
        os.sched_setaffinity(0, local)
        require(os.sched_getaffinity(0) == local, 'CPU affinity was not applied')
    netdevs = sorted(p.name for p in (base / 'net').iterdir())
    require(netdevs, 'no netdev found for selected RDMA device')
    return dict(rdma_device=device, nic_numa_node=nic_node, memory_numa_node=node,
                numa_override=override is not None, cpu_numa_node=cpu_node,
                cpus=sorted(local), netdevs=netdevs, memory_backing=backing, dax_targets=targets)


def require_remote_pages(placement, pages, capacity):
    nic, mem = (placement.get(k) for k in ('nic_numa_node','memory_numa_node'))
    require(type(nic) is int and type(mem) is int and min(nic,mem)>=0 and nic!=mem and
            placement.get('cpu_numa_node')==nic, 'expected NIC-local CPUs and NIC-remote DRAM or CXL system RAM')
    backing = placement.get('memory_backing','dram')
    require(backing in NUMA_BACKINGS, 'unrecognized NUMA backing evidence')
    if backing=='cxl-system-ram':
        targets = placement.get('dax_targets',[])
        require(targets and all(t.get('mode')=='system-ram' and t.get('target_node')==mem for t in targets),
                'missing matching CXL system-ram target evidence')
    counts = {str(k):v for k,v in pages.get('pages_by_node',{}).items()}
    page = pages.get('kernel_page_bytes',0)
    require(type(capacity) is int and capacity>0 and
            pages.get('backing',NUMA_BACKINGS[backing])==NUMA_BACKINGS[backing] and type(page) is int and page>0 and
            counts.get(str(mem),0)*page>=capacity and all(k==str(mem) or not v for k,v in counts.items()),
            f'MR is not fully resident on remote NUMA node {mem}')


def pair_barrier(directory, phase, ordinal, node, peer, timeout=300):
    """Local sender rendezvous; an abort never releases an exposed target."""
    directory = Path(directory)
    deadline = time.monotonic()+timeout
    (directory/f'{phase}-{ordinal}-{node}').touch(exist_ok=False)
    while True:
        require(not (directory/'abort').exists(), 'paired DRAM run aborted; no next WRITE')
        if (directory/f'{phase}-{ordinal}-{peer}').is_file():
            return
        require(time.monotonic()<deadline, f'paired DRAM rendezvous timed out: {phase} {ordinal}')
        time.sleep(.01)


def bind_memory(address, length, node):
    require(type(node) is int and node>=0,'invalid NUMA binding node')
    name = ctypes.util.find_library('numa')
    require(name, 'libnuma is required; install the lab OS libnuma package')
    lib = ctypes.CDLL(name, use_errno=True)
    word = ctypes.sizeof(ctypes.c_ulong) * 8
    # Include a zero sentinel bit above the selected node. Older kernels subtract
    # one from maxnode before importing the mask; node+1 can drop our only bit.
    mask = (ctypes.c_ulong * ((node + 1) // word + 1))()
    mask[node // word] = 1 << (node % word)
    lib.mbind.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_int,
                         ctypes.POINTER(ctypes.c_ulong), ctypes.c_ulong, ctypes.c_uint]
    lib.mbind.restype = ctypes.c_long
    # MPOL_BIND, before first touch. Do not migrate unrelated process pages.
    maxnode=len(mask)*word
    if lib.mbind(address, length, 2, mask, maxnode, 0) != 0:
        raise OSError(ctypes.get_errno(),
            f'mbind failed; node={node}, length={length}, address_page_offset={address % mmap.PAGESIZE}, '
            f'maxnode={maxnode}, mask_words={len(mask)}; no NUMA fallback')


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
            'Repeat prepare for bounded provisioning on this node; run never changes HugeTLB settings')
    return dict(free_pages_before=free,required_pages=required,page_bytes=2**21)


class Memory:
    def __init__(self, capacity, node, page_mode='native', *, backing='dram'):
        self.capacity, self.node = allocation_geometry(capacity,page_mode), node
        self.page_mode=page_mode
        self.page_evidence=dict(mode=page_mode,backing=NUMA_BACKINGS[backing],hugetlb=page_mode=='huge2MB',
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
    def __init__(self, memory, segment, node_id, peer_id, fingerprint, placement, output, peer_backing='dram'):
        self.memory, self.output = memory, Path(output)
        self.lock = threading.Lock()
        self.state, self.attempt, self.sealed = 'IDLE', None, False
        self.completed_trials, self.finished = 0, False
        self.seen = set()
        self.description = dict(node_id=node_id, peer_id=peer_id, fingerprint=fingerprint,
                                epoch=secrets.token_hex(16), segment=segment,
                                capacity=memory.capacity, memory=placement.get('memory_backing','dram'),
                                peer_backing=peer_backing, placement=placement,
                                transport_env={k:os.environ.get(k) for k in ('MC_MTU','MC_GID_INDEX')},
                                configured_qps=16, active_qps_verified=False, source_identity=source_identity())
        self.description['mr_pages']=getattr(memory,'page_evidence',dict(mode='native'))
        self.description['access_patterns']=list(ACCESS_PATTERNS)

    def handle(self, method, d):
        with self.lock:
            if method == 'health':
                return dict(self.description, state=self.state,sealed=self.sealed)
            if method == 'seal':
                require(self.state=='IDLE','active/quarantined receiver cannot be sealed or stopped')
                self.sealed=True
                return dict(sealed=True,state='IDLE',epoch=self.description['epoch'])
            if method == 'finish':
                require(d.get('fingerprint')==self.description['fingerprint'] and
                        d.get('sender')==self.description['peer_id'] and
                        type(d.get('trials')) is int and d['trials']>0 and
                        self.completed_trials==d['trials'] and self.state=='IDLE',
                        'cleanup requires all expected verified trials and an idle receiver')
                self.sealed, self.finished = True, True
                return dict(sealed=True,state='IDLE',completed_trials=self.completed_trials)
            if method == 'prepare':
                require(not self.sealed and self.state == 'IDLE', 'receiver is busy or quarantined')
                require(d['fingerprint'] == self.description['fingerprint'] and
                        d['sender'] == self.description['peer_id'], 'wrong benchmark peer/config')
                require(d.get('sender_backing','dram')==self.description['peer_backing'],
                        'sender backing differs from receiver configuration')
                identity, length = d['attempt'], d['length']
                require(isinstance(identity, str) and len(identity) == 32 and
                        all(c in '0123456789abcdef' for c in identity), 'invalid attempt')
                require(identity not in self.seen and len(self.seen) < 4096, 'old attempt or ledger full')
                require(type(length) is int and 0 < length <= self.memory.capacity, 'target out of bounds')
                self.seen.add(identity)
                ctypes.memset(self.memory.address, 0, length)
                self.attempt = dict(attempt=identity, length=length, checksum=d['checksum'],
                                    deferred_checksum=d.get('deferred_checksum',False))
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
                    expected=d.get('checksum') if self.attempt['deferred_checksum'] else self.attempt['checksum']
                    require(isinstance(expected,str) and checksum == expected, 'final DRAM checksum mismatch')
                    placement = resident_pages(self.memory.address, self.memory.node, self.memory.capacity)
                    result = dict(status='PASS', attempt=self.attempt['attempt'], checksum=checksum,
                                  placement=placement, before=self.before,
                                  after=snapshot(self.description['placement']))
                    save(self.output / f"receiver-{d['attempt']}.json", result)
                except BaseException:
                    self.state = 'QUARANTINED'
                    raise
                self.state = 'IDLE'
                self.completed_trials += 1
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
    backing=getattr(args,'backing','dram')
    peer_backing=getattr(args,'peer_backing',None) or backing
    path_mode=numa_path(backing,peer_backing)
    require(health['node_id'] == peer['id'] and health['peer_id'] == node['id'] and
            health['fingerprint'] == c['fingerprint'] and
            health['configured_qps'] == 16 and health['source_identity'] == source_identity(),
            'wrong NUMA receiver or mismatched benchmark source')
    require(health['memory']==peer_backing and health.get('peer_backing','dram')==backing,
            'sender/receiver backing mismatch; no WRITE permitted')
    require(health.get('mr_pages',{}).get('mode','native')==page_mode,
            'DRAM sender/receiver page mode mismatch; no mixed-backing result')
    if getattr(args,'require_remote_numa',False):
        require(not health.get('sealed') and health.get('state')=='IDLE',
                'remote DRAM receiver is not idle or is sealed')
        require(health['capacity']==memory.capacity and health['mr_pages'].get('backing')==NUMA_BACKINGS[peer_backing] and
                health['placement'].get('memory_backing','dram')==peer_backing and
                memory.page_evidence.get('backing')==NUMA_BACKINGS[backing] and
                placement.get('memory_backing','dram')==backing,
                'receiver or sender registration differs from the expected backing and size')
        require_remote_pages(health['placement'],health['mr_pages'],health['capacity'])
        require_remote_pages(placement,memory.page_evidence,memory.capacity)
    if page_mode=='huge2MB':
        for pages in (health['mr_pages'],getattr(memory,'page_evidence',{})):
            require(pages.get('hugetlb') is True and pages.get('kernel_page_bytes')==2**21,
                    'HugeTLB sender/receiver allocation evidence missing; no WRITE permitted')
    results = []
    pattern=getattr(args,'access_pattern','overwrite')
    requested_window=getattr(args,'access_window_bytes',None)
    seed=getattr(args,'random_seed',0)
    if pattern!='overwrite' or requested_window is not None:
        require(pattern in health.get('access_patterns',[]),'receiver lacks access-strategy support')
    for size in args.sizes:
        length = block_bytes(size, args.unit)
        window=requested_window or (length if pattern=='overwrite' else
                                    max(block_bytes(n,args.unit) for n in args.sizes))
        access_strategy(pattern,length,window,seed)
        require(window<=min(memory.capacity,health['capacity']),'access window exceeds existing DRAM MR; no resize')
        require(length <= health['capacity'], 'receiver window is smaller than the requested block')
        expected = memory.digest(length)
        requested_mode=getattr(args,'submission_mode',None)
        modes=['serialization','batched'] if requested_mode=='compare' else [requested_mode]
        for index,mode in ((i,m) for i in range(args.repetitions) for m in modes):
            identity = secrets.token_hex(16)
            protocol_wall_start = time.time_ns()
            protocol_start = time.monotonic_ns()
            # A lost prepare reply might still authorize a target. Do not auto-retry.
            safety['exposed'] = True
            target = rpc(peer_url, 'prepare', dict(attempt=identity, length=window,
                         deferred_checksum=window!=length or pattern!='overwrite',
                         checksum=expected, sender=node['id'], fingerprint=c['fingerprint'],sender_backing=backing))
            require(target['epoch'] == health['epoch'] and target['length'] == window and
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
            tracking = bool(getattr(args,'sample_queues',False) or (mode and args.pipeline_instrumentation!='off'))
            monitor = None
            if tracking:
                before_rdma = rdma_snapshot(engine)
                require(before_rdma['contexts'] and all(x['tracked'] and x.get('diagnostic_version',0)>=2
                        for x in before_rdma['contexts']),
                        'queue samples require rebuilt diagnostic v2 and MC_TRACK_RDMA_POSTED_SLICES=1 at startup')
                queue, stop = [], threading.Event()
                monitor = threading.Thread(target=sample_rdma,args=(engine,stop,queue)) if getattr(args,'sample_queues',False) else None
            try:
                target_ready = time.monotonic_ns()
                if getattr(args,'paired_gate',None) and not mode:
                    pair_barrier(args.paired_gate,'ready',len(results),node['id'],args.local_partner)
                released = time.monotonic_ns()
                if monitor:
                    monitor.start()
                if mode:
                    def ready():
                        if getattr(args,'paired_gate',None):
                            pair_barrier(args.paired_gate,'ready',len(results),node['id'],args.local_partner)
                    def drained():
                        if getattr(args,'paired_gate',None):
                            pair_barrier(args.paired_gate,'drained',len(results),node['id'],args.local_partner)
                    result=pipeline_write_loop(engine,target['segment'],memory.address,target['remote_base'],
                        length,TOTAL_BYTES,samples,owner=memory,mode=mode,
                        depth=args.pipeline_depth,byte_limit=args.pipeline_max_bytes,
                        descriptors=args.descriptors_per_submit,instrumentation=args.pipeline_instrumentation,
                        access_pattern=pattern,window_bytes=window,random_seed=seed,
                        source_backing='numa-source-'+node['id'],destination_backing='numa-target-'+peer['id'],
                        ready=ready,drained=drained)
                elif getattr(args,'diagnose',None):
                    result = diagnostic_write_loop(engine,target['segment'],memory.address,target['remote_base'],
                        length,TOTAL_BYTES,length,samples,driver=args.diagnose,access_pattern=pattern,
                        window_bytes=window,random_seed=seed)
                else:
                    result = write_loop(write, memory.address, target['remote_base'], length,
                                        getattr(args,'smoke_bytes',0) or TOTAL_BYTES, length,samples,
                                        access_pattern=pattern,window_bytes=window,random_seed=seed)
                completed = time.monotonic_ns()
                if monitor:
                    stop.set()
                    monitor.join()
                if getattr(args,'paired_gate',None) and not mode:
                    pair_barrier(args.paired_gate,'drained',len(results),node['id'],args.local_partner)
                if tracking:
                    after_rdma = rdma_snapshot(engine)
                    result.update(sender_rnic_before=before_rdma,sender_rnic_after=after_rdma,
                        sender_queue_samples=queue,queue_sample_cap_reached=len(queue)==4096,
                        native_queue_progress=rdma_trial_progress(before_rdma,after_rdma,result['bytes']))
                if mode and args.pipeline_instrumentation!='off':
                    result['sender_inventory']=rdma_inventory(engine)
                    if args.pipeline_instrumentation=='sampled':
                        begin=result['sender_loop_start_monotonic_ns']
                        result['sampled_slice_latencies']=sampled_slice_latencies(
                            result['sender_inventory'],begin,begin+result['sender_write_loop_ns'])
                observed=dict(result,status='PENDING_VALIDATION',attempt=identity,path_mode=path_mode,
                              sender_backing=backing,receiver_backing=peer_backing)
                save(output/f'observed-{identity}.json',observed)
                write_request_csv(output/f'observed-{identity}.csv',observed)
            except BaseException:
                if monitor:
                    stop.set()
                    if monitor.ident is not None:
                        monitor.join()
                if getattr(args,'paired_gate',None):
                    (args.paired_gate/'abort').touch()
                try:
                    partial=dict(status='FAIL',attempt=identity,requests=samples,
                                 path_mode=path_mode,block_bytes=length,
                                 sender_backing=backing,receiver_backing=peer_backing)
                    save(output/f'failed-{identity}.json',partial)
                    write_request_csv(output/f'failed-{identity}.csv',partial)
                except OSError:
                    pass
                raise
            after = snapshot(placement)
            validation_start = time.monotonic_ns()
            local_placement = resident_pages(memory.address, memory.node, memory.capacity)
            coverage=written_ranges(samples,window)
            if pattern!='overwrite' or window!=length:
                checksum_ranges=written_ranges(samples+([dict(request_bytes=length)] if args.warmup_blocks else []),window)
                expected=final_window_digest(memory.memory,window,checksum_ranges)
            verified = rpc(peer_url, 'complete', dict(attempt=identity, sender_drained=True,checksum=expected), timeout=300)
            require(verified['checksum'] == expected, 'receiver validation failed')
            safety['exposed'] = False
            if getattr(args,'require_remote_numa',False):
                require_remote_pages(placement,local_placement,memory.capacity)
                require_remote_pages(health['placement'],verified['placement'],health['capacity'])
            result['protocol_e2e_ns'] = time.monotonic_ns()-protocol_start
            result['trial_phase_ns'] = dict(receiver_prepare=target_ready-protocol_start,
                ready_gate=result.get('native_ready_gate_ns',released-target_ready),
                transfer=result['sender_write_loop_ns'], post_transfer_gate_and_evidence=validation_start-completed,
                verification=time.monotonic_ns()-validation_start)
            result['controller_start_unix_ns'] = protocol_wall_start
            result['controller_end_unix_ns'] = time.time_ns()
            result.update(status='PASS', evidence_class='TRANSFER_ONLY', path_mode=path_mode,
                sender_backing=backing,receiver_backing=peer_backing,
                actual_transport='rdma', opcode='WRITE', direction=f"{node['id']}->{peer['id']}",
                block_unit=args.unit, declared_size=size, repetition=index, attempt=identity,
                warmup_blocks=args.warmup_blocks, warmup_bytes=args.warmup_blocks*length,
                batch_cardinality=1, configured_qps=16, active_qps_verified=False,
                GBps=result['bytes']/result['sender_write_loop_ns'],
                GiBps=result['bytes']/(1024**3)/(result['sender_write_loop_ns']/1e9),
                checksum_scope='final reused block window' if pattern=='overwrite' and window==length else 'final access window with zeroed untouched ranges',
                verified_ranges=coverage,receiver=verified,
                sender_before=before, sender_after=after, sender_placement=local_placement,
                store_compatible=False, gpu_delivery_proven=False, serving_proven=False)
            result.update(source=node['id'],receiver_node=peer['id'],mr_page_mode=page_mode,
                          measurement_kind=('CORRECTNESS_SMOKE' if getattr(args,'smoke_bytes',0) else
                                            'DIAGNOSTIC_ONLY' if getattr(args,'diagnose',None) else 'PERFORMANCE'),
                          mr_pages={node['id']:getattr(memory,'page_evidence',None),
                                    peer['id']:health.get('mr_pages')})
            if getattr(args,'require_remote_numa',False):
                result.update(experiment_case=('remote_numa_dram' if path_mode=='dram_rdma_write' else 'remote_numa_cxl_system_ram'),
                              sender_topology=placement,
                              receiver_topology=health['placement'],
                              sender_clock=dict(boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                                                time_namespace=os.readlink('/proc/self/ns/time')))
            result = enrich_trial(result)
            prefix=(mode+'-') if mode else ''
            save(output / f'{prefix}block-{size}{args.unit}-rep-{index}.json', result)
            write_request_csv(output/f'{prefix}requests-{size}{args.unit}-rep-{index}.csv',result,len(results))
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
    parser.add_argument('--require-remote-numa',action='store_true')
    parser.add_argument('--backing',choices=NUMA_BACKINGS,default='dram')
    parser.add_argument('--peer-backing',choices=NUMA_BACKINGS)
    parser.add_argument('--paired-gate',type=Path,help=argparse.SUPPRESS)
    parser.add_argument('--local-partner',help=argparse.SUPPRESS)
    parser.add_argument('--page-mode',choices=['native','huge2MB'],default='native')
    parser.add_argument('--access-pattern',choices=ACCESS_PATTERNS,default='overwrite')
    parser.add_argument('--access-window-bytes',type=int)
    parser.add_argument('--random-seed',type=int,default=0)
    parser.add_argument('--sizes', nargs='+', type=int, choices=SIZES, default=list(SIZES))
    parser.add_argument('--unit', choices=['MB', 'MiB'], default='MiB')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--warmup-blocks', type=int, default=0)
    parser.add_argument('--diagnose',choices=('python','native'))
    parser.add_argument('--sample-queues',action='store_true',help='Diagnostic only; requires native tracking at startup')
    parser.add_argument('--submission-mode',choices=('serialization','batched','compare'))
    parser.add_argument('--pipeline-depth',type=int,default=4)
    parser.add_argument('--pipeline-max-bytes',type=int,default=256<<20)
    parser.add_argument('--descriptors-per-submit',type=int,default=1)
    parser.add_argument('--pipeline-instrumentation',choices=('off','counters','sampled'),default='off')
    parser.add_argument('--smoke-bytes',type=int,default=0,
                        help='Explicit correctness-only byte count; not a performance trial')
    parser.add_argument('--output', required=True)
    parser.add_argument('--auto-cleanup',action='store_true',
                        help='After verified sender report, seal the idle receiver and release owned MRs')
    args = parser.parse_args()
    output = Path(args.output)
    stage = 'configuration'
    service = None
    output_created = False
    try:
        require(sys.platform == 'linux', 'real DRAM RDMA benchmark requires Linux')
        require(not args.sample_queues or args.diagnose or
                (args.submission_mode and args.pipeline_instrumentation!='off'), 'queue samples require diagnostic instrumentation')
        require(not args.submission_mode or (not args.diagnose and not args.smoke_bytes),
                'native comparison uses full payload without legacy diagnostics/smoke')
        require(0<args.pipeline_depth<=4096 and 0<args.pipeline_max_bytes<=1<<40 and
                0<args.descriptors_per_submit<=1024,'invalid pipeline limits')
        if args.submission_mode:
            os.environ['MC_TRACK_RDMA_POSTED_SLICES']='0' if args.pipeline_instrumentation=='off' else '1'
            os.environ['MC_SAMPLE_RDMA_WRITE_TRACE']='1' if args.pipeline_instrumentation=='sampled' else '0'
        require(not args.diagnose or (args.role=='sender' and not args.smoke_bytes and
                not args.warmup_blocks), 'diagnostics require the full sender workload without warmup')
        args.peer_backing = args.peer_backing or args.backing
        require(args.require_remote_numa or args.backing==args.peer_backing=='dram',
                'CXL system RAM requires explicit remote NUMA placement')
        require(args.page_mode=='native' or args.backing==args.peer_backing=='dram',
                'CXL system-RAM comparison supports native pages only')
        require(0 < args.repetitions <= 100 and args.warmup_blocks >= 0 and
                len(set(args.sizes)) == len(args.sizes), 'invalid sweep settings')
        require(args.smoke_bytes>=0 and (not args.smoke_bytes or
                (len(args.sizes)==1 and args.repetitions==1 and args.warmup_blocks==0
                 and args.smoke_bytes<=block_bytes(args.sizes[0],args.unit))), 'invalid correctness-only volume')
        require(1 <= args.control_port <= 65535 and 1 <= args.te_port <= 65535,
                'invalid benchmark port')
        require((args.paired_gate is None)==(args.local_partner is None), 'incomplete paired rendezvous')
        if args.paired_gate:
            require(args.role=='sender' and args.require_remote_numa and args.paired_gate.is_dir() and
                    args.local_partner!=args.node,'invalid paired sender rendezvous')
        receipt = setup_environment(args.setup_run)
        raw = json.loads(Path(args.config).read_text())
        c = (profile_transport(load_profile(args.config))
             if raw.get('experiment') in ('cxl_ddr_1to1','numa_rdma_1to1') else config(args.config))
        require(c['mode'] == 'rdma', 'DRAM control requires an RDMA manifest')
        nodes = {n['id']: n for n in c['nodes']}
        node, peer = nodes[args.node], nodes[args.peer]
        require(node['id'] != peer['id'] and node['pool'] != peer['pool'], 'select a cross-pool pair')
        require(node['can_receive' if args.role == 'receiver' else 'can_send'], 'capability disabled')
        control = f"http://{urlparse(node['url']).hostname}:{args.control_port}"
        occupied = set() if c.get('numa_profile') else {urlparse(c['master']).port, *(urlparse(n['url']).port for n in c['nodes'])}
        te_ports = set() if c.get('numa_profile') else {int(n['te_endpoint'].rsplit(':', 1)[1]) for n in c['nodes']}
        require(args.control_port not in occupied | te_ports and args.te_port not in occupied | te_ports
                and args.control_port != args.te_port, 'benchmark ports overlap campaign ports')
        secret = Path(args.token_file).read_text().strip()
        require(len(secret) >= 32 and Path(args.token_file).stat().st_mode & 0o077 == 0,
                'shared private token needs >=32 characters and mode 0600')
        os.environ['CALLOSUM_CONTROL_TOKEN'] = secret
        os.environ.setdefault('MC_NUM_QP_PER_EP', '16')
        require(os.environ['MC_NUM_QP_PER_EP'] == '16', 'benchmark requires 16 configured QPs')
        window = max(block_bytes(n,args.unit) for n in args.sizes)
        if args.require_remote_numa and args.access_window_bytes is not None:
            window = max(window,args.access_window_bytes)
        capacity = allocation_geometry(window,args.page_mode)
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
        placement = (nic_placement(node['rdma_device'],args.numa_node,require_remote=True,backing=args.backing)
                     if args.require_remote_numa else nic_placement(node['rdma_device'],args.numa_node))
        stage = 'native-origin'
        import mooncake.engine as engine_module
        import callosum_relay_native as native
        require(native.ABI_VERSION == 2, 'native relay ABI mismatch')
        require(not args.submission_mode or hasattr(engine_module.TransferEngine,'relay_write_pipeline'),
                'rebuild native pipeline binding before starting this experiment')
        require(Path(native.__file__).resolve().parent == Path(os.environ['CALLOSUM_NATIVE_BUILD']).resolve(),
                'stale native module')
        require(Path(engine_module.__file__).resolve() ==
                (Path(receipt['build'])/'python/mooncake/engine.so').resolve(), 'stale engine module')
        stage = 'dram-allocation'
        memory = Memory(capacity, placement['memory_numa_node'], args.page_mode, backing=args.backing)
        if args.require_remote_numa:
            require_remote_pages(placement,memory.page_evidence,capacity)
        stage = 'engine-registration'
        engine = engine_module.TransferEngine()
        require(engine.relay_abi_version() == 2, 'engine relay ABI mismatch')
        endpoint = node['te_endpoint'].rsplit(':', 1)[0] + ':' + str(args.te_port)
        require(engine.initialize(endpoint, 'P2PHANDSHAKE', 'rdma', node['rdma_device']) == 0,
                'engine initialization failed')
        require(engine.relay_rdma_available(), 'RDMA unavailable; no TCP fallback')
        require(engine.register_memory(memory.address, capacity) == 0, f'{args.backing} registration failed')
        if args.require_remote_numa:
            # Long-term pinning can migrate pages out of ZONE_MOVABLE. Verify
            # the registered mapping before exposing any receiver target.
            try:
                memory.placement = resident_pages(memory.address, memory.node, capacity)
                memory.page_evidence.update(memory.placement)
                require_remote_pages(placement,memory.page_evidence,capacity)
            except BaseException:
                require(engine.unregister_memory(memory.address) == 0,
                        'NUMA residency failed after registration; local unregister also failed (no target exposed)')
                memory.memory.close()
                raise
        segment = endpoint.rsplit(':', 1)[0] + ':' + str(engine.get_rpc_port())
        save(output/'environment.json', dict(node=node['id'], peer=peer['id'], role=args.role, pid=os.getpid(),
             fingerprint=c['fingerprint'], host=socket.gethostname(), placement=placement,
             residency=memory.placement, mr_pages=getattr(memory,'page_evidence',None),
             capacity=capacity, memlock=[soft, hard], backing=args.backing, peer_backing=args.peer_backing,
             configured_qps=16, active_qps_verified=False, setup_run=str(Path(args.setup_run).resolve()),
             modules={str(Path(m.__file__).resolve()): hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()
                      for m in (engine_module, native)},
             software_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
             loop_sha256=hashlib.sha256(Path(__file__).with_name('callosum_transport_benchmark.py').read_bytes()).hexdigest(),
             transport_env={k:os.environ.get(k) for k in ('MC_MTU','MC_GID_INDEX','MC_NUM_QP_PER_EP')}))
        if args.diagnose or args.sample_queues or (args.submission_mode and args.pipeline_instrumentation!='off'):
            save(output/'transport-inventory.json',rdma_inventory(engine))
        if args.role == 'receiver':
            service = Receiver(memory, segment, node['id'], peer['id'], c['fingerprint'], placement, output,
                               peer_backing=args.peer_backing)
            http = server(control, service)
            # Join request handlers before unregistering memory or exiting.
            http.daemon_threads = False
            stopped = threading.Event()
            signal.signal(signal.SIGINT, lambda *_: stopped.set())
            signal.signal(signal.SIGTERM, lambda *_: stopped.set())
            signal.signal(signal.SIGHUP, lambda *_: stopped.set())
            threading.Thread(target=http.serve_forever, daemon=True).start()
            print(f'[READY] NUMA receiver {node["id"]} ({args.backing}) at {control}; evidence={output}', flush=True)
            while True:
                stopped.wait(0.25)
                if not stopped.is_set() and not service.finished:
                    continue
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
                     path_mode=numa_path(args.backing,args.peer_backing), results=results, active_qps_verified=False,
                     report=report,interpretation=(
                         'Native serialization/batched comparison; no saturation or serving claim.'
                         if args.submission_mode else 'Serial drained WRITE control; no saturation or serving claim.')))
                print((output/'report.md').read_text(),flush=True)
                if args.auto_cleanup:
                    rpc(f"http://{urlparse(peer['url']).hostname}:{args.control_port}",'finish',
                        dict(sender=node['id'],fingerprint=c['fingerprint'],trials=len(results)))
            except BaseException as error:
                if safety.get('exposed'):
                    retain_uncertain(error, output)
                raise
        # Only normal, drained completion permits local unregister/free.
        require(engine.unregister_memory(memory.address) == 0, 'DRAM unregister failed')
        memory.memory.close()
        if args.auto_cleanup:
            print('[CLEANUP PASS] Owned memory unregistered; no active target retained.',flush=True)
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
