#!/usr/bin/env python3
"""Callosum dedicated-arena relay service. No Store allocation ownership.

Control: authenticated JSON/HTTP on a trusted cluster network.
Payload: Mooncake legacy RDMA, or explicitly labelled local-file test DMA.
No eviction/reuse: uncertain operations permanently quarantine their extents.
"""
import argparse
import ctypes
import ctypes.util
import hashlib
import hmac
import json
import math
import mmap
import os
import re
from pathlib import Path
import secrets
import sqlite3
import stat
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from callosum_dispatch import SourceDispatcher, partition_source
from callosum_initiation import CacheMiss
from callosum_transport_benchmark import block_bytes, write_loop, TOTAL_BYTES

RUNTIME_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
IMPLEMENTATION_SHA256 = hashlib.sha256(b''.join(
    (Path(__file__).parent / name).read_bytes() for name in
    ('callosum_runtime.py', 'callosum_dispatch.py', 'callosum_transport_benchmark.py',
     'callosum_initiation.py', 'callosum_worker.py'))).hexdigest()

STAGES = ['lookup', 'reserved', 'targets_ready', 'dispatch_begin',
          'writes_complete', 'verified', 'published', 'handles_released', 'failed']


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def token():
    value = os.environ.get('CALLOSUM_CONTROL_TOKEN', '')
    require(len(value) >= 32, 'CALLOSUM_CONTROL_TOKEN must contain at least 32 characters')
    return value


def rpc(url, method, data=None, timeout=60):
    request = urllib.request.Request(url.rstrip('/') + '/' + method,
        data=json.dumps(data or {}).encode(), headers={
            'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token()})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        result = json.load(response)
    if not result.get('ok') and result.get('error_kind') == 'cache_miss_before_submit':
        raise CacheMiss(result['error'])
    require(result.get('ok'), result.get('error', 'control RPC failed'))
    return result['result']


class Log:
    def __init__(self, path, node):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.file = open(path, 'a', buffering=1)
        self.node = node
        self.lock = threading.Lock()

    def emit(self, stage, **fields):
        record = dict(stage=stage, node=self.node, monotonic_ns=time.monotonic_ns(),
                      wall_time_ns=time.time_ns(), evidence_class='TRANSFER_ONLY')
        # Preserve the coordinator timestamp without overwriting the sink clock.
        if 'monotonic_ns' in fields:
            fields['coordinator_monotonic_ns'] = fields.pop('monotonic_ns')
        record.update(fields)
        with self.lock:
            self.file.write(json.dumps(record, sort_keys=True) + '\n')

    def payload_trace(self, identity):
        require(isinstance(identity, str) and len(identity) == 32
                and all(c in '0123456789abcdef' for c in identity), 'invalid trace transfer ID')
        # Authenticated control traffic only. Do not expose arbitrary files,
        # object keys, payload, target addresses or registration secrets.
        allowed = {'stage','node','monotonic_ns','wall_time_ns','evidence_class',
                   'transfer_id','sender','receiver','chunk','batch_id','cardinality',
                   'bytes','opcode','duration_ns','mode','path_mode','actual_transport',
                   'object_byte_start'}
        events = []
        with self.lock:
            self.file.flush()
            with open(self.file.name) as stream:
                for line in stream:
                    event = json.loads(line)
                    if event.get('transfer_id') == identity and event.get('stage') in ('batch_complete','chunk_complete'):
                        require(len(events) < 2048, 'trace exceeds bounded smoke evidence limit')
                        events.append({k:v for k,v in event.items() if k in allowed})
        return events


def config(path):
    c = json.loads(Path(path).read_text())
    require(c['mode'] in ('rdma', 'local-file-test'), 'unknown mode')
    page_mode = c.get('mr_page_mode', 'native')
    require(page_mode in ('native', 'huge2MB'), 'mr_page_mode must be native or huge2MB')
    require(page_mode == 'native' or c['mode'] == 'rdma',
            'huge2MB requires real devdax/RDMA, not file or anonymous DRAM')
    expected_path = 'direct_dax_write' if c['mode'] == 'rdma' else 'file_copy'
    require(c.get('path_mode', expected_path) == expected_path,
            'unsupported path_mode; staged WRITE is not implemented; no fallback')
    require(c.get('evidence_class', 'TRANSFER_ONLY') == 'TRANSFER_ONLY',
            'this runtime cannot serve as Store or GlobalMaster authority')
    require(len(c['nodes']) in (2, 4) and len(c['pools']) == 2,
            'require two or four workers and two pools')
    require(len({n['id'] for n in c['nodes']}) == len(c['nodes']), 'duplicate node')
    urls = [c['master']] + [n['url'] for n in c['nodes']]
    require(len(urls) == len(set(urls)), 'duplicate service endpoint')
    for url in urls:
        parsed = urlparse(url)
        require(parsed.scheme == 'http' and parsed.hostname and parsed.port and
                not parsed.username and not parsed.password and not parsed.query and
                not parsed.fragment and parsed.path in ('', '/'),
                'expected explicit http://host:port control endpoint')
        if c['mode'] == 'local-file-test':
            require(parsed.hostname == '127.0.0.1', 'file test is loopback-only')
    for pool, p in c['pools'].items():
        require(type(p['capacity']) is int and 8192 <= p['capacity'] < 2**63 and
                p['capacity'] % 4096 == 0 and
                type(p.get('map_offset', 0)) is int and p.get('map_offset', 0) >= 0 and
                p.get('map_offset', 0) + p['capacity'] < 2**63 and
                p.get('map_offset', 0) % 4096 == 0, 'invalid pool geometry')
        require(sum(n['pool'] == pool for n in c['nodes']) == len(c['nodes']) // 2,
                'each pool needs half the configured workers')
    for n in c['nodes']:
        require(n['pool'] in c['pools'], 'unknown pool')
        require(Path(n['path']).is_absolute(), 'mapping path must be absolute')
        require(type(n['can_send']) is bool and type(n['can_receive']) is bool,
                'capabilities must be booleans')
    if c['mode'] == 'rdma':
        require(c.get('abi_version') == 2, 'RDMA manifest requires abi_version=2')
        require(c.get('path_mode') == 'direct_dax_write', 'select direct_dax_write explicitly')
        require(isinstance(c.get('metadata'), str) and c['metadata'], 'missing TE metadata endpoint')
        identities = []
        for p in c['pools'].values():
            require(isinstance(p.get('owner'), str) and p['owner'].strip(),
                    'exclusive arena owner is required')
            require(isinstance(p.get('physical_pool_id'), str) and p['physical_pool_id'].strip(),
                    'physical pool identity is required; DAX names do not prove isolation')
            identities.append(p['physical_pool_id'])
            alignment = p.get('mapping_alignment')
            require(type(alignment) is int and alignment >= 4096 and
                    alignment & (alignment-1) == 0 and
                    p['capacity'] % alignment == 0 and p.get('map_offset',0) % alignment == 0,
                    'invalid device mapping alignment')
            if page_mode == 'huge2MB':
                require(alignment == 2**21, 'huge2MB requires 2097152-byte DAX alignment')
        require(len(set(identities)) == 2, 'physical pools must be independent')
        require(all(n.get('te_endpoint') and n.get('rdma_device') for n in c['nodes']),
                'explicit TE endpoint and RDMA device required per worker')
        require(len({n['te_endpoint'] for n in c['nodes']}) == len(c['nodes']),
                'duplicate TE endpoint')
    require(type(c.get('batch_cap', 32)) is int and 1 <= c.get('batch_cap', 32) <= 256,
            'batch_cap must be 1..256')
    require(type(c.get('max_inflight_bytes', 64*1024**2)) is int and
            c.get('max_inflight_bytes', 64*1024**2) > 0, 'invalid in-flight byte budget')
    policy = c.get('policy', {})
    require(isinstance(policy, dict) and type(policy.get('forced',True)) is bool,
            'invalid fixture policy')
    require(set(policy) <= {'forced','transfer_us','recompute_us','margin_us',
                            'margin_fraction','uncertainty_us'},
            'unknown fixture policy field')
    for name in ('transfer_us','recompute_us','margin_us','margin_fraction','uncertainty_us'):
        value = policy.get(name, -1 if name in ('transfer_us','recompute_us') else 0)
        require(type(value) in (int,float) and math.isfinite(value) and
                (value >= 0 or (name in ('transfer_us','recompute_us') and value == -1)),
                'invalid fixture policy estimate')
    c['fingerprint'] = hashlib.sha256(json.dumps(c, sort_keys=True).encode()).hexdigest()
    return c


def mapping_page_evidence(address, capacity, device, page_mode):
    """Report kernel VMA geometry, not an RNIC translation-page guarantee.

    Device DAX selects its fault granularity from device alignment. MAP_HUGETLB
    selects a different backing allocator and must not replace this mapping.
    """
    result = dict(mode=page_mode, backing='devdax', hugetlb=False,
                  device_alignment_bytes=None, kernel_page_size_bytes=None,
                  mmu_page_size_bytes=None, virtual_alignment_2MiB=address % 2**21 == 0,
                  rnic_page_size_bytes=None, rnic_page_size_status='UNVERIFIED_DRIVER_SELECTED')
    try:
        info = os.stat(device)
        require(stat.S_ISCHR(info.st_mode), 'expected DAX character device')
        sysdev = Path(f'/sys/dev/char/{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}')
        result['device_alignment_bytes'] = int((sysdev/'align').read_text().strip(), 0)
        header = re.compile(r'^([0-9a-f]+)-([0-9a-f]+)\s')
        selected = False
        with open('/proc/self/smaps') as stream:
            for line in stream:
                match = header.match(line)
                if match:
                    if selected:
                        break
                    selected = int(match[1],16) == address and int(match[2],16) == address+capacity
                elif selected and line.startswith(('KernelPageSize:', 'MMUPageSize:')):
                    field = 'kernel_page_size_bytes' if line.startswith('Kernel') else 'mmu_page_size_bytes'
                    result[field] = int(line.split()[1])*1024
        result['vma_evidence_status'] = 'OBSERVED' if result['kernel_page_size_bytes'] else 'UNAVAILABLE'
    except (OSError, ValueError, RuntimeError) as error:
        result['vma_evidence_status'] = 'UNAVAILABLE'
        result['evidence_error'] = str(error)
    if page_mode == 'huge2MB':
        require(result['device_alignment_bytes'] == 2**21 and
                result['kernel_page_size_bytes'] == 2**21 and
                result['virtual_alignment_2MiB'] and capacity % 2**21 == 0,
                'huge2MB requires observed 2 MiB device/VMA geometry; no fallback or device reconfiguration')
    return result


class Mapping:
    def __init__(self, c, node):
        p = c['pools'][node['pool']]
        require(os.environ.get('CALLOSUM_DESTRUCTIVE_ACK') == '1',
                'set CALLOSUM_DESTRUCTIVE_ACK=1 for dedicated-arena access')
        self.capacity = p['capacity']
        self.fd = os.open(node['path'], os.O_RDWR)
        try:
            info = os.fstat(self.fd)
            offset = p.get('map_offset', 0)
            if c['mode'] == 'rdma':
                require(stat.S_ISCHR(info.st_mode), 'RDMA mode requires explicit devdax character device')
            else:
                require(stat.S_ISREG(info.st_mode) and info.st_size >= offset + self.capacity,
                        'test pool file too small')
            self.memory = mmap.mmap(self.fd, self.capacity, flags=mmap.MAP_SHARED,
                                    prot=mmap.PROT_READ | mmap.PROT_WRITE, offset=offset)
            self.address = ctypes.addressof(ctypes.c_char.from_buffer(self.memory))
            self.page_evidence = (mapping_page_evidence(self.address,self.capacity,node['path'],
                                  c.get('mr_page_mode','native')) if c['mode']=='rdma' else
                                  dict(mode='native',backing='file',hugetlb=False))
        except BaseException:
            # Registration has not started. No engine or DMA can reference this mapping.
            if hasattr(self,'memory'):
                self.memory.close()
            os.close(self.fd)
            raise

    def bounds(self, offset, length):
        require(isinstance(offset, int) and isinstance(length, int) and
                offset >= 0 and length > 0 and offset + length <= self.capacity,
                'range outside mapping')

    def digest(self, offset, length):
        self.bounds(offset, length)
        digest = hashlib.sha256()
        for pos in range(offset, offset + length, 1024 * 1024):
            digest.update(self.memory[pos:min(pos + 1024 * 1024, offset + length)])
        return digest.hexdigest()


class Cuda:
    """GPU completion is separate from pool publication. No GDR claim."""
    def __init__(self, device):
        library = os.environ.get('CALLOSUM_CUDART') or ctypes.util.find_library('cudart')
        require(library, 'set CALLOSUM_CUDART to the CUDA runtime shared library')
        self.lib = ctypes.CDLL(library)
        P, S, I = ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int
        for name, args in {'cudaSetDevice':[I], 'cudaMalloc':[ctypes.POINTER(P), S],
            'cudaFree':[P], 'cudaStreamCreate':[ctypes.POINTER(P)],
            'cudaStreamDestroy':[P], 'cudaStreamSynchronize':[P],
            'cudaMemcpyAsync':[P, P, S, I, P], 'cudaMemcpy':[P, P, S, I]}.items():
            getattr(self.lib, name).argtypes = args
            getattr(self.lib, name).restype = I
        self.check(self.lib.cudaSetDevice(device))

    @staticmethod
    def check(code):
        require(code == 0, f'CUDA runtime error {code}')

    def copy(self, source, length, expected, destination=None):
        dst, stream = ctypes.c_void_p(destination), ctypes.c_void_p()
        owned = destination is None
        if owned:
            self.check(self.lib.cudaMalloc(ctypes.byref(dst), length))
        try:
            self.check(self.lib.cudaStreamCreate(ctypes.byref(stream)))
            start = time.monotonic_ns()
            self.check(self.lib.cudaMemcpyAsync(dst, source, length, 1, stream))
            self.check(self.lib.cudaStreamSynchronize(stream))
            ready = time.monotonic_ns()
            # Validate actual GPU bytes after completion, in bounded host chunks.
            digest = hashlib.sha256()
            buf = ctypes.create_string_buffer(min(length, 1024 * 1024))
            for offset in range(0, length, len(buf)):
                count = min(len(buf), length - offset)
                self.check(self.lib.cudaMemcpy(buf, dst.value + offset, count, 2))
                digest.update(buf.raw[:count])
            require(digest.hexdigest() == expected, 'GPU checksum mismatch')
            return dict(gpu_ready_ns=ready, cxl_to_gpu_ns=ready-start,
                        gpu_checksum=digest.hexdigest(), path='cxl_cuda_copy')
        finally:
            if stream.value:
                self.lib.cudaStreamSynchronize(stream)
                self.lib.cudaStreamDestroy(stream)
            if owned and dst.value:
                self.lib.cudaFree(dst)


class Agent:
    def __init__(self, c, node_id, log):
        self.c, self.log = c, log
        self.node = next(n for n in c['nodes'] if n['id'] == node_id)
        self.mapping = Mapping(c, self.node)
        self.epoch = secrets.token_hex(16)
        self.lock = threading.Lock()
        self.targets, self.jobs = {}, {}
        self.dispatcher = SourceDispatcher(self.send_group)
        self.engine = None
        if c['mode'] == 'rdma':
            from mooncake.engine import TransferEngine
            self.engine = TransferEngine()
            require(hasattr(self.engine, 'relay_write_batch_drained'),
                    'rebuild engine: relay_write_batch_drained missing')
            require(hasattr(self.engine, 'relay_abi_version') and self.engine.relay_abi_version() == 2,
                    'engine relay ABI mismatch; rebuild and verify imported module')
            require(self.engine.initialize(self.node['te_endpoint'], c['metadata'],
                        'rdma', self.node['rdma_device']) == 0, 'Transfer Engine init failed')
            require(self.engine.relay_rdma_available(), 'legacy RDMA unavailable; TCP fallback refused')
            require(self.engine.register_memory(self.mapping.address, self.mapping.capacity) == 0,
                    'DAX RDMA registration failed; no DRAM/TCP fallback is allowed')
        self.segment = self.node.get('te_endpoint', self.node['id'])
        if c.get('metadata') == 'P2PHANDSHAKE' and self.engine:
            self.segment = self.segment.rsplit(':', 1)[0] + ':' + str(self.engine.get_rpc_port())
        self.log.emit('agent_ready', pool=self.node['pool'], epoch=self.epoch,
                      mode=c['mode'], mr_pages=self.mapping.page_evidence, evidence_class='TRANSFER_ONLY')

    def descriptor(self):
        return dict(id=self.node['id'], pool=self.node['pool'],
                    capacity=self.mapping.capacity, can_send=self.node['can_send'],
                    can_receive=self.node['can_receive'], epoch=self.epoch,
                    segment=self.segment, fingerprint=self.c['fingerprint'], abi_version=2,
                    runtime_sha256=RUNTIME_SHA256,
                    implementation_sha256=IMPLEMENTATION_SHA256,
                    transport_request_timing_version=1,
                    mr_pages=self.mapping.page_evidence,
                    configured_qps=os.environ.get('MC_NUM_QP_PER_EP', 'default'))

    def handle(self, method, d):
        if method == 'health':
            return self.descriptor()
        if method == 'payload_trace':
            return self.log.payload_trace(d.get('transfer_id'))
        if method == 'dispatch_status':
            return self.dispatcher.status()
        if method == 'dispatch':
            require(d['source']['pool'] == self.node['pool'] and self.node['can_send'],
                    'dispatch must execute at an eligible source member')
            source = d['source']
            self.mapping.bounds(source['offset'], source['length'])
            require(len({g['sender'] for g in d['senders']}) == len(d['senders']),
                    'duplicate source sender group')
            for group in d['senders']:
                member = next(n for n in self.c['nodes'] if n['id'] == group['sender'])
                require(member['pool'] == self.node['pool'] and member['can_send'],
                        'ineligible source sender')
                for w in group['writes']:
                    e = w['source']
                    require(e['pool'] == source['pool'] and e['offset'] >= source['offset'] and
                            e['offset'] + e['length'] <= source['offset'] + source['length'],
                            'dispatch outside source grant')
            planned = partition_source(d)
            self.log.emit('source_scatter',transfer_id=d['transfer_id'],
                          senders=[g['sender'] for g in planned['senders']],
                          chunks=sum(len(g['writes']) for g in planned['senders']))
            return self.dispatcher.execute(planned)
        if method == 'transport_write_loop':
            require(self.engine is not None and self.node['can_send'],
                    'transport baseline requires real RDMA engine')
            require(os.environ.get('MC_NUM_QP_PER_EP') == '16', 'baseline requires 16 configured QPs')
            extent, target = d['source'], d['target']
            require(extent['pool'] == self.node['pool'], 'wrong source pool')
            require(0 < d['block_bytes'] <= min(extent['length'],target['extent']['length']) and
                    target['plan_id'] == d['transfer_id'], 'baseline exceeds authorized window')
            self.mapping.bounds(extent['offset'], d['block_bytes'])
            peer = next(n for n in self.c['nodes'] if n['id'] == target['receiver'])
            require(peer['pool'] != self.node['pool'] and
                    target['address_kind'] == 'classic_va' and
                    rpc(peer['url'], 'target', dict(transfer_id=d['transfer_id'])) == target,
                    'stale transport target')
            with self.lock:
                require(d['transfer_id'] not in self.jobs, 'benchmark replay refused')
                self.jobs[d['transfer_id']] = 'RUNNING'
            try:
                samples = []
                result = write_loop(lambda src,dst,n: self.engine.relay_write_batch_drained(
                    [target['segment']], [src], [dst], [n])[0],
                    self.mapping.address + extent['offset'], target['remote_base'],
                    d['block_bytes'], TOTAL_BYTES, self.c.get('max_inflight_bytes',64*1024**2),samples)
                from callosum_benchmark_report import write_request_csv
                observed = dict(result,status='PENDING_VALIDATION',transfer_id=d['transfer_id'],
                                path_mode='direct_dax_write')
                artifact = Path(self.log.file.name).parent / ('transport-observed-'+d['transfer_id']+'.json')
                with artifact.open('x') as stream:
                    json.dump(observed,stream)
                write_request_csv(artifact.with_suffix('.csv'),observed)
                self.jobs[d['transfer_id']] = 'DRAINED'
                self.log.emit('transport_baseline_complete', transfer_id=d['transfer_id'],
                              opcode='WRITE', actual_transport='rdma',
                              **{k:v for k,v in result.items() if k != 'requests'})
                return result
            except BaseException:
                self.jobs[d['transfer_id']] = 'QUARANTINED'
                try:
                    artifact = Path(self.log.file.name).parent / ('transport-failed-'+d['transfer_id']+'.json')
                    with artifact.open('x') as stream:
                        partial=dict(status='FAIL',transfer_id=d['transfer_id'],requests=samples,
                                     path_mode='direct_dax_write',block_bytes=d['block_bytes'])
                        json.dump(partial,stream)
                    from callosum_benchmark_report import write_request_csv
                    write_request_csv(artifact.with_suffix('.csv'),partial)
                except OSError:
                    pass
                raise
        if method == 'marker':
            # The first page is reserved for destructive topology challenges.
            if 'value' in d:
                value = bytes.fromhex(d['value'])
                require(len(value) == 32, 'invalid challenge')
                self.mapping.memory[:32] = value
            return self.mapping.memory[:32].hex()
        if method == 'digest':
            return self.mapping.digest(d['offset'], d['length'])
        if method == 'seed':
            offset, length = d['offset'], d['length']
            self.mapping.bounds(offset, length)
            require(offset >= 4096, 'reserved topology page')
            pattern = hashlib.sha256(d['key'].encode()).digest() * 32768
            for pos in range(0, length, len(pattern)):
                self.mapping.memory[offset+pos:offset+min(pos+len(pattern), length)] = pattern[:min(len(pattern), length-pos)]
            return self.mapping.digest(offset, length)
        if method == 'prepare':
            require(self.node['can_receive'], 'node cannot receive')
            e = d['extent']
            require(e['pool'] == self.node['pool'] and e['offset'] >= 4096, 'wrong target pool')
            self.mapping.bounds(e['offset'], e['length'])
            target = dict(receiver=self.node['id'], extent=e, segment=self.segment,
                          remote_base=self.mapping.address + e['offset'], epoch=self.epoch,
                          address_kind='classic_va', plan_id=d['transfer_id'])
            with self.lock:
                previous = self.targets.get(d['transfer_id'], target)
                require(previous == target, 'conflicting registration replay')
                for identity, other in self.targets.items():
                    if identity == d['transfer_id']:
                        continue
                    a, b = e, other['extent']
                    require(a['offset'] >= b['offset']+b['length'] or
                            b['offset'] >= a['offset']+a['length'],
                            'target aliases another retained reservation')
                self.targets[d['transfer_id']] = target
            return target
        if method == 'target':
            with self.lock:
                return self.targets[d['transfer_id']]
        if method == 'write':
            return self.write(d)
        if method == 'consume':
            start = time.monotonic_ns()
            result = rpc(self.c['master'], 'fetch', dict(d, requester=self.node['id']), timeout=600)
            require(self.mapping.digest(result['offset'], result['length']) == result['checksum'],
                    'requester cannot observe committed bytes')
            if d.get('gpu', False):
                result.update(Cuda(d.get('device', 0)).copy(self.mapping.address+result['offset'],
                              result['length'], result['checksum']))
                stage = 'gpu_ready'
            else:
                stage = 'host_ready'
            result['request_to_ready_ns'] = time.monotonic_ns() - start
            self.log.emit(stage, transfer_id=result['transfer_id'], request_id=d.get('request_id'),
                          worker=self.node['id'], bytes=result['length'],
                          request_to_ready_ns=result['request_to_ready_ns'])
            return result
        raise RuntimeError('unknown agent operation')

    def send_group(self, plan, group):
        node = next(n for n in self.c['nodes'] if n['id'] == group['sender'])
        return rpc(node['url'], 'write', dict(group, transfer_id=plan['transfer_id'],
            targets=plan['targets'], epoch=plan['sender_epochs'][group['sender']],
            source_object_offset=plan['source']['offset']), timeout=300)

    def write(self, d):
        transfer = d['transfer_id']
        require(self.node['can_send'] and d['sender'] == self.node['id'], 'wrong sender')
        require(d['epoch'] == self.epoch, 'sender epoch changed')
        with self.lock:
            require(transfer not in self.jobs, 'WRITE replay refused; inspect prior operation')
            self.jobs[transfer] = 'RUNNING'
        completions = []
        try:
            prepared = []
            require(d['writes'], 'empty sender plan')
            require(len({w['chunk'] for w in d['writes']}) == len(d['writes']),
                    'duplicate sender chunk')
            for w in d['writes']:
                src, dst = w['source'], w['destination']
                require(src['pool'] == self.node['pool'] and src['offset'] >= 4096,
                        'wrong source pool')
                self.mapping.bounds(src['offset'], src['length'])
                t = d['targets'][w['target_index']]
                require(t['address_kind'] == 'classic_va' and t['plan_id'] == transfer,
                        'wrong target address_kind or plan')
                peer = next(n for n in self.c['nodes'] if n['id'] == t['receiver'])
                live = rpc(peer['url'], 'target', dict(transfer_id=transfer))
                require(live == t and peer['pool'] == dst['pool'], 'stale receiver target')
                relative = dst['offset'] - t['extent']['offset']
                require(relative >= 0 and relative + src['length'] <= t['extent']['length'] and
                        src['length'] == dst['length'] and
                        w['remote_address'] == t['remote_base'] + relative, 'WRITE outside assigned MR')
                prepared.append((w, t, peer))
            # Across-batch overlap is also unsafe. A cap of one must not hide it.
            intervals = sorted((t['segment'],w['remote_address'],w['source']['length'])
                               for w,t,p in prepared)
            for previous, current in zip(intervals, intervals[1:]):
                require(previous[0] != current[0] or previous[1]+previous[2] <= current[1],
                        'overlapping WRITE targets across sender plan')
            # Validate all projected addresses before any WRITE. Reuse the
            # exact native preflight used by the production engine binding.
            from callosum_relay_native import validate_write_batch
            cap = self.c.get('batch_cap', 32)
            budget = self.c.get('max_inflight_bytes', 64*1024**2)
            batches, batch, total = [], [], 0
            for item in prepared:
                length = item[0]['source']['length']
                require(length <= budget, 'chunk exceeds in-flight byte budget')
                if batch and (len(batch) == cap or total + length > budget):
                    batches.append(batch)
                    batch, total = [], 0
                batch.append(item)
                total += length
            if batch:
                batches.append(batch)
            for batch in batches:
                validate_write_batch(
                    [t['segment'] for w,t,p in batch],
                    [self.mapping.address+w['source']['offset'] for w,t,p in batch],
                    [w['remote_address'] for w,t,p in batch],
                    [w['source']['length'] for w,t,p in batch])
            for batch_id, batch in enumerate(batches):
                start = time.monotonic_ns()
                if self.engine:
                    counts = self.engine.relay_write_batch_drained(
                        [t['segment'] for w,t,p in batch],
                        [self.mapping.address+w['source']['offset'] for w,t,p in batch],
                        [w['remote_address'] for w,t,p in batch],
                        [w['source']['length'] for w,t,p in batch])
                else:
                    # Explicit T0 software DMA; never called as an RDMA fallback.
                    counts = []
                    for w,t,peer in batch:
                        src, dst = w['source'], w['destination']
                        other = Mapping(self.c, peer)
                        try:
                            other.memory[dst['offset']:dst['offset']+src['length']] = self.mapping.memory[
                                src['offset']:src['offset']+src['length']]
                        finally:
                            other.memory.close(); os.close(other.fd)
                        counts.append(src['length'])
                require(len(counts) == len(batch), 'batch completion count mismatch')
                self.log.emit('batch_complete', transfer_id=transfer, sender=self.node['id'],
                    batch_id=batch_id, cardinality=len(batch), bytes=sum(counts), opcode='WRITE',
                    duration_ns=time.monotonic_ns()-start, mode=self.c['mode'],
                    path_mode='direct_dax_write' if self.engine else 'file_copy',
                    actual_transport='rdma' if self.engine else 'file-copy')
                for (w,t,peer), count in zip(batch, counts):
                    completions.append(dict(chunk=w['chunk'], bytes=count,
                                            success=count==w['source']['length']))
                    self.log.emit('chunk_complete', transfer_id=transfer, sender=self.node['id'],
                        receiver=t['receiver'], chunk=w['chunk'], batch_id=batch_id, bytes=count,
                        object_byte_start=w['source']['offset']-d['source_object_offset'],
                        mode=self.c['mode'])
            self.jobs[transfer] = 'DRAINED'
            return completions
        except Exception:
            self.jobs[transfer] = 'QUARANTINED'
            raise


class Master:
    def __init__(self, c, database, log):
        self.c, self.log = c, log
        self.lock = threading.RLock()
        self.fetch_locks = {}
        self.seed_locks = {}
        self.preflight_lock = threading.Lock()
        self.db = sqlite3.connect(database, check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
          CREATE TABLE IF NOT EXISTS members (id TEXT PRIMARY KEY, descriptor TEXT);
          CREATE TABLE IF NOT EXISTS pools (id TEXT PRIMARY KEY, next INTEGER);
          CREATE TABLE IF NOT EXISTS objects (key TEXT, pool TEXT, record TEXT, PRIMARY KEY(key,pool));
          CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, state TEXT, record TEXT);
        ''')
        old = self.db.execute("SELECT value FROM settings WHERE key='fingerprint'").fetchone()
        require(not old or old[0] == c['fingerprint'], 'configuration changed: retain old journal; do not reuse arena')
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO settings VALUES ('fingerprint',?)", (c['fingerprint'],))
            for pool in c['pools']:
                self.db.execute('INSERT OR IGNORE INTO pools VALUES (?,4096)', (pool,))
            self.db.execute("UPDATE operations SET state='QUARANTINED' WHERE state NOT IN ('COMMITTED','QUARANTINED')")
        self.topology_ok = False
        self.startup_aborted = bool(self.db.execute(
            "SELECT 1 FROM settings WHERE key='startup_aborted'").fetchone())

    def node(self, identity):
        return next(n for n in self.c['nodes'] if n['id'] == identity)

    def members(self):
        with self.lock:
            records = [json.loads(row[0]) for row in self.db.execute('SELECT descriptor FROM members ORDER BY id')]
        require(len(records) == len(self.c['nodes']), 'all configured workers must register')
        for d in records:
            actual = rpc(self.node(d['id'])['url'], 'health')
            require(actual == d, 'worker changed epoch/configuration; stop and preserve arena')
        return records

    def allocate(self, pool, length):
        require(type(length) is int and 0 < length <= self.c.get('max_object_bytes', 1024**3),
                'invalid object size')
        with self.lock:
            start = self.db.execute('SELECT next FROM pools WHERE id=?', (pool,)).fetchone()[0]
            end = (start + length + 4095) // 4096 * 4096
            require(end <= self.c['pools'][pool]['capacity'], 'arena full; no unsafe extent reuse')
            with self.db:
                self.db.execute('UPDATE pools SET next=? WHERE id=?', (end, pool))
        return dict(pool=pool, offset=start, length=length)

    def operation(self, identity, state, record):
        with self.lock, self.db:
            self.db.execute('INSERT OR REPLACE INTO operations VALUES (?,?,?)',
                            (identity, state, json.dumps(record)))

    def handle(self, method, d):
        if method == 'abort_empty_startup':
            # This is not DMA drain or campaign cleanup. Close admission before
            # stopping only a coordinator that never admitted a participant.
            with self.lock:
                require(d.get('fingerprint') == self.c['fingerprint'], 'startup abort configuration mismatch')
                counts = {table:self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                          for table in ('members','objects','operations')}
                allocated = self.db.execute('SELECT 1 FROM pools WHERE next != 4096').fetchone()
                require(not any(counts.values()) and not allocated and not self.topology_ok,
                        'startup is not empty; retain coordinator, participants and arenas')
                with self.db:
                    self.db.execute("INSERT OR REPLACE INTO settings VALUES ('startup_aborted','1')")
                self.startup_aborted = True
                return dict(startup_aborted=True, no_participants_admitted=True, reclaimed=False)
        if method != 'status':
            with self.lock:
                require(not self.startup_aborted, 'startup admission is closed; retain this journal')
        if method == 'transport_benchmark':
            return self.transport_benchmark(d)
        if method == 'fetch':
            with self.lock:
                require(self.topology_ok, 'run destructive topology preflight first')
                identity = (d['key'], self.node(d['requester'])['pool'])
                single_flight = self.fetch_locks.setdefault(identity, threading.Lock())
            with single_flight:
                return self.fetch(d)
        if method == 'register':
            n = self.node(d['id'])
            live = rpc(n['url'], 'health')
            require(live == d and d['fingerprint'] == self.c['fingerprint'] and
                    d['pool'] == n['pool'] and d.get('abi_version') == 2 and
                    d.get('runtime_sha256') == RUNTIME_SHA256 and
                    d.get('implementation_sha256') == IMPLEMENTATION_SHA256,
                    'registration/code ABI mismatch')
            with self.lock:
                # A registration health RPC can race the empty-startup abort.
                require(not self.startup_aborted, 'startup admission is closed; registration refused')
                old = self.db.execute('SELECT descriptor FROM members WHERE id=?', (d['id'],)).fetchone()
                require(not old or json.loads(old[0]) == d,
                        'worker restart detected; old remote targets may be live; arena remains quarantined')
                with self.db:
                    self.db.execute('INSERT OR REPLACE INTO members VALUES (?,?)', (d['id'], json.dumps(d)))
                return dict(registered=d['id'])
        if method == 'status':
            with self.lock:
                return dict(mode=self.c['mode'], evidence_class='TRANSFER_ONLY',
                    fingerprint=self.c['fingerprint'], startup_aborted=self.startup_aborted,
                    registered_nodes=[row[0] for row in self.db.execute('SELECT id FROM members ORDER BY id')],
                    benchmark_request_timing_versions={i:json.loads(v).get('transport_request_timing_version',0)
                        for i,v in self.db.execute('SELECT id,descriptor FROM members')},
                    mr_pages={i:json.loads(v).get('mr_pages')
                        for i,v in self.db.execute('SELECT id,descriptor FROM members')},
                    remaining_capacity_bytes={p:self.c['pools'][p]['capacity']-n
                        for p,n in self.db.execute('SELECT id,next FROM pools')},
                    topology_verified=self.topology_ok,
                    operations=[dict(id=i, state=s) for i,s in self.db.execute('SELECT id,state FROM operations')])
        if method == 'preflight':
            with self.preflight_lock:
                with self.lock:
                    self.topology_ok = False
                members = self.members()
                # Distinct tokens at the SAME logical offset expose accidental
                # pool aliasing as well as members which do not share memory.
                challenges = {p:secrets.token_hex(32) for p in self.c['pools']}
                for pool, value in challenges.items():
                    member = next(m for m in members if m['pool'] == pool)
                    rpc(self.node(member['id'])['url'], 'marker', dict(value=value))
                for member in members:
                    observed = rpc(self.node(member['id'])['url'], 'marker')
                    require(observed == challenges[member['pool']], 'physical pool sharing/isolation check failed')
                with self.lock:
                    self.topology_ok = True
                self.log.emit('topology_verified', mode=self.c['mode'], members=len(members))
                return dict(shared_pairs_verified=True, separate_pools_verified=True, mode=self.c['mode'])
        if method == 'seed':
            require(isinstance(d['key'], str) and 0 < len(d['key']) <= 512, 'invalid key')
            with self.lock:
                require(self.topology_ok, 'run destructive topology preflight first')
                single_seed = self.seed_locks.setdefault(d['key'],threading.Lock())
            with single_seed:
                return self.seed(d)
        raise RuntimeError('unknown master operation')

    def seed(self, d):
        members = self.members()
        pool = self.node(d['node'])['pool']
        with self.lock:
            require(not self.db.execute('SELECT 1 FROM objects WHERE key=?', (d['key'],)).fetchone(),
                    'immutable key already exists')
            allocation_start = time.monotonic_ns()
            extent = self.allocate(pool, d['length'])
            allocation_ns = time.monotonic_ns() - allocation_start
        digest = rpc(self.node(d['node'])['url'], 'seed', dict(extent, key=d['key']), timeout=300)
        for m in members:
            if m['pool'] == pool:
                require(rpc(self.node(m['id'])['url'], 'digest', extent, timeout=300) == digest,
                        'source is not visible to every source member')
        record = dict(extent, key=d['key'], version=secrets.token_hex(16), checksum=digest)
        publication_start = time.monotonic_ns()
        with self.lock, self.db:
            self.db.execute('INSERT INTO objects VALUES (?,?,?)', (d['key'], pool, json.dumps(record)))
        return dict(record, initial_allocation_ns=allocation_ns,
                    initial_publication_ns=time.monotonic_ns()-publication_start)

    def transport_benchmark(self, d):
        block = block_bytes(d['block_size'], d['block_unit'])
        require(self.c.get('max_inflight_bytes',64*1024**2) >= block,
                'baseline block exceeds native request budget; configure max_inflight_bytes before agent startup')
        # Allocation and journal mutation only: release the authority lock before
        # posting the long transport loop. This dedicated range is never published.
        with self.lock:
            require(self.topology_ok and self.c['mode']=='rdma', 'RDMA preflight required')
            source_node, receiver = self.node(d['node']), self.node(d['receiver'])
            require(source_node['pool'] != receiver['pool'], 'independent pools required')
            row = self.db.execute('SELECT record FROM objects WHERE key=? AND pool=?',
                                 (d['key'],source_node['pool'])).fetchone()
            require(row, 'seed source object before baseline')
            source = json.loads(row[0])
            require(source['length'] >= block, 'source window smaller than block')
            identity = secrets.token_hex(16)
            destination = self.allocate(receiver['pool'], block)
            self.operation(identity, 'RESERVED', dict(source=source,destination=destination,
                                                     benchmark=True))
        try:
            members = self.members()
            require(all(m.get('configured_qps')=='16' for m in members),
                    'all endpoints must start with MC_NUM_QP_PER_EP=16')
            target = rpc(receiver['url'], 'prepare', dict(transfer_id=identity, extent=destination))
            result = rpc(source_node['url'], 'transport_write_loop', dict(
                transfer_id=identity,source=source,target=target,block_bytes=block), timeout=900)
            require(result['bytes'] == TOTAL_BYTES and result['block_bytes'] == block,
                    'baseline byte accounting mismatch')
            source_range = dict(source, length=block)
            expected = rpc(source_node['url'],'digest',source_range,timeout=300)
            require(rpc(receiver['url'],'digest',destination,timeout=300)==expected,
                    'baseline destination checksum mismatch')
            with self.lock:
                self.operation(identity, 'DRAINED_RETAINED', dict(source=source,
                    destination=destination,benchmark=True,result={k:v for k,v in result.items() if k != 'requests'}))
            return dict(result, status='PASS', transfer_id=identity,
                        evidence_class='TRANSFER_ONLY', block_unit=d['block_unit'],
                        configured_qps=16, active_qps_verified=False,
                        qp_gate='INCOMPLETE_REQUIRES_ENDPOINT_EVIDENCE',
                        path_mode='direct_dax_write', actual_transport='rdma',
                        mr_page_mode=self.c.get('mr_page_mode','native'),
                        mr_pages={m['id']:m.get('mr_pages') for m in members
                                  if m['id'] in (source_node['id'],receiver['id'])},
                        checksum_scope='final block window', store_compatible=False)
        except BaseException:
            with self.lock:
                self.operation(identity,'QUARANTINED',dict(source=source,
                    destination=destination,benchmark=True))
            raise

    def fetch(self, request):
        import callosum_relay_native
        requester = self.node(request['requester'])
        key = request['key']
        with self.lock:
            lookup_start = time.monotonic_ns()
            records = [json.loads(r[0]) for r in self.db.execute('SELECT record FROM objects WHERE key=?', (key,))]
        if not records:
            raise CacheMiss('KV not found; requester must recompute')
        local = next((r for r in records if r['pool'] == requester['pool']), None)
        metadata_lookup_ns = time.monotonic_ns() - lookup_start
        validation_start = time.monotonic_ns()
        if local:
            # A local hit must not depend on a remote source's health. Check
            # only this consumer's current mapping epoch. This is still the
            # experiment's directory, not a production LocalCxlMaster.
            with self.lock:
                row = self.db.execute('SELECT descriptor FROM members WHERE id=?',
                                      (requester['id'],)).fetchone()
            require(row and rpc(requester['url'],'health') == json.loads(row[0]),
                    'local worker epoch changed; preserve arena')
            return dict(local, transfer_id='local-hit', local_hit=True,
                        metadata_lookup_ns=metadata_lookup_ns,
                        participant_validation_ns=time.monotonic_ns()-validation_start)
        members = self.members()
        participant_validation_ns = time.monotonic_ns() - validation_start
        source = records[0]
        transfer_id = secrets.token_hex(16)
        # Fixed route is the explicit functional baseline. Optional costs are
        # fixtures, not live inference measurements or an adaptive scheduler.
        policy = self.c.get('policy', {})
        eligible = (any(m['pool']==source['pool'] and m['can_send'] for m in members) and
                    any(m['pool']==requester['pool'] and m['can_receive'] for m in members))
        decision = callosum_relay_native.select_delivery(False, True, eligible,
            policy.get('forced', True), policy.get('transfer_us', -1),
            policy.get('recompute_us', -1), policy.get('margin_us', 0),
            policy.get('margin_fraction', 0), policy.get('uncertainty_us', 0))
        self.log.emit('decision', transfer_id=transfer_id, request_id=request.get('request_id'),
                      object_hash=hashlib.sha256(key.encode()).hexdigest(),
                      action=decision['action'], reason=decision['reason'],
                      cost_source='fixture' if policy else 'forced_no_cost_model',
                      forced=policy.get('forced', True),
                      transfer_us=policy.get('transfer_us',-1),
                      recompute_us=policy.get('recompute_us',-1),
                      margin_us=policy.get('margin_us',0),
                      margin_fraction=policy.get('margin_fraction',0),
                      uncertainty_us=policy.get('uncertainty_us',0))
        require(decision['action']=='TRANSFER', 'policy selected '+decision['action']+': '+decision['reason'])
        chunk = request.get('chunk_bytes', 1024*1024)
        require(type(chunk) is int and chunk >= 4096 and
                source['length']//chunk <= 16384, 'invalid or excessive chunk plan')
        context = Session(self, request, transfer_id, source)
        self.operation(transfer_id, 'PINNED', dict(source=source, request=request))
        self.log.emit('request_begin', transfer_id=transfer_id, request_id=request.get('request_id'),
                      worker=requester['id'], source_pool=source['pool'], destination_pool=requester['pool'],
                      object_hash=hashlib.sha256(key.encode()).hexdigest(), source_generation=source['version'])
        try:
            result = callosum_relay_native.run(members, dict(transfer_id=transfer_id,
                key=key, source_pool=source['pool'], requester=requester['id'],
                worker=request.get('worker', requester['id']), chunk_bytes=chunk), context)
            return dict(result, checksum=source['checksum'], version=source['version'],
                        transfer_id=transfer_id, local_hit=False,
                        metadata_lookup_ns=metadata_lookup_ns,
                        participant_validation_ns=participant_validation_ns,
                        destination_allocation_ns=context.allocation_ns)
        except Exception as error:
            self.operation(transfer_id, 'QUARANTINED', dict(source=source, destination=context.destination,
                                                         error=str(error)))
            self.log.emit('request_failed', transfer_id=transfer_id, error=str(error))
            raise


class Session:
    def __init__(self, master, request, identity, source):
        self.m, self.request, self.id, self.source = master, request, identity, source
        self.destination = None

    def lookup_and_pin(self, key, pool):
        require(key == self.source['key'] and pool == self.source['pool'], 'source changed')
        # Immutable objects + durable PINNED journal entry prohibit reuse.
        return self.source

    def reserve(self, key, pool, length):
        start = time.monotonic_ns()
        self.destination = self.m.allocate(pool, length)
        self.allocation_ns = time.monotonic_ns() - start
        self.m.operation(self.id, 'RESERVED', dict(source=self.source, destination=self.destination))
        return self.destination

    def prepare(self, identity, receiver, extent):
        return rpc(self.m.node(receiver)['url'], 'prepare', dict(transfer_id=identity, extent=extent))

    def execute(self, plan):
        self.m.operation(self.id, 'DISPATCHED', dict(plan=plan, source=self.source, destination=self.destination))
        descriptors = {m['id']:m for m in self.m.members()}
        dispatcher = self.m.node(plan['senders'][0]['sender'])
        # Source member performs fan-out. The master handles control records;
        # neither dispatcher nor master adds a payload copy hop.
        return rpc(dispatcher['url'], 'dispatch', dict(plan, source=self.source,
            sender_epochs={identity:d['epoch'] for identity,d in descriptors.items()}), timeout=300)

    def verify(self):
        self.m.members()  # Reject changed boot epochs before publication.
        for n in self.m.c['nodes']:
            extent = self.source if n['pool'] == self.source['pool'] else self.destination
            actual = rpc(n['url'], 'digest', extent, timeout=300)
            require(actual == self.source['checksum'], 'source/destination SHA-256 mismatch')
        return True

    def commit(self):
        record = dict(self.destination, key=self.source['key'], checksum=self.source['checksum'],
                      version=self.source['version'])
        with self.m.lock, self.m.db:
            self.m.db.execute('INSERT INTO objects VALUES (?,?,?)',
                             (record['key'], record['pool'], json.dumps(record)))
            self.m.db.execute("UPDATE operations SET state='COMMITTED' WHERE id=?", (self.id,))

    def event(self, event):
        stage = STAGES[event.pop('stage')]
        self.m.log.emit(stage, **event, request_id=self.request.get('request_id'),
                        tp_rank=self.request.get('tp_rank'), source_pool=self.source['pool'],
                        destination_pool=self.m.node(self.request['requester'])['pool'],
                        object_hash=hashlib.sha256(self.source['key'].encode()).hexdigest(),
                        source_generation=self.source['version'])


def server(url, service):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                require(hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token()),
                        'unauthorized')
                length = int(self.headers.get('Content-Length', '0'))
                require(0 < length <= 8*1024*1024, 'control message size invalid')
                self.connection.settimeout(30)
                data = json.loads(self.rfile.read(length))
                result = service.handle(self.path.strip('/'), data)
                payload = dict(ok=True, result=result)
            except Exception as error:
                payload = dict(ok=False, error=str(error))
                if isinstance(error, CacheMiss):
                    payload['error_kind'] = 'cache_miss_before_submit'
            raw = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass  # Journal remains authoritative if a caller disconnects.
    parsed = urlparse(url)
    return ThreadingHTTPServer((parsed.hostname, parsed.port), Handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('role', choices=['check','master','agent','preflight','seed','consume','status','transport-benchmark'])
    parser.add_argument('--config', required=True)
    parser.add_argument('--node')
    parser.add_argument('--state-dir', default='./callosum-state')
    parser.add_argument('--key', default='kv-demo')
    parser.add_argument('--bytes', type=int, default=16*1024*1024)
    parser.add_argument('--chunk-bytes', type=int, default=1024*1024)
    parser.add_argument('--gpu', action='store_true')
    parser.add_argument('--device', type=int, default=0)
    parser.add_argument('--request-id', default='manual')
    parser.add_argument('--receiver')
    parser.add_argument('--block-size', type=int, choices=[1,2,4,8,16,32,64,128,256,512])
    parser.add_argument('--block-unit', choices=['MB','MiB'])
    args = parser.parse_args()
    c = config(args.config)
    import callosum_relay_native as native
    require(getattr(native, 'ABI_VERSION', None) == 2, 'native coordinator ABI mismatch')
    if args.role == 'check':
        # Exclude unknown/secret fields from the printed effective manifest.
        effective = {k:c[k] for k in ('mode','path_mode','evidence_class','abi_version','master',
                     'batch_cap','max_inflight_bytes','fingerprint') if k in c}
        effective.setdefault('batch_cap', 32)
        effective.setdefault('max_inflight_bytes', 64*1024**2)
        effective['mr_page_mode'] = c.get('mr_page_mode','native')
        effective['policy'] = dict(forced=True, transfer_us=-1, recompute_us=-1,
                                   margin_us=0, margin_fraction=0, uncertainty_us=0)
        effective['policy'].update(c.get('policy', {}))
        effective['pools'] = {p:{k:v[k] for k in ('capacity','map_offset','mapping_alignment',
                            'owner','physical_pool_id') if k in v} for p,v in c['pools'].items()}
        effective['nodes'] = [{k:n[k] for k in ('id','pool','url','path','te_endpoint','rdma_device',
                             'can_send','can_receive') if k in n} for n in c['nodes']]
        print(json.dumps(dict(status='PASS', evidence_class='TRANSFER_ONLY',
            path_mode=c.get('path_mode','file_copy' if c['mode']=='local-file-test' else 'direct_dax_write'),
            native_module=native.__file__, native_sha256=hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
            config=effective, hardware_validated=False, store_compatible=False),indent=2))
        return
    token()
    state = Path(args.state_dir)
    state.mkdir(parents=True, exist_ok=True)
    if args.role in ('master', 'agent'):
        # Never allow two processes to own the same local journal/agent state.
        import fcntl
        lock = open(state / (args.role + '-' + str(args.node) + '.lock'), 'a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        log = Log(state / (args.role + '-' + str(args.node) + '.jsonl'), args.node or 'master')
        if args.role == 'master':
            service = Master(c, state / 'master.sqlite', log)
            server(c['master'], service).serve_forever()
        else:
            service = Agent(c, args.node, log)
            http = server(service.node['url'], service)
            threading.Thread(target=http.serve_forever, daemon=True).start()
            print(json.dumps(rpc(c['master'], 'register', service.descriptor())), flush=True)
            threading.Event().wait()
    elif args.role in ('preflight', 'status'):
        print(json.dumps(rpc(c['master'], args.role), indent=2))
    elif args.role == 'seed':
        print(json.dumps(rpc(c['master'], 'seed', dict(node=args.node, key=args.key, length=args.bytes),
                             timeout=600), indent=2))
    elif args.role == 'transport-benchmark':
        require(args.node and args.receiver and args.block_size and args.block_unit,
                'baseline requires --node source --receiver --block-size --block-unit')
        wall_start = time.time_ns()
        start = time.monotonic_ns()
        result = rpc(c['master'], 'transport_benchmark', dict(node=args.node,receiver=args.receiver,
            key=args.key,block_size=args.block_size,block_unit=args.block_unit),timeout=1200)
        result['requester_rpc_ns'] = time.monotonic_ns()-start
        result['controller_start_unix_ns'] = wall_start
        result['controller_end_unix_ns'] = time.time_ns()
        from callosum_benchmark_report import enrich_trial
        result = enrich_trial(result)
        print(json.dumps(result,indent=2))
    else:
        node = next(n for n in c['nodes'] if n['id'] == args.node)
        print(json.dumps(rpc(node['url'], 'consume', dict(key=args.key, gpu=args.gpu,
            device=args.device, request_id=args.request_id, chunk_bytes=args.chunk_bytes), timeout=900), indent=2))


if __name__ == '__main__':
    main()
