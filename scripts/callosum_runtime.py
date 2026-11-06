#!/usr/bin/env python3
"""Callosum dedicated-arena relay service. No Store allocation ownership.

Control: authenticated JSON/HTTP on a trusted cluster network.
Payload: Mooncake legacy RDMA, or explicitly labelled local-file test DMA.
No eviction/reuse: uncertain operations permanently quarantine their extents.
"""
import argparse
import concurrent.futures
import ctypes
import ctypes.util
import hashlib
import hmac
import json
import math
import mmap
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

RUNTIME_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()

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


def config(path):
    c = json.loads(Path(path).read_text())
    require(c['mode'] in ('rdma', 'local-file-test'), 'unknown mode')
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


class Mapping:
    def __init__(self, c, node):
        p = c['pools'][node['pool']]
        require(os.environ.get('CALLOSUM_DESTRUCTIVE_ACK') == '1',
                'set CALLOSUM_DESTRUCTIVE_ACK=1 for dedicated-arena access')
        self.capacity = p['capacity']
        self.fd = os.open(node['path'], os.O_RDWR)
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
                      mode=c['mode'], evidence_class='TRANSFER_ONLY')

    def descriptor(self):
        return dict(id=self.node['id'], pool=self.node['pool'],
                    capacity=self.mapping.capacity, can_send=self.node['can_send'],
                    can_receive=self.node['can_receive'], epoch=self.epoch,
                    segment=self.segment, fingerprint=self.c['fingerprint'], abi_version=2,
                    runtime_sha256=RUNTIME_SHA256)

    def handle(self, method, d):
        if method == 'health':
            return self.descriptor()
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
                previous = self.targets.setdefault(d['transfer_id'], target)
                require(previous == target, 'conflicting registration replay')
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

    def node(self, identity):
        return next(n for n in self.c['nodes'] if n['id'] == identity)

    def members(self):
        records = [json.loads(row[0]) for row in self.db.execute('SELECT descriptor FROM members ORDER BY id')]
        require(len(records) == len(self.c['nodes']), 'all configured workers must register')
        for d in records:
            actual = rpc(self.node(d['id'])['url'], 'health')
            require(actual == d, 'worker changed epoch/configuration; stop and preserve arena')
        return records

    def allocate(self, pool, length):
        require(type(length) is int and 0 < length <= self.c.get('max_object_bytes', 1024**3),
                'invalid object size')
        start = self.db.execute('SELECT next FROM pools WHERE id=?', (pool,)).fetchone()[0]
        end = (start + length + 4095) // 4096 * 4096
        require(end <= self.c['pools'][pool]['capacity'], 'arena full; no unsafe extent reuse')
        with self.db:
            self.db.execute('UPDATE pools SET next=? WHERE id=?', (end, pool))
        return dict(pool=pool, offset=start, length=length)

    def operation(self, identity, state, record):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO operations VALUES (?,?,?)',
                            (identity, state, json.dumps(record)))

    def handle(self, method, d):
        with self.lock:
            if method == 'register':
                n = self.node(d['id'])
                live = rpc(n['url'], 'health')
                require(live == d and d['fingerprint'] == self.c['fingerprint'] and
                        d['pool'] == n['pool'] and d.get('abi_version') == 2 and
                        d.get('runtime_sha256') == RUNTIME_SHA256,
                        'registration/code ABI mismatch')
                old = self.db.execute('SELECT descriptor FROM members WHERE id=?', (d['id'],)).fetchone()
                require(not old or json.loads(old[0]) == d,
                        'worker restart detected; old remote targets may be live; arena remains quarantined')
                with self.db:
                    self.db.execute('INSERT OR REPLACE INTO members VALUES (?,?)', (d['id'], json.dumps(d)))
                return dict(registered=d['id'])
            if method == 'status':
                return dict(mode=self.c['mode'], evidence_class='TRANSFER_ONLY',
                    topology_verified=self.topology_ok,
                    operations=[dict(id=i, state=s) for i,s in self.db.execute('SELECT id,state FROM operations')])
            if method == 'preflight':
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
                self.topology_ok = True
                self.log.emit('topology_verified', mode=self.c['mode'], members=len(members))
                return dict(shared_pairs_verified=True, separate_pools_verified=True, mode=self.c['mode'])
            require(self.topology_ok, 'run destructive topology preflight first')
            if method == 'fetch':
                return self.fetch(d)
            members = self.members()
            if method == 'seed':
                require(isinstance(d['key'], str) and 0 < len(d['key']) <= 512, 'invalid key')
                pool = self.node(d['node'])['pool']
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
                with self.db:
                    self.db.execute('INSERT INTO objects VALUES (?,?,?)', (d['key'], pool, json.dumps(record)))
                return dict(record, initial_allocation_ns=allocation_ns,
                            initial_publication_ns=time.monotonic_ns()-publication_start)
            raise RuntimeError('unknown master operation')

    def fetch(self, request):
        import callosum_relay_native
        requester = self.node(request['requester'])
        key = request['key']
        lookup_start = time.monotonic_ns()
        records = [json.loads(r[0]) for r in self.db.execute('SELECT record FROM objects WHERE key=?', (key,))]
        require(records, 'KV not found; requester must recompute')
        local = next((r for r in records if r['pool'] == requester['pool']), None)
        metadata_lookup_ns = time.monotonic_ns() - lookup_start
        validation_start = time.monotonic_ns()
        if local:
            # A local hit must not depend on a remote source's health. Check
            # only this consumer's current mapping epoch. This is still the
            # experiment's directory, not a production LocalCxlMaster.
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
        def send(group):
            node = self.m.node(group['sender'])
            return rpc(node['url'], 'write', dict(group, transfer_id=self.id,
                targets=plan['targets'], epoch=descriptors[node['id']]['epoch'],
                source_object_offset=self.source['offset']), timeout=300)
        # HTTP timeout means uncertainty, not drain. Session failure quarantines
        # all extents; agents retain mappings/MRs for their entire process life.
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(plan['senders'])) as executor:
            pending = [executor.submit(send, group) for group in plan['senders']]
            results, errors = [], []
            for future in pending:
                try:
                    results.extend(future.result())
                except Exception as error:
                    errors.append(error)
            if errors:
                raise errors[0]
            return results

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
        with self.m.db:
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
    parser.add_argument('role', choices=['check','master','agent','preflight','seed','consume','status'])
    parser.add_argument('--config', required=True)
    parser.add_argument('--node')
    parser.add_argument('--state-dir', default='./callosum-state')
    parser.add_argument('--key', default='kv-demo')
    parser.add_argument('--bytes', type=int, default=16*1024*1024)
    parser.add_argument('--chunk-bytes', type=int, default=1024*1024)
    parser.add_argument('--gpu', action='store_true')
    parser.add_argument('--device', type=int, default=0)
    parser.add_argument('--request-id', default='manual')
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
    else:
        node = next(n for n in c['nodes'] if n['id'] == args.node)
        print(json.dumps(rpc(node['url'], 'consume', dict(key=args.key, gpu=args.gpu,
            device=args.device, request_id=args.request_id, chunk_bytes=args.chunk_bytes), timeout=900), indent=2))


if __name__ == '__main__':
    main()
