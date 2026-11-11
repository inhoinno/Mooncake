#!/usr/bin/env python3
"""Real Store integration probe. No prototype runtime, allocator, or DMA model.

The source and requester use this program with the same experiment JSON.
The report command requires both native client logs and both probe receipts.
"""
import argparse
import ctypes
import hashlib
import importlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import resource
import subprocess
import secrets
import uuid

EXTENT_CONFIRMATION_REQUIRED = 'operator_confirmation_required_at_run'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def host(endpoint):
    # The bounded two-worker probe requires numeric IPv4 endpoints.
    require(isinstance(endpoint, str) and ':' in endpoint, 'endpoint must be an IPv4:port string')
    address, port = endpoint.rsplit(':', 1)
    ipaddress.IPv4Address(address)
    require(0 < int(port) < 65536, 'endpoint port outside 1..65535')
    return address


def load_config(path):
    path = Path(path).resolve()
    data = path.read_bytes()
    c = validate_config(json.loads(data), path)
    c['config_sha256'] = hashlib.sha256(data).hexdigest()
    return c


def validate_config(c, path):
    require(c.get('schema_version') == 1, 'schema_version must be 1')
    require(re.fullmatch(r'[A-Za-z0-9_-]{8,80}', c.get('run_id', '')),
            'run_id must be a fresh shared identifier (8..80 characters)')
    require(type(c.get('block_bytes')) is int and 0 < c['block_bytes'] <= 64 * 1024**2,
            'block_bytes must be 1..67108864; this is a correctness probe')
    require(type(c.get('wait_seconds')) is int and 0 < c['wait_seconds'] <= 300,
            'wait_seconds must be 1..300 (the native bridge bound)')
    host(c['master_address'])
    require(c.get('metadata') == 'P2PHANDSHAKE', 'probe requires P2PHANDSHAKE')
    require(c['source'] != c['requester'] and
            set(c['nodes']) == {c['source'], c['requester']}, 'exactly two distinct nodes required')
    require(all(re.fullmatch(r'[A-Za-z0-9_-]{1,64}', n) for n in c['nodes']), 'invalid node identifier')
    pools, hosts = set(), set()
    for node in c['nodes'].values():
        hosts.add(host(node['local_hostname']))
        require(node.get('rdma_device'), 'rdma_device required')
        require(node.get('store_module','mooncake.store') in ('mooncake.store','store'),
                'store_module must be the installed package or the native CMake build module')
        b = node['bridge']
        pools.add(b['pool_id'])
        require(b.get('backend_kind') == 'devdax' and Path(b['path']).is_absolute(),
                'live probe requires an absolute devdax path')
        for field in ('capacity', 'alignment', 'owned_size'):
            require(type(b.get(field)) is int and b[field] > 0, field + ' must be positive')
        for field in ('mapping_offset', 'owned_offset'):
            require(type(b.get(field)) is int and b[field] >= 0, field + ' must be nonnegative')
        require(b['mapping_offset'] % b['alignment'] == 0 and
                b['capacity'] % b['alignment'] == 0 and
                b['owned_offset'] % b['alignment'] == 0 and
                b['owned_size'] % b['alignment'] == 0, 'unaligned owned mapping')
        require(b['owned_offset'] + b['owned_size'] <= b['capacity'] and
                b['owned_size'] >= 4 * c['block_bytes'], 'owned arena too small or out of bounds')
        require(node.get('extent_authorization'), 'record the exclusive extent owner/authorization')
        require(type(b.get('mux_capacity',2)) is int and 0 < b.get('mux_capacity',2) <= 32 and
                type(b.get('mux_queue_limit',32)) is int and 0 < b.get('mux_queue_limit',32) <= 4096,
                'mux capacity/queue limit outside native bounds')
        require(node.get('expected_store_sha256') and
                re.fullmatch(r'[0-9a-f]{64}', node['expected_store_sha256']),
                'expected_store_sha256 must identify the built Store module')
        b['token_file'] = str((path.parent / b['token_file']).resolve())
    require(len(pools) == 2 and len(hosts) == 2, 'two independent pool IDs and physical hosts required')
    return c


def payload(c):
    pattern = hashlib.sha256(c['run_id'].encode()).digest()
    return (pattern * ((c['block_bytes'] + 31) // 32))[:c['block_bytes']]


def key(c):
    return 'callosum-store-proof/' + c['run_id'] + '/kv'


def crc64(data):
    value = 0
    for byte in data:
        value ^= byte << 56
        for _ in range(8):
            value = ((value << 1) ^ (0x42F0E1EBA9EA3693 if value >> 63 else 0)) & ((1 << 64) - 1)
    return value


def snapshot(store, module, name):
    result = []
    for replica in store.get_replica_desc(name):
        require(replica.is_memory_replica() and replica.status == module.ReplicaStatus.COMPLETE,
                'lookup returned a non-complete memory replica')
        d = replica.get_memory_descriptor().buffer_descriptor
        require(d.protocol == 'rdma', 'lookup returned non-RDMA payload replica')
        result.append(dict(endpoint=d.transport_endpoint, bytes=int(d.size), protocol=d.protocol))
    return sorted(result, key=lambda row: row['endpoint'])


def expect_replicas(rows, endpoints, size):
    require(len(rows) == len(endpoints) and
            {host(r['endpoint']) for r in rows} == set(endpoints) and
            all(r['bytes'] == size and r['protocol'] == 'rdma' for r in rows),
            'replica ownership/cardinality/size differs from the required Store state')


def write_json(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def dax_geometry(device, sysroot=Path('/sys')):
    info = device.lstat()
    require(device.parent == Path('/dev') and re.fullmatch(r'dax[0-9]+\.[0-9]+', device.name) and
            stat.S_ISCHR(info.st_mode), 'expected a direct DAX character-device path')
    geometry = sysroot / 'bus/dax/devices' / device.name
    return info, int((geometry / 'size').read_text().strip(), 0), int((geometry / 'align').read_text().strip(), 0)


def native_store_hash():
    module = importlib.import_module('store')
    require(getattr(module, 'CALLOSUM_STORE_ABI', None) == 1 and
            getattr(module, 'CALLOSUM_STORE_MUX_ABI', None) == 1,
            'Store binding lacks Store ABI=1 / native mux ABI=1')
    native = Path(module.__file__).resolve()
    build = Path(sys.executable).absolute().parent.parent.parent
    require(native.parent == (build / 'mooncake-integration').resolve(),
            f'expected the Store module from the interpreter\'s build tree, got {native}')
    return hashlib.sha256(native.read_bytes()).hexdigest()


def private_token(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
            info.st_mode & 0o077 == 0 and len(path.read_bytes().strip()) >= 32,
            'private owned shared token file (>=32 characters) required')


def initialize(args):
    """Collect one read-only host receipt, or assemble both receipts on M2."""
    require(args.template, 'init requires --template')
    require(os.geteuid() != 0, 'init must run as the benchmark account')
    raw = Path(args.template).read_bytes()
    c = json.loads(raw)
    template_sha = hashlib.sha256(raw).hexdigest()
    if args.node:
        require(args.node in c['nodes'] and not args.node_input, 'select one template node')
        out = Path(args.output or f'store-proof.{args.node}.json').resolve()
        require(not os.path.lexists(out), f'{out} exists; preserve it or choose a new --output')
        node = c['nodes'][args.node]
        b = node['bridge']
        require(sys.platform.startswith('linux'), 'node init requires the Linux lab host')
        address = host(node['local_hostname'])
        links = json.loads(subprocess.check_output(['ip', '-j', '-4', 'address', 'show'], text=True))
        require(any(a['local'] == address for link in links for a in link.get('addr_info', [])),
                f'{address} is not a local IPv4 address for {args.node}')
        hca = node['rdma_device']
        require(re.fullmatch(r'[A-Za-z0-9_.-]+', hca) and hca not in ('.', '..') and
                (Path('/sys/class/infiniband') / hca).is_dir(), f'RDMA device {hca} not found')
        _, device_size, align = dax_geometry(Path(b['path']))
        digest = native_store_hash()
        offset, size = b['mapping_offset'], b['capacity']
        owned_offset, owned_size = b['owned_offset'], b['owned_size']
        require(all(type(v) is int for v in (offset, size, owned_offset, owned_size, b['alignment'])) and
                align > 0 and b['alignment'] >= align and b['alignment'] % align == 0 and
                offset >= 0 and size > 0 and owned_offset >= 0 and
                owned_size >= 4 * c['block_bytes'] and owned_offset + owned_size <= size and
                all(v % b['alignment'] == 0 for v in (offset, size, owned_offset, owned_size)) and
                offset + size <= device_size,
                f'{b["path"]}: configured range is too small, unaligned, or outside device; '
                f'device size={device_size}, device alignment={align}')
        node.update(store_module='store', expected_store_sha256=digest,
                    extent_authorization=EXTENT_CONFIRMATION_REQUIRED)
        write_json(out, dict(node=args.node, template_sha256=template_sha, config=node))
        return dict(status='NODE_INPUT_SAVED', node=args.node, path=str(out), device=b['path'],
                    mapping_offset=offset, capacity=size, alignment=b['alignment'], devices_mapped=False)

    require(args.config and len(args.node_input) == 2, 'assembly requires --config and two --node-input files')
    out = Path(args.config).resolve()
    require(not os.path.lexists(out), f'{out} exists; preserve it or choose a new --config path')
    receipts = [json.loads(Path(p).read_text()) for p in args.node_input]
    require(all(r['template_sha256'] == template_sha for r in receipts) and
            {r['node'] for r in receipts} == set(c['nodes']), 'node inputs must match the same template and cover both nodes')
    c['nodes'] = {r['node']: r['config'] for r in receipts}
    c['run_id'] = uuid.uuid4().hex
    c['master_address'] = args.master_address or c['master_address']
    validate_config(json.loads(json.dumps(c)), out)
    tokens = {node['bridge']['token_file'] for node in c['nodes'].values()}
    require(len(tokens) == 1, 'both nodes must use the same token file')
    token = out.parent / tokens.pop()
    require(token.parent == out.parent and token.name == 'store-proof.control.token',
            'init requires the dedicated store-proof.control.token beside the config')
    if not os.path.lexists(token):
        fd = os.open(token, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            stream.write(secrets.token_hex(32) + '\n')
    private_token(token)
    write_json(out, c)
    return dict(status='CONFIG_SAVED', config=str(out), token_file=str(token),
                run_id=c['run_id'], devices_mapped=False)


def prepare_resources(bridge, block_bytes, output, authorize=False, sysroot=Path('/sys')):
    """Reuse the benchmark ACL policy; elevate only ACL/prlimit commands.

    No mapping, payload write, hugepage mutation, chown or root worker.
    """
    from callosum_benchmark_prepare import dax_acl_change
    device = Path(bridge['path'])
    record = dict(status='CHECKING', uid=os.geteuid(), device=str(device), changes=[])
    authenticated = False
    def command(argv):
        result = subprocess.run(argv, text=True, stdout=subprocess.PIPE, check=True)
        return result.stdout
    def sudo():
        nonlocal authenticated
        require(authorize, 'resource preparation required; use check --prepare-resources after extent approval')
        if not authenticated:
            subprocess.run(['sudo', '-v'], check=True)
            authenticated = True
    try:
        info, size, alignment = dax_geometry(device, sysroot)
        require(bridge['mapping_offset'] + bridge['capacity'] <= size and
                bridge['mapping_offset'] % alignment == 0 and bridge['capacity'] % alignment == 0,
                'approved mapping exceeds device geometry or alignment')
        record.update(device_size=size, device_alignment=alignment, device_number=info.st_rdev)
        try:
            fd = os.open(device, os.O_RDWR | os.O_NOFOLLOW)
            os.close(fd)
        except PermissionError:
            sudo()
            before = command(['getfacl', '-c', '-p', '-n', '--', str(device)])
            change = dax_acl_change(before, os.geteuid(), info.st_uid)
            current = device.lstat()
            require((current.st_rdev, current.st_ino, current.st_mode) ==
                    (info.st_rdev, info.st_ino, info.st_mode), 'DAX identity changed before ACL grant')
            record['changes'].append(dict(operation='dax_acl', requested=change,
                scope='DEVICE_WIDE numeric-UID grant; not extent isolation', before=before))
            command(['sudo', '-n', 'setfacl', '--no-mask', '-m', change, '--', str(device)])
            record['acl_after'] = command(['getfacl', '-c', '-p', '-n', '--', str(device)])
            fd = os.open(device, os.O_RDWR | os.O_NOFOLLOW)
            os.close(fd)  # Verify under the original UID, with no payload access.
        needed = bridge['owned_size'] + max(block_bytes, 16 * 1024**2) + block_bytes
        soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        record.update(memlock_before=[soft, hard], required_memlock_bytes=needed)
        if soft != resource.RLIM_INFINITY and soft < needed:
            sudo()
            target_hard = 'unlimited' if hard == resource.RLIM_INFINITY else str(max(hard, needed))
            record['changes'].append(dict(operation='current_process_memlock', pid=os.getpid()))
            command(['sudo', '-n', 'prlimit', '--pid', str(os.getpid()), '--memlock='+str(needed)+':'+target_hard])
        soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        require(soft == resource.RLIM_INFINITY or soft >= needed, 'scoped prlimit did not raise caller memlock')
        record.update(status='PASS', memlock_after=[soft, hard], devices_mapped=False, services_started=False)
        return record
    except Exception as error:
        record.update(status='FAIL', error=str(error))
        raise
    finally:
        write_json(Path(output) / 'resources.json', record)


def wait_for(check, seconds, description):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.1)
    raise TimeoutError(description + '; no automatic retry or resource reclamation')


def source(store, module, c):
    name = key(c)
    require(not snapshot(store, module, name) and store.is_exist(name + '/done') == 0,
            'run_id already exists; do not overwrite historical objects')
    data = payload(c)
    require(store.put(name, data) == 0, 'Store PUT failed')
    initial = snapshot(store, module, name)
    own = host(c['nodes'][c['source']]['local_hostname'])
    expect_replicas(initial, {own}, len(data))
    print(json.dumps(dict(status='SOURCE_READY', key=name, replicas=initial)), flush=True)
    wait_for(lambda: store.is_exist(name + '/done') == 1, c['wait_seconds'], 'requester completion missing')
    final = snapshot(store, module, name)
    expect_replicas(final, {host(n['local_hostname']) for n in c['nodes'].values()}, len(data))
    return dict(before=initial, after=final, payload_sha256=hashlib.sha256(data).hexdigest())


def requester(store, module, c, retained):
    name = key(c)
    initial = wait_for(lambda: snapshot(store, module, name), c['wait_seconds'], 'source key missing')
    expect_replicas(initial, {host(c['nodes'][c['source']]['local_hostname'])}, c['block_bytes'])
    require(store.is_exist(name + '/done') == 0, 'completion key already exists')
    buffer = ctypes.create_string_buffer(c['block_bytes'])
    retained.append(buffer)  # Unknown native outcomes must not free this backing.
    address = ctypes.addressof(buffer)
    require(store.register_buffer(address, c['block_bytes']) == 0, 'consumer buffer registration failed')
    expected = hashlib.sha256(payload(c)).hexdigest()
    measurements = []
    all_hosts = {host(n['local_hostname']) for n in c['nodes'].values()}
    for phase in ('materialize', 'local_reuse'):
        ctypes.memset(address, 0, c['block_bytes'])
        start = time.monotonic_ns()
        result = store.batch_get_into_multi_buffers([name], [[address]], [[c['block_bytes']]])
        elapsed = time.monotonic_ns() - start
        require(result == [c['block_bytes']], 'Store multi-buffer load failed')
        require(hashlib.sha256(buffer.raw).hexdigest() == expected, 'consumer SHA-256 mismatch')
        rows = snapshot(store, module, name)
        expect_replicas(rows, all_hosts, c['block_bytes'])
        measurements.append(dict(phase=phase, elapsed_ns=elapsed, replicas=rows, payload_sha256=expected))
    require(measurements[0]['replicas'] == measurements[1]['replicas'], 'reuse changed replica placement')
    require(store.unregister_buffer(address) == 0, 'consumer unregister failed')
    retained.clear()  # Both synchronous loads completed and validated.
    require(store.put(name + '/done', expected.encode()) == 0, 'completion publication failed')
    return dict(before=initial, loads=measurements, payload_sha256=expected)


def events(path):
    result = []
    for line in Path(path).read_text().splitlines():
        if 'component=callosum_store ' in line:
            result.append(dict(re.findall(r'([a-z_]+)=([^\s]+)', line)))
    return result


def verify(c, source_receipt, requester_receipt, source_log, requester_log):
    for node_id, receipt in ((c['source'], source_receipt), (c['requester'], requester_receipt)):
        require(receipt.get('status') == 'CLIENT_PATH_VALIDATED_NATIVE_REVIEW_REQUIRED' and
                receipt.get('node') == node_id and receipt.get('run_id') == c['run_id'] and
                receipt.get('config_sha256') == c['config_sha256'] and
                receipt.get('store_sha256') == c['nodes'][node_id]['expected_store_sha256'],
                'receipt identity/config/module does not match this experiment')
    expected = hashlib.sha256(payload(c)).hexdigest()
    require(source_receipt['result']['payload_sha256'] == requester_receipt['result']['payload_sha256'] == expected,
            'source/requester payload mismatch')
    source_host = host(c['nodes'][c['source']]['local_hostname'])
    all_hosts = {host(n['local_hostname']) for n in c['nodes'].values()}
    expect_replicas(source_receipt['result']['before'], {source_host}, c['block_bytes'])
    expect_replicas(requester_receipt['result']['before'], {source_host}, c['block_bytes'])
    expect_replicas(source_receipt['result']['after'], all_hosts, c['block_bytes'])
    loads = requester_receipt['result']['loads']
    require([r['phase'] for r in loads] == ['materialize', 'local_reuse'], 'both load phases required')
    for load in loads:
        expect_replicas(load['replicas'], all_hosts, c['block_bytes'])
        require(load['payload_sha256'] == expected and load['elapsed_ns'] > 0, 'load evidence incomplete')
    require(loads[0]['replicas'] == loads[1]['replicas'], 'reuse changed placement')
    tx, rx = events(source_log), events(requester_log)
    key_crc = str(crc64(key(c).encode()))
    auth = [r for r in tx if r.get('event') == 'receiver_authorized' and r.get('key_crc') == key_crc]
    require(len(auth) == 1, 'exactly one receiver authorization required for this key')
    a = auth[0]
    require(a.get('source_pool') == c['nodes'][c['source']]['bridge']['pool_id'] and
            a.get('destination_pool') == c['nodes'][c['requester']]['bridge']['pool_id'] and
            int(a.get('bytes', 0)) == c['block_bytes'] and a.get('receiver_epoch'),
            'authorization pool/extent/epoch mismatch')
    attempt = a['attempt']
    drained = [r for r in tx if r.get('event') == 'write_drained' and r.get('attempt') == attempt]
    require(drained, 'native WRITE drain evidence missing')
    source_endpoint = source_receipt['result']['before'][0]['endpoint']
    receiver_host = host(c['nodes'][c['requester']]['local_hostname'])
    receiver_endpoint = next(r['endpoint'] for r in loads[0]['replicas'] if host(r['endpoint']) == receiver_host)
    require({r.get('sender') for r in drained} == {source_endpoint} and a.get('receiver') == receiver_endpoint,
            'actual native sender/receiver differs from Store placement')
    end = 0
    for r in drained:
        require(r.get('protocol') == 'rdma' and r.get('opcode') == 'WRITE' and
                r.get('source_generation') == a['source_generation'] and
                r.get('target_generation') == a['target_generation'], 'WRITE identity/protocol mismatch')
        end += int(r['batch_bytes'])
        require(int(r['completed_end']) == end, 'native batch coverage gap/overlap')
    require(end == c['block_bytes'], 'native drained bytes differ from object length')
    valid = [r for r in rx if r.get('event') == 'receiver_validated' and r.get('attempt') == attempt]
    require(valid and all(r.get('content_ok') == '1' and int(r['bytes']) == end and
                         r.get('epoch') == a['receiver_epoch'] for r in valid),
            'receiver content/visibility validation missing or failed')
    copied = [r for r in tx if r.get('event') == 'copy_verified' and r.get('attempt') == attempt]
    published = [r for r in tx if r.get('event') == 'replica_published' and r.get('key_crc') == key_crc]
    require(len(copied) == len(published) == 1 and
            copied[0].get('key_crc') == key_crc and int(copied[0]['bytes']) == end and
            published[0].get('source_generation') == a['source_generation'] and
            published[0].get('target_generation') == a['target_generation'], 'publication identity mismatch')
    require(tx.index(a) < tx.index(drained[0]) <= tx.index(drained[-1]) <
            tx.index(copied[0]) < tx.index(published[0]), 'publication preceded native drain/validation')
    mux = {}
    for event in ('mux_queued', 'mux_admitted', 'mux_gathered'):
        rows = [r for r in tx if r.get('event') == event and r.get('attempt') == attempt]
        require(len(rows) == 1 and int(rows[0]['bytes']) == end, 'native Store-to-mux evidence missing')
        mux[event] = rows[0]
    queued, admitted, gathered = (mux[n] for n in ('mux_queued', 'mux_admitted', 'mux_gathered'))
    require(tx.index(a) < tx.index(queued) < tx.index(admitted) < tx.index(drained[0]) <=
            tx.index(drained[-1]) < tx.index(gathered) < tx.index(copied[0]),
            'Store-to-mux execution order invalid')
    require(int(admitted['timestamp_ns'])-int(queued['timestamp_ns']) == int(admitted['queue_wait_ns']) and
            int(gathered['timestamp_ns'])-int(admitted['timestamp_ns']) == int(gathered['dispatch_ns']),
            'source-clock mux span mismatch')
    misses = [r for r in rx if r.get('event') == 'initial_local_miss' and r.get('key_crc') == key_crc]
    require(len(misses) == 1, 'initial miss/reuse evidence inconsistent')
    materializations = [r for r in rx if r.get('event') == 'remote_materialization' and r.get('key_crc') == key_crc]
    completed_loads = [r for r in rx if r.get('event') == 'connector_load_complete' and r.get('key_crc') == key_crc]
    require(len(materializations) == 1 and len(completed_loads) == 2 and
            all(r.get('success') == '1' and int(r['bytes']) == end for r in completed_loads),
            'native materialization/local-reuse load evidence missing')
    tasks = [r for r in tx if r.get('event') == 'store_task_dispatch' and r.get('key_crc') == key_crc]
    require(len(tasks) == 1 and tasks[0].get('task') == materializations[0].get('task') and tasks[0].get('task'),
            'requester/source Store task correlation missing')
    usable = [r for r in rx if r.get('event') == 'replica_usable' and r.get('key_crc') == key_crc]
    require([r.get('phase') for r in usable] == ['remote', 'local'] and
            all(int(r['request_to_usable_replica_ns']) > 0 for r in usable), 'usable-replica timing missing')
    samples = [dict(bytes=int(r['batch_bytes']), latency_ns=int(r['native_write_ns']),
                    start_ns=int(r['native_start_ns']), end_ns=int(r['native_end_ns'])) for r in drained]
    require(all(r['latency_ns'] > 0 and r['end_ns']-r['start_ns'] == r['latency_ns'] for r in samples),
            'native terminal timing missing/inconsistent')
    require(all(int(admitted['timestamp_ns']) <= r['start_ns'] < r['end_ns'] <= int(gathered['timestamp_ns']) for r in samples) and
            all(a['end_ns'] <= b['start_ns'] for a,b in zip(samples,samples[1:])),
            'native timing lies outside the source dispatch window')
    transport_ns = sum(r['latency_ns'] for r in samples)  # One actual source endpoint, serial batches.
    ready_ns = int(usable[0]['request_to_usable_replica_ns'])
    requests = [dict(request_index=i,request_bytes=r['bytes'],native_calls=1,status='PASS',
                     sender_start_offset_ns=r['start_ns']-samples[0]['start_ns'],
                     sender_end_offset_ns=r['end_ns']-samples[0]['start_ns'],
                     write_request_e2e_ns=r['latency_ns']) for i,r in enumerate(samples)]
    return dict(status='PASS', scope='TRANSFER_ONLY', gate='NATIVE_STORE_TO_MUX', run_id=c['run_id'],
                export_origin='seeded_store_put', transport='rdma', opcode='WRITE',
                native_batches=len(drained), bytes=end, attempt=attempt,
                requests=requests, path_mode='store_mux_direct_dax_write',
                experiment_case='STORE_TO_MUX_NATIVE_BATCH', block_bytes=end,
                source_generation=a['source_generation'], target_generation=a['target_generation'],
                store_key_path_validated=True, destination_local_replica_reuse=True,
                store_to_mux_validated=True, store_task=tasks[0]['task'],
                participants=dict(sender=source_endpoint, receiver=receiver_endpoint,
                                  physical_multi_source='NOT_EXERCISED'),
                performance=dict(request_to_usable_replica_ns=ready_ns,
                    local_replica_lookup_ns=int(usable[1]['request_to_usable_replica_ns']),
                    source_queue_wait_ns=int(admitted['queue_wait_ns']),
                    source_dispatch_gather_ns=int(gathered['dispatch_ns']),
                    serial_native_write_ns=transport_ns, native_write_samples=samples,
                    useful_bytes=end, logical_write_payload_bytes=end,
                    wire_bytes='UNAVAILABLE_NO_NIC_ATTRIBUTION',
                    transport_GBps=end/transport_ns, request_to_replica_GBps=end/ready_ns,
                    timing='separate requester/source monotonic clocks; spans must not be summed'),
                gpu_delivery_proven=False, serving_proven=False,
                physical_pool_isolation='REQUIRES_SEPARATE_LAB_PREFLIGHT',
                performance_baseline='NOT_APPLICABLE')


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['init', 'check', 'run', 'verify'])
    p.add_argument('--config')
    p.add_argument('--node')
    p.add_argument('--template', help='init: shared experiment template')
    p.add_argument('--node-input', action='append', default=[], help='init: one receipt per node')
    p.add_argument('--master-address', help='init: override the template master IPv4:port')
    p.add_argument('--confirm-exclusive-extent', action='store_true',
                   help='run: confirm both pools are physically independent and this node owns the approved, drained test extent')
    p.add_argument('--output', help='new evidence directory; default runs/store-<run_id>/<node or action>')
    p.add_argument('--prepare-resources', action='store_true', help='check: scoped sudo DAX ACL/current-process memlock preparation')
    for name in ('source-receipt', 'requester-receipt', 'source-log', 'requester-log'):
        p.add_argument('--' + name)
    args = p.parse_args(argv)
    store = None
    retained = []
    out = None
    created = False
    stage = 'configuration'
    try:
        if args.action == 'init':
            stage = 'configuration-initialization'
            print(json.dumps(initialize(args), indent=2))
            return 0
        require(args.config, '--config is required')
        c = load_config(args.config)
        require(os.geteuid() != 0, 'run as the benchmark account, not sudo; no output created')
        if args.action != 'verify':
            require(args.node in c['nodes'], 'select a configured --node')
            if args.action == 'run':
                if c['nodes'][args.node]['extent_authorization'] == EXTENT_CONFIRMATION_REQUIRED:
                    require(args.confirm_exclusive_extent,
                            'confirm the configured DAX range is approved and free of other writers or uncertain DMA with --confirm-exclusive-extent')
                else:
                    require(os.environ.get('CALLOSUM_DESTRUCTIVE_ACK') == '1',
                            'CALLOSUM_DESTRUCTIVE_ACK=1 required after exclusive extent authorization')
        else:
            require(args.node is None, 'verify combines both node receipts; do not pass --node')
        leaf = args.node if args.action == 'run' else args.action + ('-'+args.node if args.node else '')
        out = (Path(args.output) if args.output else Path('runs') / ('store-'+c['run_id']) / leaf).resolve()
        out.mkdir(parents=True, mode=0o700, exist_ok=False)
        created = True
        if args.action == 'verify':
            stage = 'native-trace-verification'
            values = []
            for name in ('source_receipt', 'requester_receipt'):
                values.append(json.loads(Path(getattr(args, name)).read_text()))
            result = verify(c, *values, args.source_log, args.requester_log)
            write_json(out / 'summary.json', result)
            from callosum_benchmark_report import write_request_csv
            write_request_csv(out / 'requests.csv', result)
            print(json.dumps(result, indent=2))
            return 0
        require(args.node in c['nodes'], 'select a configured --node')
        node = c['nodes'][args.node]
        stage = 'local-import-preflight'
        require(sys.platform.startswith('linux'), 'real Store/RDMA probe requires Linux; no file-mode substitution')
        b = dict(node['bridge'])
        token = Path(b['token_file'])
        private_token(token)
        require(stat.S_ISCHR(Path(b['path']).stat().st_mode), 'local DAX path is not a character device')
        require('MC_CXL_DEV_PATH' not in os.environ, 'unset incompatible MC_CXL_DEV_PATH')
        require(not any(n in os.environ for n in ('MC_USE_TENT','MC_USE_TEV1')),
                'unset TENT/TEV1 switches; this Store bridge uses classic RDMA')
        os.environ['GLOG_logtostderr'] = '1'
        module = importlib.import_module(node.get('store_module','mooncake.store'))
        require(getattr(module, 'CALLOSUM_STORE_ABI', None) == 1 and
                getattr(module, 'CALLOSUM_STORE_MUX_ABI', None) == 1,
                'Store binding lacks Store ABI=1 / native mux ABI=1; rebuild Store, not only engine.so')
        native = Path(module.__file__).resolve()
        digest = hashlib.sha256(native.read_bytes()).hexdigest()
        require(digest == node['expected_store_sha256'], 'imported Store module differs from the approved build')
        evidence = dict(node=args.node, run_id=c['run_id'], config_sha256=c['config_sha256'],
                        store_module=str(native), store_sha256=digest, platform=sys.platform,
                        rdma_device=node['rdma_device'],
                        operator_extent_confirmation=args.action == 'run' and args.confirm_exclusive_extent,
                        transport_env={name:os.environ.get(name,'default') for name in
                                       ('MC_NUM_QP_PER_EP','MC_GID_INDEX','MC_MTU')})
        write_json(out / 'preflight.json', evidence)
        stage = 'local-resource-preparation'
        prepare_resources(b, c['block_bytes'], out, authorize=args.prepare_resources or args.action == 'run')
        if args.action == 'check':
            print(json.dumps(dict(evidence, status='PASS', scope='IMPORT_CONFIG_RESOURCES_ONLY',
                                  evidence=str(out), devices_mapped=False)))
            return 0
        b.update(campaign=c['run_id'], journal=str(out / 'bridge.journal'), wait_ms=c['wait_seconds'] * 1000)
        write_json(out / 'bridge.json', b)
        os.environ['MC_CALLOSUM_STORE_CONFIG'] = str(out / 'bridge.json')
        log_fd = os.open(out / 'native.log', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.dup2(log_fd, 2)
        os.close(log_fd)
        print(json.dumps(dict(stage='native-store-start', evidence=str(out), native_log=str(out/'native.log'))), flush=True)
        store = module.MooncakeDistributedStore()
        stage = 'native-store-setup'
        require(store.setup(node['local_hostname'], c['metadata'], 0, max(c['block_bytes'], 16 * 1024**2),
                            'rdma', node['rdma_device'], c['master_address']) == 0, 'real Store setup failed')
        stage = 'store-load-through-mux'
        result = source(store, module, c) if args.node == c['source'] else requester(store, module, c, retained)
        write_json(out / 'receipt.json', dict(evidence, result=result,
                   status='CLIENT_PATH_VALIDATED_NATIVE_REVIEW_REQUIRED'))
        # Keep both native owners alive for evidence collection and positive-drain
        # retirement. The existing master retains allocation ownership as well.
        print(json.dumps(dict(status='CLIENT_PATH_VALIDATED_NATIVE_REVIEW_REQUIRED',
                              receipt=str(out / 'receipt.json'), services_retained=True)), flush=True)
        while True:
            time.sleep(60)
    except Exception as error:
        failure = dict(status='FAIL', stage=stage, error=str(error), store_object_created=store is not None,
                       action='No automatic reclaim. Retain native owners if setup or a WRITE may have started.'
                       if store is not None else 'Correct inputs and rerun; preserve existing files and evidence.')
        if created and (out / 'resources.json').exists():
            failure['resource_preparation'] = json.loads((out / 'resources.json').read_text())
        if created and not (out / 'failure.json').exists():
            write_json(out / 'failure.json', failure)
        print(json.dumps(failure, indent=2), flush=True)
        if store is not None:
            while True:  # Preserve possible DMA, registrations and consumer backing.
                time.sleep(60)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
