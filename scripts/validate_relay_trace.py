"""Validate completed TRANSFER_ONLY relay traces; never infer serving reuse."""
import argparse
import json
from pathlib import Path


def validate(events, transfer_id, expected_bytes, transport):
    if type(expected_bytes) is not int or expected_bytes <= 0:
        raise ValueError('expected bytes must be positive')
    selected = [e for e in events if e.get('transfer_id') == transfer_id]
    for event in selected:
        if event.get('evidence_class') != 'TRANSFER_ONLY':
            raise ValueError('missing or unsupported evidence class')
        if any(k in event for k in ('key','payload','rkey','token','authorization','remote_address')):
            raise ValueError('ordinary trace exposes restricted fields')
    required = ['request_begin','lookup','reserved','targets_ready','dispatch_begin',
                'writes_complete','verified','published','handles_released']
    stages = [e['stage'] for e in selected if e['stage'] in required]
    if stages != required:
        raise ValueError('missing, duplicate, or out-of-order coordinator stages')
    phase_events = [e for e in selected if e['stage'] in required]
    if len({e['node'] for e in phase_events}) != 1:
        raise ValueError('coordinator stages span different clocks')
    times=[e['monotonic_ns'] for e in phase_events]
    if times != sorted(times):
        raise ValueError('coordinator clock goes backward')
    if any(e.get('bytes') != expected_bytes for e in phase_events[1:]):
        raise ValueError('coordinator object byte count mismatch')
    if any(e['stage'] in ('failed','request_failed') for e in selected):
        raise ValueError('failed transfer cannot pass validation')
    chunks = [e for e in selected if e['stage']=='chunk_complete']
    batches = [e for e in selected if e['stage']=='batch_complete']
    if not chunks or not batches:
        raise ValueError('missing payload evidence')
    if len({e['chunk'] for e in chunks}) != len(chunks):
        raise ValueError('duplicate chunk completion')
    cursor = 0
    for c in sorted(chunks, key=lambda e:e['object_byte_start']):
        if c['object_byte_start'] != cursor or c['bytes'] <= 0:
            raise ValueError('gap, overlap or invalid chunk size')
        cursor += c['bytes']
    if cursor != expected_bytes:
        raise ValueError('wrong complete-object byte coverage')
    groups = {(b['sender'],b['batch_id']): b for b in batches}
    if len(groups) != len(batches):
        raise ValueError('duplicate batch completion')
    if set(groups) != {(c['sender'],c['batch_id']) for c in chunks}:
        raise ValueError('unmatched batch/chunk evidence')
    by_batch = {identity: [] for identity in groups}
    for chunk in chunks:
        by_batch[(chunk['sender'],chunk['batch_id'])].append(chunk)
    for identity,b in groups.items():
        members = by_batch[identity]
        if b['opcode'] != 'WRITE' or b['actual_transport'] != transport:
            raise ValueError('payload transport/opcode mismatch')
        expected_path = 'direct_dax_write' if transport == 'rdma' else 'file_copy'
        if b.get('path_mode') != expected_path:
            raise ValueError('payload path mode mismatch')
        if len(members) != b['cardinality'] or sum(c['bytes'] for c in members)!=b['bytes']:
            raise ValueError('batch cardinality/byte mismatch')
    return dict(status='PASS', evidence_class='TRANSFER_ONLY', transport=transport,
                useful_bytes=cursor, chunks=len(chunks), batches=len(batches),
                serving_proven=False)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--logs', nargs='+', required=True)
    p.add_argument('--transfer-id', required=True)
    p.add_argument('--bytes', type=int, required=True)
    p.add_argument('--transport', choices=['rdma','file-copy'], required=True)
    a=p.parse_args()
    events=[json.loads(line) for path in a.logs for line in Path(path).read_text().splitlines() if line]
    print(json.dumps(validate(events,a.transfer_id,a.bytes,a.transport),indent=2))


if __name__=='__main__':
    main()
