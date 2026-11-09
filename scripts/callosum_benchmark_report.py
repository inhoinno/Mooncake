#!/usr/bin/env python3
"""Common, offline-capable WRITE benchmark report. Never synthesize latency samples."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

REQUEST_FIELDS = ['trial_id','transfer_id','trial_status','path_mode','experiment_case','requested_page_mode','mr_page_mode','direction','block_bytes','repetition',
                  'request_index','request_bytes','native_calls','status',
                  'sender_start_offset_ns','sender_end_offset_ns','write_request_e2e_ns',
                  'write_request_GBps','write_request_GiBps']


def request_rates(sample, validated):
    """Derived payload rates, never wire rate or successful rate for unknown DMA."""
    elapsed=sample.get('write_request_e2e_ns')
    size=sample.get('request_bytes')
    valid=(validated and sample.get('status')=='PASS' and type(elapsed) is int and elapsed>0
           and type(size) is int and size>0)
    return dict(write_request_GBps=size/elapsed if valid else None,
                write_request_GiBps=size/2**30/(elapsed/1e9) if valid else None)


def request_records(row, trial_id=0):
    identity = {k:row.get(k,'') for k in REQUEST_FIELDS}
    identity.update(trial_id=trial_id,trial_status=row.get('status','UNKNOWN'),
                    transfer_id=row.get('transfer_id',row.get('attempt','')))
    for sample in row.get('requests') or []:
        yield dict(identity,**dict(sample,**request_rates(sample,row.get('status')=='PASS')))


def write_request_csv(path, row, trial_id=0):
    """Also preserve partial failed/unknown observations; never label them completed."""
    with Path(path).open('x',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=REQUEST_FIELDS,extrasaction='ignore')
        writer.writeheader(); writer.writerows(request_records(row,trial_id))


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered)*p)-1)]


def enrich_trial(row):
    row = dict(row)
    if row.get('status') != 'PASS' or row.get('actual_transport') != 'rdma':
        raise ValueError('only completed RDMA trials can enter the success report')
    if any(type(row.get(k)) is not int or row[k] <= 0 for k in ('bytes','block_bytes','sender_write_loop_ns')):
        raise ValueError('trial requires positive integer bytes and loop time')
    samples = row.get('requests')
    if row.get('request_timing_version') == 1 and samples is None:
        raise ValueError('instrumented trial is missing its request observations')
    if samples is not None:
        if (not isinstance(samples,list) or len(samples) != row['logical_blocks'] or
                len(samples) != (row['bytes']+row['block_bytes']-1)//row['block_bytes']):
            raise ValueError('missing request samples')
        cursor = 0
        for index, sample in enumerate(samples):
            begin, end = sample['sender_start_offset_ns'], sample['sender_end_offset_ns']
            if (any(type(sample.get(k)) is not int for k in ('request_index','request_bytes',
                    'sender_start_offset_ns','sender_end_offset_ns','write_request_e2e_ns')) or
                    sample['request_index'] != index or sample['status'] != 'PASS' or
                    not cursor <= begin < end <= row['sender_write_loop_ns'] or
                    sample['write_request_e2e_ns'] != end-begin or
                    sample['request_bytes'] != min(row['block_bytes'],row['bytes']-index*row['block_bytes']) or
                    type(sample['native_calls']) is not int or sample['native_calls'] <= 0):
                raise ValueError('invalid, incomplete or unordered request timing')
            cursor = end
        if (sum(s['request_bytes'] for s in samples) != row['bytes'] or
                sum(s['native_calls'] for s in samples) != row['native_calls']):
            raise ValueError('request byte/call accounting mismatch')
        samples=[dict(s,**request_rates(s,True)) for s in samples]
        row['requests']=samples
    row.update(repetition=row.get('repetition',0), loop_ms=row['sender_write_loop_ns']/1e6,
               GBps=row['bytes']/row['sender_write_loop_ns'],
               GiBps=row['bytes']/2**30/(row['sender_write_loop_ns']/1e9),
               request_samples=len(samples) if samples is not None else 0,
               latency_available=samples is not None)
    latencies = [s['write_request_e2e_ns'] for s in samples] if samples is not None else []
    stats = dict(unit='ns',count=len(latencies),available=samples is not None,
                 min=min(latencies) if latencies else None,max=max(latencies) if latencies else None,
                 mean=statistics.mean(latencies) if latencies else None,
                 stddev_sample=statistics.stdev(latencies) if len(latencies)>1 else None,
                 **{f'p{label}':percentile(latencies,p) for label,p in
                    [('50',.5),('90',.9),('95',.95),('99',.99),('99_9',.999)]})
    protocol = row.get('requester_rpc_ns',row.get('protocol_e2e_ns'))
    if protocol is not None and (type(protocol) is not int or protocol <= 0):
        raise ValueError('protocol timer must be positive integer nanoseconds when present')
    row.update(schema_version=2,
        latency=dict(write_request_e2e=stats,scope='sender logical WRITE start through drain; not KV/GPU-ready',
                     percentile_method='nearest_rank',correlated_serial_samples=True,
                     trial_protocol_e2e_ns=protocol),
        bandwidth=dict(sender_loop=dict(bytes_per_second=row['GBps']*1e9,
                        GB_per_second=row['GBps'],GiB_per_second=row['GiBps']),
                       trial_protocol=dict(GB_per_second=row['bytes']/protocol,
                            GiB_per_second=row['bytes']/2**30/(protocol/1e9)) if protocol else None,
                       useful_bytes=row['bytes'],scope='active sender window; not a sum of link rates'),
        wall_time=dict(sender_elapsed_ns=row['sender_write_loop_ns'],
                       trial_protocol_elapsed_ns=protocol,
                       sender_start_unix_ns=row.get('sender_start_unix_ns'),
                       sender_end_unix_ns=row.get('sender_end_unix_ns'),
                       controller_start_unix_ns=row.get('controller_start_unix_ns'),
                       controller_end_unix_ns=row.get('controller_end_unix_ns'),
                       controller_invocation_elapsed_ns=row.get('controller_invocation_elapsed_ns'),
                       duration_clock='monotonic_ns',timestamp_clock='time_ns; never subtract hosts'),
        measurement=dict(request_clock_domain='sender process',trial_clock_domain='initiating process',
                         source=row.get('source'),receiver=row.get('receiver_node',row.get('receiver')),
                         request_scope='logical WRITE start through terminal drain',
                         request_excludes=['seed','target preparation','control RPC','checksum','publication','GPU load'],
                         source_preparation_timed=False,memory_registration_timed=False,
                         reused_window=True,window_bytes=row['block_bytes'],
                         warmup_blocks=row.get('warmup_blocks'),
                         registration_state='pre-registered' if row.get('request_timing_version')==1 else 'UNRECORDED'),
        byte_accounting=dict(completed_payload_bytes=row['bytes'],
                             wire_bytes=None,wire_bytes_status='NOT_MEASURED',
                             checksum_scope=row.get('checksum_scope','final reused block window')),
        request_counts=dict(logical=row.get('logical_blocks'),native_calls=row.get('native_calls'),
                            observed=len(latencies),failed=0 if samples is not None else None,
                            missing_samples=(row.get('logical_blocks') if samples is None else 0)))
    return row


def write_report(output, rows):
    """Write one common schema for CXL and DRAM. Output is a fresh result directory."""
    output = Path(output)
    rows = [enrich_trial(row) for row in rows]
    if not rows:
        raise ValueError('no completed trials; an empty report is not PASS')
    paths = [output/name for name in ('trials.csv','requests.csv','summary.csv','report.md')]
    if any(path.exists() for path in paths):
        raise FileExistsError('report files already exist; preserve them')
    trial_fields = ['trial_id','transfer_id','path_mode','experiment_case','requested_page_mode','mr_page_mode','direction','block_unit','block_bytes','repetition',
                    'bytes','logical_blocks','native_calls','loop_ms','GBps','GiBps',
                    'warmup_blocks','warmup_bytes','requester_rpc_ns','protocol_e2e_ns',
                    'request_samples','latency_available','status']
    with paths[0].open('x',newline='') as trial_file, paths[1].open('x',newline='') as request_file:
        trials = csv.DictWriter(trial_file,fieldnames=trial_fields,extrasaction='ignore')
        requests = csv.DictWriter(request_file,fieldnames=REQUEST_FIELDS,extrasaction='ignore')
        trials.writeheader(); requests.writeheader()
        for index,row in enumerate(rows):
            identity = dict(row,trial_id=index,transfer_id=row.get('transfer_id',row.get('attempt','')))
            trials.writerow(identity)
            requests.writerows(request_records(row,index))
    aggregates = []
    def group_key(row):
        return (row['path_mode'],row.get('experiment_case',''),row.get('requested_page_mode',''),
                row.get('mr_page_mode','UNRECORDED'),row.get('direction',''),row['block_bytes'])
    for path,case,requested_mode,page_mode,direction,size in sorted({group_key(r) for r in rows}):
        group = [r for r in rows if group_key(r)==(path,case,requested_mode,page_mode,direction,size)]
        latencies = [s['write_request_e2e_ns'] for r in group for s in r.get('requests') or []]
        complete = all(r['latency_available'] for r in group)
        item = dict(path_mode=path,experiment_case=case,requested_page_mode=requested_mode,
            mr_page_mode=page_mode,direction=direction,block_bytes=size,trials=len(group),
            bytes=sum(r['bytes'] for r in group),loop_ms_p50=statistics.median(r['loop_ms'] for r in group),
            GBps=sum(r['bytes'] for r in group)/sum(r['sender_write_loop_ns'] for r in group),
            GiBps=sum(r['bytes'] for r in group)/2**30/(sum(r['sender_write_loop_ns'] for r in group)/1e9),
            trial_GBps_min=min(r['GBps'] for r in group),
            trial_GBps_p50=statistics.median(r['GBps'] for r in group),
            trial_GBps_max=max(r['GBps'] for r in group),
            trial_GBps_stddev_sample=statistics.stdev(r['GBps'] for r in group) if len(group)>1 else None,
            request_samples=len(latencies),latency_available=complete,
            write_p50_us=percentile(latencies,.50)/1000 if complete else None,
            write_p95_us=percentile(latencies,.95)/1000 if complete else None,
            write_p99_us=percentile(latencies,.99)/1000 if complete else None)
        aggregates.append(item)
    with paths[2].open('x',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(aggregates[0]))
        writer.writeheader(); writer.writerows(aggregates)
    text = ['# WRITE benchmark result','',
        'Scope: TRANSFER_ONLY. Serial logical WRITE → terminal drain. No GPU/serving claim.',
        'Throughput = total measured bytes / summed sender-loop time. GB/s is decimal.',
        'Instrumented loop times include sample-collection overhead; legacy instrumentation is not established.',
        'No file I/O occurs inside the instrumented loop.',
        'WRITE latency excludes preparation, metadata, verification and publication.',
        'Trial RPC/protocol time is separate; never assign it to individual WRITE requests.',
        'Percentiles use nearest rank over recorded requests. Serial samples are correlated;',
        'within-run percentiles are not confidence intervals or independent-trial statistics.',
        'Missing legacy samples remain unavailable; no latency is inferred from throughput.','',
        '| Path / direction | Block (MiB) | Trials | Loop p50 (ms) | GB/s | Latency samples | WRITE p50 / p95 / p99 (µs) |',
        '|---|---:|---:|---:|---:|---:|---|']
    for r in aggregates:
        latency = ' / '.join(f"{r[k]:.3f}" for k in ('write_p50_us','write_p95_us','write_p99_us')) if r['latency_available'] else 'UNAVAILABLE'
        text.append(f"| {r['experiment_case'] or r['path_mode']} / {r['mr_page_mode']} {r['direction']} | {r['block_bytes']/2**20:g} | {r['trials']} | "
                    f"{r['loop_ms_p50']:.2f} | {r['GBps']:.2f} | {r['request_samples']} | {latency} |")
    text.extend(['','Checksum scope: final reused block window, not every transient overwrite.',
                 'Per-trial rate dispersion is in summary.csv; no confidence interval is inferred from correlated requests.',
                 'Sixteen configured QPs do not prove sixteen active QPs.',
                 'Keep per-host module/NUMA/configuration evidence with these files.',''])
    paths[3].write_text('\n'.join(text))
    return dict(schema_version=1,report_module_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                trials=len(rows),request_samples=sum(r['request_samples'] for r in rows),
                latency_complete=all(r['latency_available'] for r in rows),aggregates=aggregates,
                files=[p.name for p in paths])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',required=True,help='Existing directory of block*.json trial files')
    parser.add_argument('--output',required=True,help='New report directory; never overwrite old evidence')
    args=parser.parse_args()
    inputs=sorted(Path(args.input).glob('block*.json'))
    if not inputs:
        parser.error('no block*.json inputs; no report generated')
    rows=[json.loads(path.read_text()) for path in inputs]
    output=Path(args.output)
    output.mkdir(parents=True,exist_ok=False)
    metadata=write_report(output,rows)
    (output/'summary.json').write_text(json.dumps(dict(status='PASS',evidence_class='TRANSFER_ONLY',
        analysis_only=True,report=metadata,results=rows),indent=2)+'\n')
    print((output/'report.md').read_text())
    print(f'[evidence] {output}')


if __name__=='__main__':
    main()
