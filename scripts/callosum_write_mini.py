#!/usr/bin/env python3
"""JSON-only CXL WRITE mini-benchmark driver. Reuse healthy services, never restart them."""
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import secrets
import statistics
import sys
import time

from callosum_lab import Runner, clean_env
from callosum_runtime import config as read_topology, require
from callosum_transport_benchmark import block_bytes, TOTAL_BYTES, access_strategy, MIXED_WRITE
from callosum_benchmark_report import enrich_trial, write_report, write_request_csv


def resolve(base, value):
    require(isinstance(value,str) and bool(value.strip()), 'nonempty configuration path required')
    path=Path(value)
    return (path if path.is_absolute() else base/path).resolve()


def load_spec(path, workers, mode_override=None):
    path=Path(path).resolve()
    spec=json.loads(path.read_text())
    require(spec.get('schema_version')==1,'mini config schema_version must be 1')
    require(set(spec) <= {'schema_version','topology_file','runtime','token_file','output','output_root',
                         'pairs','schedule','sizes','unit','repetitions','mr_page_mode','require_dax_2m',
                         'access_pattern','access_window_bytes','random_seed','diagnostic_driver',
                         'diagnostic_pair','sample_queues','synchronized','submission_mode',
                         'pipeline_depth','pipeline_max_bytes','descriptors_per_submit','pipeline_instrumentation','warmup_blocks'},'unknown mini config fields')
    require(spec.get('submission_mode') in (None,'serialization','batched','compare'),'unknown submission mode')
    require(not spec.get('submission_mode') or not spec.get('diagnostic_driver'),
            'native mode comparison is separate from old Python/C++ diagnostic drivers')
    for name,default,limit in (('pipeline_depth',4,4096),('pipeline_max_bytes',256<<20,1<<40),
                               ('descriptors_per_submit',1,1024)):
        require(type(spec.get(name,default)) is int and 0<spec.get(name,default)<=limit,'invalid '+name)
    require(spec.get('pipeline_instrumentation','off') in ('off','counters','sampled'),'invalid pipeline instrumentation')
    require(type(spec.get('warmup_blocks',0)) is int and 0<=spec.get('warmup_blocks',0)<=16,'invalid warmup')
    require(type(spec.get('require_dax_2m',False)) is bool,'require_dax_2m must be boolean')
    if mode_override is not None:
        spec['mr_page_mode']=mode_override
    require(workers in (2,4),'expected two or four workers')
    topology=resolve(path.parent,spec['topology_file'])
    c=read_topology(topology)
    require(c['mode']=='rdma' and (workers==2 or len(c['nodes'])==4),
            'wrong topology/substrate for this entry')
    require(spec.get('mr_page_mode','native') in ('native','huge2MB'),'unknown MR page mode')
    require(spec.get('mr_page_mode','native') == c.get('mr_page_mode','native'),
            'requested MR mode differs from topology; select mode before agent mapping/registration, not on a live MR')
    pairs=spec['pairs']
    nodes={n['id']:n for n in c['nodes']}
    require(isinstance(pairs,list) and pairs,'pairs must be a nonempty list')
    require(spec['schedule'] in ('serial_pairs','concurrent_pairs'),'unknown schedule')
    for pair in pairs:
        require(set(pair)=={'source','receiver'} and pair['source'] in nodes and pair['receiver'] in nodes,
                'pair requires source and receiver node IDs')
        source,receiver=nodes[pair['source']],nodes[pair['receiver']]
        require(source['pool']!=receiver['pool'] and source['can_send'] and receiver['can_receive'],
                'pair must have cross-pool send/receive capability')
    require(len({(p['source'],p['receiver']) for p in pairs})==len(pairs),'duplicate pair')
    if workers==2:
        require(len(pairs)==1,'two-worker mini requires exactly one pair')
    else:
        require(len(pairs)>=2 and {p[k] for p in pairs for k in ('source','receiver')}==set(nodes),
                'four-worker mini must exercise all four workers')
    if c.get('experiment_layout')=='colocated_pairs':
        require(workers==4 and len(pairs)==2, 'colocated benchmark must exercise both pairs')
    require(type(spec.get('synchronized',False)) is bool and (not spec.get('synchronized')
            or (c.get('experiment_layout')=='colocated_pairs' and spec['schedule']=='concurrent_pairs')),
            'synchronized transfer requires the colocated concurrent_pairs profile')
    driver,pair=spec.get('diagnostic_driver'),spec.get('diagnostic_pair')
    require(driver in (None,'python','native') and type(spec.get('sample_queues',False)) is bool and
            (not spec.get('sample_queues') or driver or spec.get('submission_mode')) and
            (pair is None or driver or spec.get('synchronized')) and
            (pair is None or pair in {p['source'] for p in pairs}), 'invalid diagnostic driver/pair')
    if pair is not None:
        pairs=spec['pairs']=[p for p in pairs if p['source']==pair]
    if spec['schedule']=='concurrent_pairs':
        endpoints=[p[k] for p in pairs for k in ('source','receiver')]
        require(len(endpoints)==len(set(endpoints)),'concurrent pairs must use disjoint endpoints')
    sizes=spec['sizes']; repetitions=spec['repetitions']
    require(isinstance(sizes,list) and sizes and all(type(n) is int for n in sizes) and
            len(sizes)==len(set(sizes)) and type(repetitions) is int and 0<repetitions<=100,
            'invalid sizes/repetitions')
    lengths=[block_bytes(n,spec['unit']) for n in sizes]
    require(spec.get('access_window_bytes') is None or
            (type(spec['access_window_bytes']) is int and spec['access_window_bytes']>0),'access window must be positive')
    if spec.get('access_pattern','overwrite')!='overwrite':
        spec.setdefault('access_window_bytes',max(lengths))
    for length in lengths:
        access_strategy(spec.get('access_pattern','overwrite'),length,
                        spec.get('access_window_bytes') or length,spec.get('random_seed',0))
    require(max(lengths)<=c['max_inflight_bytes'],'size exceeds running native request budget')
    rt=spec['runtime']
    if 'setup_run' in rt:
        require(set(rt)=={'setup_run'},'use setup_run OR explicit runtime paths, not both')
        receipt=json.loads((resolve(path.parent,rt['setup_run'])/'setup.json').read_text())
        require(receipt['status']=='PASS','runtime setup receipt is not PASS')
        env=clean_env()
        for name in ('PYTHON_BIN','CALLOSUM_NATIVE_BUILD','PYTHONPATH','LD_LIBRARY_PATH'):
            env[name]=receipt['env'][name]
    else:
        require(set(rt)=={'python','native_build','python_paths','library_paths'},'incomplete explicit runtime')
        require(isinstance(rt['python_paths'],list) and isinstance(rt['library_paths'],list),
                'runtime search paths must be lists')
        env=clean_env()
        env.update(PYTHON_BIN=str(resolve(path.parent,rt['python'])),
                   CALLOSUM_NATIVE_BUILD=str(resolve(path.parent,rt['native_build'])),
                   PYTHONPATH=os.pathsep.join(str(resolve(path.parent,p)) for p in rt['python_paths']),
                   LD_LIBRARY_PATH=os.pathsep.join(str(resolve(path.parent,p)) for p in rt['library_paths']))
    require(os.access(env['PYTHON_BIN'],os.X_OK),'runtime.python inaccessible; use the build owner account')
    secret_path=resolve(path.parent,spec['token_file'])
    secret=secret_path.read_text().strip()
    require(len(secret)>=32 and secret_path.stat().st_mode & 0o077 == 0,'private shared token needs mode 0600 and >=32 characters')
    env['CALLOSUM_CONTROL_TOKEN']=secret
    require(('output' in spec) != ('output_root' in spec),'choose output OR output_root')
    output=(resolve(path.parent,spec['output']) if 'output' in spec else
            resolve(path.parent,spec['output_root'])/('run-'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+secrets.token_hex(4)))
    require(not output.exists(),'output exists; never overwrite experiment evidence')
    # Static capacity bounds only. The live authority still checks all previous allocations.
    required={p:4096 for p in c['pools']}
    for pair in pairs:
        window=spec.get('access_window_bytes') or max(lengths)
        required[nodes[pair['source']]['pool']]+=max(max(lengths),window)
        required[nodes[pair['receiver']]['pool']]+=(window if spec.get('access_pattern','overwrite')!='overwrite' or spec.get('access_window_bytes') is not None
                                                   else sum(lengths)*repetitions)
    require(all(required[p]<=c['pools'][p]['capacity'] for p in required),'declared arena too small for planned retained windows')
    return spec,c,topology,env,output


def execute(spec,c,topology,env,output,runner):
    experiment_started=time.monotonic_ns()
    relay=Path(__file__).with_name('run_callosum_relay.sh')
    def command(stage,*args):
        raw=runner.call(stage,['bash',relay,*args,'--config',topology],env,
                        capture=True,log_stdout=not stage.startswith('block-'))
        row=json.loads(raw)
        return row
    effective=command('effective-config','check')
    status=command('status-before','status')
    (output/'effective-config.json').write_text(json.dumps(effective,indent=2)+'\n')
    (output/'status-before.json').write_text(json.dumps(status,indent=2)+'\n')
    require(status.get('fingerprint')==c['fingerprint']==effective['config']['fingerprint'] and
            status.get('topology_verified') and not status.get('startup_aborted') and
            set(status.get('registered_nodes',[]))=={n['id'] for n in c['nodes']} and
            all(o['state'] in ('COMMITTED','DRAINED_RETAINED') for o in status.get('operations',[])),
            'live campaign is not healthy or does not match configured topology')
    require(all(status.get('benchmark_request_timing_versions',{}).get(p['source'])==1 for p in spec['pairs']),
            'running sender lacks request timing; do not hot-restart retained services')
    driver=spec.get('diagnostic_driver')
    pipeline_mode=spec.get('submission_mode')
    synchronized=spec.get('synchronized',False)
    if synchronized:
        require(status.get('synchronized_transport_version')==1,
                'live campaign lacks synchronized transfer gates; no seed or WRITE sent')
        if not driver and spec.get('pipeline_instrumentation','off')=='off':
            require(not any(status.get('benchmark_diagnostic_tracking',{}).values()),
                    'disable native tracking for the formal synchronized sweep; no seed or WRITE sent')
    if pipeline_mode:
        require(all(status.get('benchmark_pipeline_versions',{}).get(p['source'])==1 for p in spec['pairs']),
                'native pipeline binding not loaded; rebuild/prepare before any seed or WRITE')
        require(spec.get('pipeline_instrumentation','off')=='off' or
                all(status.get('benchmark_diagnostic_tracking',{}).get(p['source']) for p in spec['pairs']),
                'pipeline counters require tracked sender startup; no seed or WRITE sent')
        require(spec.get('pipeline_instrumentation')!='sampled' or
                all(status.get('benchmark_slice_trace',{}).get(p['source']) for p in spec['pairs']),
                'sampled mode requires MC_SAMPLE_RDMA_WRITE_TRACE=1 at sender startup; no seed or WRITE sent')
    active_mode=None
    def mode_options():
        return (dict(submission_mode=active_mode,pipeline_depth=1 if active_mode=='serialization' else spec.get('pipeline_depth',4),
            pipeline_max_bytes=spec.get('pipeline_max_bytes',256<<20),
            descriptors_per_submit=1 if active_mode=='serialization' else spec.get('descriptors_per_submit',1),
            pipeline_instrumentation=spec.get('pipeline_instrumentation','off'),warmup_blocks=spec.get('warmup_blocks',0)) if active_mode else {})
    if driver:
        require(all(status.get('benchmark_diagnostic_versions',{}).get(n['id'])==1 for n in c['nodes']),
                'diagnostic needs rebuilt bindings on all endpoints; no seed or WRITE sent')
    if spec.get('sample_queues'):
        require(all(status.get('benchmark_diagnostic_tracking',{}).get(p['source']) is True for p in spec['pairs']),
                'start diagnostic senders with MC_TRACK_RDMA_POSTED_SLICES=1; no seed or WRITE sent')
    if c.get('experiment_layout')=='colocated_pairs':
        clocks=set()
        for pair in spec['pairs']:
            placement = status.get('dax_numa', {}).get(pair['source'], {})
            if c.get('require_remote_dax_numa',True):
                require(min(placement.get('dax_target_node',-1), placement.get('nic_numa_node',-1))>=0 and
                        placement['dax_target_node']!=placement['nic_numa_node'],
                        'running sender lacks NUMA-remote DAX evidence')
            clock = status.get('sender_clocks', {}).get(pair['source'], placement)
            require(clock.get('boot_id') and clock.get('time_namespace'),
                    'missing colocated source clock evidence')
            clocks.add((clock['boot_id'],clock['time_namespace']))
        require(len(clocks)==1,'the direct senders must share one host monotonic clock')
    if spec.get('access_pattern','overwrite')!='overwrite' or spec.get('access_window_bytes') is not None:
        require(spec.get('access_pattern','overwrite') in status.get('transport_access_patterns',[]),
                'running master lacks access-strategy support; no seed or WRITE sent')
        require(all(spec.get('access_pattern','overwrite') in status.get('benchmark_access_patterns',{}).get(n,[])
                    for p in spec['pairs'] for n in p.values()),
                'running agents lack access-strategy support; no seed or WRITE sent')
    page_mode=spec.get('mr_page_mode','native')
    pages=status.get('mr_pages',{})
    # Older native agents remain usable, but their page geometry is unknown.
    for pair in spec['pairs']:
        for member in pair.values():
            observed=pages.get(member) or {}
            require(observed.get('mode','native')==page_mode,
                    'live MR mode mismatch; do not relabel or re-register an active mapping')
            if c.get('path_mode') == MIXED_WRITE:
                node = next(n for n in c['nodes'] if n['id']==member)
                pool = c['pools'][node['pool']]
                require(observed.get('backing')=='cxl_mixed' and observed.get('device')==node['path']
                        and observed.get('map_offset_bytes')==pool['map_offset']
                        and observed.get('mapping_bytes')==pool['capacity'],
                        'live member did not register the configured mixed-CXL mapping')
            if page_mode=='huge2MB' or spec.get('require_dax_2m',False):
                require(observed.get('device_alignment_bytes')==2**21 and
                        observed.get('kernel_page_size_bytes')==2**21 and
                        observed.get('virtual_alignment_2MiB') is True,
                        'live member lacks strict 2 MiB DAX geometry evidence')
    rows=[]; groups=[]; sources={}
    for i,pair in enumerate(spec['pairs']):
        key='mini-'+secrets.token_hex(12)
        runner.remote_mutation_possible=True
        seed=command(f'seed-{i}','seed','--node',pair['source'],'--key',key,
                     '--bytes',max(spec.get('access_window_bytes') or 0,max(block_bytes(n,spec['unit']) for n in spec['sizes'])))
        (output/f'seed-{i}.json').write_text(json.dumps(seed,indent=2)+'\n')
        sources[i]=key
    def trial(i,pair,size,rep):
        name=(f'{active_mode}-' if active_mode else '')+f'block-{size}{spec["unit"]}-pair-{i}-rep-{rep}'
        begin_wall=time.time_ns(); begin=time.monotonic_ns()
        raw=runner.call(name,['bash',relay,'transport-benchmark','--node',pair['source'],
                    '--receiver',pair['receiver'],'--key',sources[i],'--block-size',size,
                 '--block-unit',spec['unit'],'--config',topology,
                 *(['--access-pattern',spec.get('access_pattern','overwrite'),'--access-window-bytes',str(spec['access_window_bytes']),
                    '--random-seed',str(spec.get('random_seed',0))] if spec.get('access_pattern','overwrite')!='overwrite' or spec.get('access_window_bytes') is not None else []),
                 *(['--diagnose',driver] if driver else []),
                 *(['--sample-queues'] if spec.get('sample_queues') else []),
                 *[arg for key,value in mode_options().items() for arg in ('--'+key.replace('_','-'),str(value))]],
                 env,capture=True,log_stdout=False)
        end=time.monotonic_ns(); end_wall=time.time_ns()
        return record_trial(name,i,pair,rep,raw,begin,end,begin_wall,end_wall)
    def record_trial(name,i,pair,rep,raw,begin,end,begin_wall,end_wall):
        # Preserve the exact response outside the controller timer, even if validation fails.
        # Do not match block*.json: offline reports must not count a trial twice.
        with (output/f'observed-{name}.json').open('x') as stream:
            stream.write(raw)
        row=json.loads(raw)
        if driver:
            require(row.get('diagnostic_driver')==driver and row.get('diagnostic_requests') and
                    row.get('controller_diagnostic_ns'), 'native diagnostic evidence is missing')
            phases=row['diagnostic_requests']
            row['diagnostic_phase_totals_ns']={
                key:sum(p[key] for p in phases) for key in
                ('check_ns','submit_ns','wait_ns','between_calls_ns','in_call_residual_ns')}
            row['diagnostic_phase_totals_ns']['tail_ns']=row['diagnostic_loop_tail_ns']
            row['diagnostic_phase_totals_ns']['native_schedule_ns']=row.get('native_schedule_build_ns',0)
        if spec.get('access_pattern','overwrite')!='overwrite' or spec.get('access_window_bytes') is not None:
            require(row.get('access_pattern')==spec.get('access_pattern','overwrite') and
                    row.get('window_bytes')==spec['access_window_bytes'] and
                    row.get('random_seed')==spec.get('random_seed',0),
                    'benchmark result does not match requested strategy/window/seed')
        require(row.get('bytes')==TOTAL_BYTES and row.get('request_timing_version')==1,
                'missing byte accounting or request timings')
        if c.get('path_mode') == MIXED_WRITE:
            require(row.get('path_mode')==MIXED_WRITE and row.get('actual_transport')=='rdma',
                    'mixed-CXL result did not use the requested native RDMA path')
        row.update(direction=f'{pair["source"]}->{pair["receiver"]}',repetition=rep,
                   source=pair['source'],receiver=pair['receiver'],pair_index=i,
                   mr_page_mode=page_mode,
                   dax_2m_required=spec.get('require_dax_2m',False),
                   mr_pages={n:pages.get(n) for n in pair.values()},
                   warmup_blocks=row.get('warmup_blocks',0),warmup_bytes=row.get('warmup_bytes',0),
                   controller_invocation_elapsed_ns=end-begin,
                   controller_invocation_start_monotonic_ns=begin,controller_invocation_end_monotonic_ns=end,
                   controller_invocation_start_unix_ns=begin_wall,controller_invocation_end_unix_ns=end_wall,
                   mini_config_sha256=runner.config_sha256)
        row['topology_fingerprint']=c['fingerprint']
        row=enrich_trial(row)
        with (output/(name+'.json')).open('x') as stream: json.dump(row,stream,indent=2)
        write_request_csv(output/(name+'-requests.csv'),row)
        return row
    modes=['serialization','batched'] if pipeline_mode=='compare' else [pipeline_mode]
    for active_mode,size,rep in ((m,s,r) for s in spec['sizes'] for r in range(spec['repetitions']) for m in modes):
            start_wall=time.time_ns(); start=time.monotonic_ns()
            if synchronized:
                begin_wall,begin=time.time_ns(),time.monotonic_ns()
                requests=[dict(node=p['source'],receiver=p['receiver'],key=sources[i],
                    block_size=size,block_unit=spec['unit'],window_bytes=spec.get('access_window_bytes'),
                    access_pattern=spec.get('access_pattern','overwrite'),random_seed=spec.get('random_seed',0),
                    diagnostic_driver=driver,sample_queues=spec.get('sample_queues',False),**mode_options())
                    for i,p in enumerate(spec['pairs'])]
                label=(f'{active_mode}-' if active_mode else '')+f'group-{size}{spec["unit"]}-rep-{rep}'
                raw=runner.call(label,['bash',relay,'transport-group','--config',topology],env,
                                capture=True,log_stdout=False,input_text=json.dumps(dict(pairs=requests)))
                end,end_wall=time.monotonic_ns(),time.time_ns()
                (output/f'observed-{label}.json').write_text(raw)
                result=json.loads(raw)
                require(isinstance(result,list) and len(result)==len(requests) and
                        all(r.get('trial_group') and r['trial_group']==result[0]['trial_group'] for r in result),
                        'synchronized group lacks complete, correlated drain evidence')
                group=[record_trial((f'{active_mode}-' if active_mode else '')+f'block-{size}{spec["unit"]}-pair-{i}-rep-{rep}',
                        i,p,rep,json.dumps(r),begin,end,begin_wall,end_wall)
                       for i,(p,r) in enumerate(zip(spec['pairs'],result))]
            elif spec['schedule']=='concurrent_pairs':
                # All futures are joined on errors. Never equate an RPC timeout with drain.
                with ThreadPoolExecutor(max_workers=len(spec['pairs'])) as pool:
                    jobs=[pool.submit(trial,i,p,size,rep) for i,p in enumerate(spec['pairs'])]
                    group=[job.result() for job in jobs]
            else:
                group=[trial(i,p,size,rep) for i,p in enumerate(spec['pairs'])]
            elapsed=max(r['controller_invocation_end_monotonic_ns'] for r in group)-start
            end_wall=max(r['controller_invocation_end_unix_ns'] for r in group)
            group_id=(f'{active_mode}-' if active_mode else '')+f'{size}{spec["unit"]}-rep-{rep}'
            common=dict(group_id=group_id,schedule=spec['schedule'],block_bytes=block_bytes(size,spec['unit']),
                        repetition=rep,controller_elapsed_ns=elapsed,start_unix_ns=start_wall,end_unix_ns=end_wall,
                        useful_bytes=sum(r['bytes'] for r in group),
                        aggregate_protocol_GBps=sum(r['bytes'] for r in group)/elapsed,
                        scope='one controller window includes CLI startup, RPC, verification and JSON return; excludes final report writing; not sender-loop sum')
            if active_mode:
                common.update(mode_options())
            if c.get('experiment_layout')=='colocated_pairs':
                starts=[r['sender_loop_start_monotonic_ns'] for r in group]
                ends=[start+r['sender_write_loop_ns'] for start,r in zip(starts,group)]
                common.update(sender_common_window_ns=max(ends)-min(starts),
                              sender_start_skew_ns=max(starts)-min(starts),
                              sender_loop_overlap_ns=max(0,min(ends)-max(starts)),
                              synchronized=synchronized,
                              aggregate_sender_GBps=common['useful_bytes']/(max(ends)-min(starts)))
                if spec.get('sample_queues'):
                    common['sampled_progress']=sampled_progress(group, max(starts), min(ends))
            groups.append(common)
            (output/f'group-{group_id}.json').write_text(json.dumps(common,indent=2)+'\n')
            for row in group:
                rows.append(row)
                runner.say(f"[PASS] {row['direction']} {size} {spec['unit']}: {row['GBps']:.3f} GB/s")
    report=write_report(output,rows)
    sender_groups=[]
    if c.get('experiment_layout')=='colocated_pairs':
        for size,mode in ((s,m) for s in spec['sizes'] for m in modes):
            trials=[g for g in groups if g['block_bytes']==block_bytes(size,spec['unit']) and g.get('submission_mode')==mode]
            rates=[g['aggregate_sender_GBps'] for g in trials]
            sender_groups.append(dict(size=size,unit=spec['unit'],repetitions=len(trials),
                submission_mode=mode,
                weighted_GBps=sum(g['useful_bytes'] for g in trials)/
                              sum(g['sender_common_window_ns'] for g in trials),
                min_GBps=min(rates),median_GBps=statistics.median(rates),max_GBps=max(rates)))
    with (output/'groups.csv').open('x',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(groups[0]))
        writer.writeheader(); writer.writerows(groups)
    with (output/'report.md').open('a') as stream:
        if sender_groups:
            stream.write('\n## Common sender transfer windows\n\n'
                'Total completed payload divided by the earliest-to-latest sender loop span. '
                'Submit, progress, wait and sender skew are timed. '
                'Only the synchronized profile excludes peer preparation and verification from this span.\n\n'
                f'Phase gates: {"synchronized" if synchronized else "independent"}.\n\n'
                '| Block | Trials | Weighted GB/s | Min / median / max GB/s |\n'
                '|---|---:|---:|---|\n')
            for g in sender_groups:
                stream.write(f"| {g['size']} {g['unit']} | {g['repetitions']} | {g['weighted_GBps']:.3f} | "
                             f"{g['min_GBps']:.3f} / {g['median_GBps']:.3f} / {g['max_GBps']:.3f} |\n")
        stream.write('\n## Common controller windows\n\n')
        stream.write('These rates include control/CLI/result-return cost. They are not summed sender-loop bandwidth.\n\n')
        stream.write('| Group | Schedule | Wall time (ms) | Useful GiB | Protocol GB/s |\n|---|---|---:|---:|---:|\n')
        for g in groups:
            stream.write(f"| {g['group_id']} | {g['schedule']} | {g['controller_elapsed_ns']/1e6:.3f} | "
                         f"{g['useful_bytes']/2**30:g} | {g['aggregate_protocol_GBps']:.3f} |\n")
        if driver:
            stream.write(f'\n## Diagnostic runs — {driver} loop\n\n'
                'Diagnostic tracing is not a final throughput measurement. Native wait includes RDMA progress.\n'
                'Times are milliseconds; sender phases are not added to controller phases.\n\n'
                '| Pair | MiB | Rep | Target prepare | Native checks | Submit | Wait | Between calls | In-call residual | Verification |\n'
                '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n')
            for r in rows:
                p, ctime=r['diagnostic_phase_totals_ns'],r['controller_diagnostic_ns']
                parts=[ctime['receiver_prepare'],p['check_ns'],p['submit_ns'],p['wait_ns'],
                       p['between_calls_ns'],p['in_call_residual_ns'],ctime['verification']]
                stream.write(f"| {r['direction']} | {r['block_bytes']/2**20:g} | {r['repetition']} | " +
                             ' | '.join(f'{ns/1e6:.3f}' for ns in parts) + ' |\n')
    (output/'summary.json').write_text(json.dumps(dict(status='PASS',evidence_class='TRANSFER_ONLY',
        experiment_elapsed_ns=time.monotonic_ns()-experiment_started,
        execution_clock_scope='controller execute entry through report completion; excludes final summary serialization',
        measured_payload_bytes=sum(r['bytes'] for r in rows),warmup_payload_bytes=sum(r.get('warmup_bytes',0) for r in rows),
        report=report,results=rows,groups=groups,sender_group_summary=sender_groups,
        services_retained=True,
        measurement_scope='DIAGNOSTIC_ONLY' if driver else 'TRANSPORT_BENCHMARK',
        experiment_layout=c.get('experiment_layout','standard'),
        multipath_object_assembly_proven=False),indent=2)+'\n')
    runner.say((output/'report.md').read_text())
    runner.say(f'[evidence] {output}')


def sampled_progress(group, begin, end):
    """Conservative completed-byte lower bound inside a shared middle window."""
    if end<=begin or len(group)<2:
        return dict(status='NO_PAIRED_OVERLAP')
    trim=(end-begin)//5
    begin,end=begin+trim,end-trim
    results={}
    port_deltas={}
    for row in group:
        usable=[s for s in row.get('sender_queue_samples',[]) if s.get('contexts') and
                begin<=s['begin_ns']<=s['end_ns']<=end and all(x['tracked'] for x in s['contexts'])]
        if len(usable)<2:
            return dict(status='INSUFFICIENT_SAMPLES')
        def counters(sample):
            return {(x['device'],x['port']):x['completed_write_bytes'] for x in sample['contexts']}
        first,last=counters(usable[0]),counters(usable[-1])
        if first.keys()!=last.keys() or any(last[k]<first[k] for k in first):
            return dict(status='COUNTER_CHANGED')
        results[row['direction']]=sum(last[k]-first[k] for k in first)
        # Use one observation series per hardware port, even when two engines
        # both report it. These are port-wide counters, not per-pair payload.
        for x in usable[0]['contexts']:
            identity=f"{x['device']}/{x['port']}"
            before=x.get('port_tx_counter_bytes')
            after=next(y.get('port_tx_counter_bytes') for y in usable[-1]['contexts']
                       if f"{y['device']}/{y['port']}"==identity)
            if before is not None and after is not None and after>=before:
                port_deltas.setdefault(identity,after-before)
    return dict(status='BOTH_ADVANCED' if all(results.values()) else 'NOT_BOTH_OBSERVED_ADVANCING',
                begin_ns=begin,end_ns=end,completed_bytes_lower_bound=results,
                aggregate_GBps_lower_bound=sum(results.values())/(end-begin),
                sender_port_tx_counter_deltas_bytes=port_deltas)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers',type=int,choices=(2,4),required=True,
                        help='Experiment participants: one pair, or all four topology members')
    parser.add_argument('--config',required=True,help='Mini experiment JSON; no environment variables required')
    parser.add_argument('--mode',choices=('native','huge2MB'),
                        help='Required live MR mode; does not modify existing mappings (also configurable as mr_page_mode)')
    args=parser.parse_args()
    runner=None
    try:
        # A CLI selector must agree with topology before any service RPC or seed.
        spec,c,topology,env,output=load_spec(args.config,args.workers,args.mode)
        output.mkdir(parents=True)
        runner=Runner(output); runner.remote_mutation_possible=False
        runner.config_sha256=hashlib.sha256(Path(args.config).read_bytes()).hexdigest()
        public=dict(spec); public['token_file']='[private file path omitted]'
        public['topology_file_sha256']=hashlib.sha256(topology.read_bytes()).hexdigest()
        public['driver_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        (output/'experiment.json').write_text(json.dumps(public,indent=2)+'\n')
        execute(spec,c,topology,env,output,runner)
        return 0
    except Exception as error:
        mutated=bool(runner and runner.remote_mutation_possible)
        failure=dict(status='FAIL',stage=runner.stage if runner else 'configuration-validation',error=str(error),
                     local_services_started=False,local_devices_mapped=False,remote_mutation_possible=mutated,
                     action=('Retain existing services, journals and mappings. No automatic retry or reclaim.' if mutated
                             else 'No seed/WRITE sent. Correct the JSON or access and rerun; no permission repair attempted.'))
        if runner:
            (runner.run_dir/'failure.json').write_text(json.dumps(failure,indent=2)+'\n')
        print(json.dumps(failure,indent=2),file=sys.stderr)
        return 1
    finally:
        if runner: runner.log.close()


if __name__=='__main__':
    sys.exit(main())
