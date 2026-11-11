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
import sys
import time

from callosum_lab import Runner, clean_env
from callosum_runtime import config as read_topology, require
from callosum_transport_benchmark import block_bytes, TOTAL_BYTES, access_strategy
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
                         'access_pattern','access_window_bytes','random_seed'},'unknown mini config fields')
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
        name=f'block-{size}{spec["unit"]}-pair-{i}-rep-{rep}'
        begin_wall=time.time_ns(); begin=time.monotonic_ns()
        raw=runner.call(name,['bash',relay,'transport-benchmark','--node',pair['source'],
                    '--receiver',pair['receiver'],'--key',sources[i],'--block-size',size,
                 '--block-unit',spec['unit'],'--config',topology,
                 *(['--access-pattern',spec.get('access_pattern','overwrite'),'--access-window-bytes',str(spec['access_window_bytes']),
                    '--random-seed',str(spec.get('random_seed',0))] if spec.get('access_pattern','overwrite')!='overwrite' or spec.get('access_window_bytes') is not None else [])],
                 env,capture=True,log_stdout=False)
        end=time.monotonic_ns(); end_wall=time.time_ns()
        # Preserve the exact response outside the controller timer, even if validation fails.
        # Do not match block*.json: offline reports must not count a trial twice.
        with (output/f'observed-{name}.json').open('x') as stream:
            stream.write(raw)
        row=json.loads(raw)
        if spec.get('access_pattern','overwrite')!='overwrite' or spec.get('access_window_bytes') is not None:
            require(row.get('access_pattern')==spec.get('access_pattern','overwrite') and
                    row.get('window_bytes')==spec['access_window_bytes'] and
                    row.get('random_seed')==spec.get('random_seed',0),
                    'benchmark result does not match requested strategy/window/seed')
        require(row.get('bytes')==TOTAL_BYTES and row.get('request_timing_version')==1,
                'missing byte accounting or request timings')
        row.update(direction=f'{pair["source"]}->{pair["receiver"]}',repetition=rep,
                   source=pair['source'],receiver=pair['receiver'],pair_index=i,
                   mr_page_mode=page_mode,
                   dax_2m_required=spec.get('require_dax_2m',False),
                   mr_pages={n:pages.get(n) for n in pair.values()},
                   warmup_blocks=0,warmup_bytes=0,
                   controller_invocation_elapsed_ns=end-begin,
                   controller_invocation_start_monotonic_ns=begin,controller_invocation_end_monotonic_ns=end,
                   controller_invocation_start_unix_ns=begin_wall,controller_invocation_end_unix_ns=end_wall,
                   mini_config_sha256=runner.config_sha256)
        row['topology_fingerprint']=c['fingerprint']
        row=enrich_trial(row)
        with (output/(name+'.json')).open('x') as stream: json.dump(row,stream,indent=2)
        write_request_csv(output/(name+'-requests.csv'),row)
        return row
    for size in spec['sizes']:
        for rep in range(spec['repetitions']):
            start_wall=time.time_ns(); start=time.monotonic_ns()
            if spec['schedule']=='concurrent_pairs':
                # All futures are joined on errors. Never equate an RPC timeout with drain.
                with ThreadPoolExecutor(max_workers=len(spec['pairs'])) as pool:
                    jobs=[pool.submit(trial,i,p,size,rep) for i,p in enumerate(spec['pairs'])]
                    group=[job.result() for job in jobs]
            else:
                group=[trial(i,p,size,rep) for i,p in enumerate(spec['pairs'])]
            elapsed=max(r['controller_invocation_end_monotonic_ns'] for r in group)-start
            end_wall=max(r['controller_invocation_end_unix_ns'] for r in group)
            group_id=f'{size}{spec["unit"]}-rep-{rep}'
            common=dict(group_id=group_id,schedule=spec['schedule'],block_bytes=block_bytes(size,spec['unit']),
                        repetition=rep,controller_elapsed_ns=elapsed,start_unix_ns=start_wall,end_unix_ns=end_wall,
                        useful_bytes=sum(r['bytes'] for r in group),
                        aggregate_protocol_GBps=sum(r['bytes'] for r in group)/elapsed,
                        scope='one controller window includes CLI startup, RPC, verification and JSON return; excludes final report writing; not sender-loop sum')
            groups.append(common)
            (output/f'group-{group_id}.json').write_text(json.dumps(common,indent=2)+'\n')
            for row in group:
                rows.append(row)
                runner.say(f"[PASS] {row['direction']} {size} {spec['unit']}: {row['GBps']:.3f} GB/s")
    report=write_report(output,rows)
    with (output/'groups.csv').open('x',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(groups[0]))
        writer.writeheader(); writer.writerows(groups)
    with (output/'report.md').open('a') as stream:
        stream.write('\n## Common controller windows\n\n')
        stream.write('These rates include control/CLI/result-return cost. They are not summed sender-loop bandwidth.\n\n')
        stream.write('| Group | Schedule | Wall time (ms) | Useful GiB | Protocol GB/s |\n|---|---|---:|---:|---:|\n')
        for g in groups:
            stream.write(f"| {g['group_id']} | {g['schedule']} | {g['controller_elapsed_ns']/1e6:.3f} | "
                         f"{g['useful_bytes']/2**30:g} | {g['aggregate_protocol_GBps']:.3f} |\n")
    (output/'summary.json').write_text(json.dumps(dict(status='PASS',evidence_class='TRANSFER_ONLY',
        report=report,results=rows,groups=groups,services_retained=True,
        multipath_object_assembly_proven=False),indent=2)+'\n')
    runner.say((output/'report.md').read_text())
    runner.say(f'[evidence] {output}')


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
