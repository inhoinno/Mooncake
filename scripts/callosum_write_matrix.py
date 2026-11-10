#!/usr/bin/env python3
"""Matched CXL/local-NUMA x native/huge2MB WRITE matrix. Reuse existing engines."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse

from callosum_lab import Runner, clean_env
from callosum_runtime import config, require, rpc
from callosum_transport_benchmark import block_bytes, TOTAL_BYTES
from callosum_dram_benchmark import allocation_geometry, hugepage_capacity, source_identity
from callosum_benchmark_report import write_report


def resolve(base,value):
    require(isinstance(value,str) and value.strip(),'nonempty path required')
    return (base/Path(value)).resolve()


def load(path,args):
    path=Path(path).resolve(); base=path.parent
    spec=json.loads(path.read_text())
    required_fields={'schema_version','topology_file','token_file','output_root',
                     'runtimes','pair','numa_nodes','control_ports','te_ports'}
    require(required_fields<=set(spec) and set(spec)<=required_fields|{'transport_env'},
            'unknown/missing matrix fields')
    require(spec['schema_version']==1,'matrix schema_version must be 1')
    topology=resolve(base,spec['topology_file']); c=config(topology)
    require(c['mode']=='rdma','matrix requires RDMA; no TCP/file fallback')
    pair=spec['pair']; nodes={n['id']:n for n in c['nodes']}
    require(set(pair)=={'source','receiver'} and all(n in nodes for n in pair.values()),'invalid pair')
    require(nodes[pair['source']]['pool']!=nodes[pair['receiver']]['pool'] and
            nodes[pair['source']]['can_send'] and nodes[pair['receiver']]['can_receive'],'ineligible cross-pool pair')
    require(args.node in pair.values(),'node not in the configured pair')
    if args.action in ('receive','run'):
        require(args.node==pair['receiver' if args.action=='receive' else 'source'],
                'node does not match the selected pair action; host names do not select roles')
    lengths=[block_bytes(n,args.unit) for n in args.sizes]
    require(args.sizes and len(set(args.sizes))==len(args.sizes) and 0<args.repetitions<=100,
            'invalid sweep/repetitions')
    if 'cxl' in args.backings and args.action=='run':
        require(max(lengths)<=c.get('max_inflight_bytes',64*1024**2),
                'CXL size exceeds live max_inflight_bytes; set the startup budget before creating the campaign')
        required=sum(lengths)*args.repetitions*len(args.page_modes)+4096
        require(required<=c['pools'][nodes[pair['receiver']]['pool']]['capacity'],
                'CXL destination arena too small for all retained matrix windows; no reclaim is permitted')
        require(max(lengths)*len(args.page_modes)+4096<=c['pools'][nodes[pair['source']]['pool']]['capacity'],
                'CXL source arena too small for the retained source windows')
    require(set(spec['runtimes'])==set(pair.values()),'one host-local runtime receipt per pair member required')
    receipt=resolve(base,spec['runtimes'][args.node]['setup_run'])
    require(set(spec['runtimes'][args.node])=={'setup_run'},'runtime needs setup_run')
    setup=json.loads((receipt/'setup.json').read_text())
    require(setup['status']=='PASS','host-local setup receipt must be PASS')
    env=clean_env()
    for key in tuple(env):
        if key.startswith('MC_'): env.pop(key)
    env['MC_NUM_QP_PER_EP']='16'
    transport=spec.get('transport_env',{})
    require(isinstance(transport,dict) and set(transport)<=set(nodes),'invalid transport_env node IDs')
    for options in transport.values():
        require(isinstance(options,dict) and set(options)<={'MC_MTU','MC_GID_INDEX'},'unsupported transport_env option')
        mtu=options.get('MC_MTU'); gid=options.get('MC_GID_INDEX')
        require(mtu is None or (type(mtu) is int and mtu in (256,512,1024,2048,4096)), 'invalid software RDMA MTU')
        require(gid is None or (type(gid) is int and gid>=0),'invalid GID index')
    env.update({k:str(v) for k,v in transport.get(args.node,{}).items() if v is not None})
    for key in ('PYTHON_BIN','PYTHONPATH','LD_LIBRARY_PATH','CALLOSUM_NATIVE_BUILD'):
        env[key]=setup['env'][key]
    require(os.access(env['PYTHON_BIN'],os.X_OK),'host-local Python is inaccessible; use its owning account')
    private=resolve(base,spec['token_file']); secret=private.read_text().strip()
    require(len(secret)>=32 and private.stat().st_mode & 0o077==0,'shared token requires >=32 characters and mode 0600')
    env['CALLOSUM_CONTROL_TOKEN']=secret
    require(set(spec['control_ports'])==set(spec['te_ports'])=={'native','huge2MB'},'configure both page-mode ports')
    ports=list(spec['control_ports'].values())+list(spec['te_ports'].values())
    occupied={urlparse(c['master']).port,*[urlparse(n['url']).port for n in c['nodes']],
              *[int(n['te_endpoint'].rsplit(':',1)[1]) for n in c['nodes']]}
    require(all(type(p) is int and 1<=p<=65535 for p in ports) and len(set(ports))==4 and
            not set(ports)&occupied,'matrix ports must be distinct and separate from campaign ports')
    require(set(spec['numa_nodes'])<=set(nodes) and
            all(type(v) is int and v>=0 for v in spec['numa_nodes'].values()),'invalid NUMA overrides')
    return spec,c,topology,receipt,private,env,resolve(base,spec['output_root'])


def dram_command(spec,args,topology,receipt,private,output,mode,role,env):
    pair=spec['pair']; peer=pair['source' if role=='receiver' else 'receiver']
    result=[env['PYTHON_BIN'],str(Path(__file__).with_name('callosum_dram_benchmark.py')),
        '--role',role,'--config',str(topology),'--node',args.node,'--peer',peer,
        '--setup-run',str(receipt),'--token-file',str(private),'--page-mode',mode,
        '--control-port',str(spec['control_ports'][mode]),'--te-port',str(spec['te_ports'][mode]),
        '--unit',args.unit,'--repetitions',str(args.repetitions),'--warmup-blocks','0',
        '--output',str(output),'--sizes',*[str(n) for n in args.sizes]]
    if args.node in spec['numa_nodes']: result+=['--numa-node',str(spec['numa_nodes'][args.node])]
    return result


def receiver_health(spec,c,args,mode):
    receiver=next(n for n in c['nodes'] if n['id']==spec['pair']['receiver'])
    url=f"http://{urlparse(receiver['url']).hostname}:{spec['control_ports'][mode]}"
    health=rpc(url,'health',timeout=5)
    options=spec.get('transport_env',{}).get(spec['pair']['receiver'],{})
    expected_transport={k:str(options[k]) if options.get(k) is not None else None for k in ('MC_MTU','MC_GID_INDEX')}
    require(health['node_id']==spec['pair']['receiver'] and health['peer_id']==spec['pair']['source'] and
            health['fingerprint']==c['fingerprint'] and health['state']=='IDLE' and
            not health.get('sealed',False) and
            health.get('transport_env')==expected_transport and
            health['source_identity']==source_identity() and health['mr_pages']['mode']==mode and
            health['capacity']>=max(block_bytes(n,args.unit) for n in args.sizes),
            'receiver mode, size, source, peer or lifetime state differs; no automatic restart')
    if mode=='huge2MB':
        require(health['mr_pages'].get('hugetlb') is True and
                health['mr_pages'].get('kernel_page_bytes')==2**21,
                'receiver lacks actual HugeTLB/2 MiB allocation evidence')
    return health


def receive(spec,c,args,topology,receipt,private,env,out,runner):
    runner.stage='receiver-local-numa-preflight'
    require('local_numa' in args.backings,'receive action is only needed for local_numa')
    node=next(n for n in c['nodes'] if n['id']==args.node)
    reused=[]; missing=[]
    for mode in args.page_modes:
        try: receiver_health(spec,c,args,mode); reused.append(mode)
        except Exception: missing.append(mode)
    if not missing:
        runner.say(f'[READY REUSED] compatible receivers={reused}; no process or reservation added')
        return
    numa=spec['numa_nodes'].get(args.node)
    if numa is None: numa=int((Path('/sys/class/infiniband')/node['rdma_device']/'device/numa_node').read_text())
    require(numa>=0,'NIC NUMA is unknown; configure a reviewed numa_nodes override')
    size=max(block_bytes(n,args.unit) for n in args.sizes)
    # Each mode uses a separate child process; RLIMIT_MEMLOCK is per process.
    required=max(allocation_geometry(size,m) for m in args.page_modes)
    soft,_=resource.getrlimit(resource.RLIMIT_MEMLOCK)
    require(soft==resource.RLIM_INFINITY or soft>=required,
            f'each receiver process needs memlock >= {required} bytes')
    if 'huge2MB' in missing: hugepage_capacity(numa,size)
    for mode in missing:
        for port in (spec['control_ports'][mode],spec['te_ports'][mode]):
            with socket.socket() as probe: probe.bind((urlparse(node['url']).hostname,port))
    children=[]
    try:
        for mode in missing:
            runner.stage='receiver-start-'+mode
            command=dram_command(spec,args,topology,receipt,private,out/mode,mode,'receiver',env)
            with (out/f'{mode}.log').open('x') as log:
                child=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
            children.append(child)
            runner.children[str(child.pid)]=child
            (out/'owned-pids.json').write_text(json.dumps([p.pid for p in children]))
            from callosum_lab import process_stamp
            (out/f'{mode}.process.json').write_text(json.dumps(dict(pid=child.pid,stamp=process_stamp(child.pid),
                state_dir=str(out/mode),config=str(topology),mode=mode)))
            deadline=time.monotonic()+60
            while True:
                require(child.poll() is None,f'{mode} receiver exited; inspect {out}/{mode}.log')
                try:
                    live=receiver_health(spec,c,args,mode)
                    break
                except (OSError,RuntimeError):
                    require(time.monotonic()<deadline,f'{mode} readiness timed out; retain child and inspect its log')
                    time.sleep(.2)
            marker=out/f'{mode}.process.json'
            record=json.loads(marker.read_text()); record['epoch']=live['epoch']
            marker.write_text(json.dumps(record))
        runner.say(f'[READY] receivers={args.page_modes}; evidence={out}; Ctrl-C requests safe owned shutdown')
        while all(p.poll() is None for p in children): time.sleep(.5)
        raise RuntimeError('a receiver exited; inspect the owned receiver logs')
    except KeyboardInterrupt:
        from callosum_benchmark_prepare import stop_receivers
        try: runner.say(json.dumps(stop_receivers(out,spec,c,args)))
        except Exception as error:
            runner.say(f'[RETAINED] {error}; inspect owned process markers; no SIGKILL or reclaim')
    except BaseException:
        runner.say('[RETAINED] Inspect owned-pids.json. No child was killed or MR reclaimed.')
        raise


def run(spec,c,args,topology,receipt,private,env,out,runner):
    runner.stage='matrix-read-only-preflight'
    # Verify every DRAM target before source initialization or CXL writes.
    if 'local_numa' in args.backings:
        health={m:receiver_health(spec,c,args,m) for m in args.page_modes}
        (out/'receiver-health.json').write_text(json.dumps(health,indent=2))
        node=next(n for n in c['nodes'] if n['id']==args.node)
        numa=spec['numa_nodes'].get(args.node)
        if numa is None: numa=int((Path('/sys/class/infiniband')/node['rdma_device']/'device/numa_node').read_text())
        require(numa>=0,'NIC NUMA unknown; configure a reviewed override')
        size=max(block_bytes(n,args.unit) for n in args.sizes)
        capacity=max(allocation_geometry(size,m) for m in args.page_modes)
        soft,_=resource.getrlimit(resource.RLIMIT_MEMLOCK)
        require(soft==resource.RLIM_INFINITY or soft>=capacity,'sender memlock too small for selected window')
        if 'huge2MB' in args.page_modes: hugepage_capacity(numa,size)
    if 'cxl' in args.backings:
        relay=Path(__file__).with_name('run_callosum_relay.sh')
        status=json.loads(runner.call('cxl-status-preflight',
            ['bash',relay,'status','--config',topology],env,capture=True))
        require(status.get('fingerprint')==c['fingerprint'] and status.get('topology_verified'),
                'CXL campaign not ready or topology differs')
        if 'huge2MB' in args.page_modes:
            for member in spec['pair'].values():
                pages=status.get('mr_pages',{}).get(member) or {}
                require(pages.get('device_alignment_bytes')==2**21 and
                        pages.get('kernel_page_size_bytes')==2**21 and pages.get('virtual_alignment_2MiB'),
                        'CXL-2MiB case needs live DAX page evidence; no page-size fabrication or restart')
    # Small correctness gate is automatic. The standalone mini is not a prerequisite.
    smoke=[]
    if 'local_numa' in args.backings:
        from types import SimpleNamespace
        small=SimpleNamespace(**dict(vars(args),sizes=[1],unit='MB',repetitions=1))
        for mode in args.page_modes:
            destination=out/('smoke-local_numa-'+mode)
            command=dram_command(spec,small,topology,receipt,private,destination,mode,'sender',env)
            command+=['--smoke-bytes','1000000']
            runner.remote_mutation_possible=True
            runner.call('smoke-local_numa-'+mode,command,env)
            proof=json.loads((destination/'summary.json').read_text())
            require(proof['status']=='PASS','DRAM smoke failed; no performance cases start')
            smoke.append(dict(case='local_numa-'+mode,status='PASS',evidence=str(destination)))
    if 'cxl' in args.backings:
        destination=out/'smoke-cxl'
        benchmark=Path(__file__).resolve().parents[2]/'experiments/two_worker_metadata_lookup/run.sh'
        runner.remote_mutation_possible=True
        runner.call('smoke-cxl',['bash',benchmark,'--config',topology,'--source',spec['pair']['source'],
            '--requester',spec['pair']['receiver'],'--sizes-bytes','1000000','--iterations','1','--warmup','0',
            '--chunk-bytes','65536','--output',destination],env)
        require(json.loads((destination/'summary.json').read_text())['status']=='PASS','CXL smoke failed')
        smoke.append(dict(case='cxl',status='PASS',evidence=str(destination)))
    (out/'smoke-results.json').write_text(json.dumps(smoke,indent=2))
    rows=[]
    for backing in [b for b in ('local_numa','cxl') if b in args.backings]:
        for mode in args.page_modes:
            case=f'{backing}-{mode}'; destination=out/case
            if backing=='cxl':
                mini=dict(schema_version=1,topology_file=str(topology),runtime=dict(setup_run=str(receipt)),
                    token_file=str(private),output=str(destination),pairs=[spec['pair']],schedule='serial_pairs',
                    sizes=args.sizes,unit=args.unit,repetitions=args.repetitions,
                    mr_page_mode=c.get('mr_page_mode','native'),require_dax_2m=mode=='huge2MB')
                path=out/f'{case}-config.json'; path.write_text(json.dumps(mini,indent=2))
                command=[env['PYTHON_BIN'],str(Path(__file__).with_name('callosum_write_mini.py')),
                         '--workers','2','--config',str(path)]
            else:
                command=dram_command(spec,args,topology,receipt,private,destination,mode,'sender',env)
            runner.remote_mutation_possible=True
            runner.call(case,command,env)
            summary=json.loads((destination/'summary.json').read_text())
            require(summary['status']=='PASS','case did not complete')
            expected={(block_bytes(n,args.unit),rep) for n in args.sizes for rep in range(args.repetitions)}
            observed=[(r['block_bytes'],r.get('repetition',0)) for r in summary['results']]
            require(len(observed)==len(expected) and set(observed)==expected and
                    all(r['bytes']==TOTAL_BYTES and r.get('measurement_kind','PERFORMANCE')=='PERFORMANCE'
                        for r in summary['results']),'incomplete or smoke-only case cannot enter performance report')
            for row in summary['results']:
                row.update(experiment_case=case,requested_page_mode=mode)
                rows.append(row)
    report=write_report(out,rows)
    with (out/'report.md').open('a') as stream:
        stream.write('\nMatrix note: CXL huge2MB requires observed 2 MiB DAX geometry. '
                     'CXL native may already have identical pages. This is not a forced 4 KiB/2 MiB A/B. '
                     'NUMA huge2MB uses explicit HugeTLB; NUMA native records actual residency. '
                     'Compare matching sender-loop scopes, not unlike trial control paths.\n')
    (out/'summary.json').write_text(json.dumps(dict(status='PASS',evidence_class='TRANSFER_ONLY',
        results=rows,report=report,cxl_page_modes_are_not_forced_4k_vs_2m=True,
        gpu_delivery_proven=False,serving_proven=False),indent=2))
    runner.say((out/'report.md').read_text())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['init','prepare','provision-hugepages','retire','start-cxl','stop-receivers','receive','run'])
    parser.add_argument('--config',required=True)
    parser.add_argument('--node',required=True)
    parser.add_argument('--work-dir',help='Preparation/evidence directory; defaults to runs/benchmark-prep/<node>')
    parser.add_argument('--peer-report',help='Signed readiness.json from the other physical host')
    parser.add_argument('--topology',help='init: existing approved topology, never generated device ownership')
    parser.add_argument('--setup-run',help='init: existing local successful build receipt')
    parser.add_argument('--peer-setup-run',help='init: peer build receipt path as used on that host')
    parser.add_argument('--token-file',help='init: existing shared private token file')
    parser.add_argument('--source',help='init: explicit source ID if absent from lab_run')
    parser.add_argument('--receiver',help='init: explicit receiver ID if absent from lab_run')
    parser.add_argument('--campaign-run',help='retire: old campaign; start-cxl: fresh campaign directory')
    parser.add_argument('--ack-exclusive-campaign',action='store_true',
                        help='retire: operator stopped canonical benchmark initiators; no unrelated/direct writers')
    parser.add_argument('--sizes',nargs='+',type=int)
    parser.add_argument('--unit',choices=['MB','MiB'],default=os.environ.get('IO_UNIT','MB'))
    parser.add_argument('--repetitions',type=int,default=3)
    parser.add_argument('--backings',nargs='+',choices=['cxl','local_numa'],default=['local_numa','cxl'])
    parser.add_argument('--page-modes',nargs='+',choices=['native','huge2MB'],default=['native','huge2MB'])
    args=parser.parse_args(); runner=None; phase='configuration'; work=None
    transition_possible=False; host_resources_changed=False
    try:
        if args.sizes is None:
            args.sizes=[int(n) for n in os.environ.get('IO_SIZES','1 2 4 8 16 32 64 128').split()]
        require(len(set(args.backings))==len(args.backings) and len(set(args.page_modes))==len(args.page_modes),
                'duplicate backing or page mode')
        import callosum_benchmark_prepare as preparation
        work=Path(args.work_dir or (Path(__file__).resolve().parents[2]/'runs/benchmark-prep'/args.node)).resolve()
        if args.action=='init':
            print(json.dumps(preparation.initialize(args.config,args),indent=2)); return 0
        if args.action in ('prepare','provision-hugepages'):
            phase=args.action
            report=preparation.inspect(args.config,args,work,load)
            if args.action=='provision-hugepages':
                require(sys.platform=='linux' and os.geteuid()==0,'provision-hugepages needs authorized Linux root')
                require(report.get('local_addresses') and report.get('numa') and report.get('hugepages') is not None,
                        'NUMA/hugepage inventory unavailable; no guessed provisioning')
                needed=0 if report.get('reused_huge_receiver') else report['hugepages_required_for_new_buffer']
                host_resources_changed=True
                print(json.dumps(preparation.provision(report['numa']['node'],needed,work),indent=2))
                print('Provisioning is not benchmark readiness. Repeat prepare on both hosts.')
                return 0
            print(json.dumps(report,indent=2))
            return 0 if report['status']=='PREPARED_NOT_TESTED' else 1
        spec,c,topology,receipt,private,env,root=load(args.config,args)
        phase=args.action
        if args.action=='stop-receivers':
            require(args.campaign_run and args.node==spec['pair']['receiver'],
                    'stop-receivers requires the owned receiver --campaign-run and receiver node')
            os.environ['CALLOSUM_CONTROL_TOKEN']=env['CALLOSUM_CONTROL_TOKEN']
            transition_possible=True
            print(json.dumps(preparation.stop_receivers(args.campaign_run,spec,c,args),indent=2)); return 0
        if args.action=='retire':
            require(args.campaign_run,'retire requires --campaign-run for the owned retained services')
            transition_possible=True
            print(json.dumps(preparation.retire(args.campaign_run,args.node,env['CALLOSUM_CONTROL_TOKEN'],
                  work,args.peer_report,args.ack_exclusive_campaign),indent=2)); return 0
        if args.action=='start-cxl':
            require(args.campaign_run and args.peer_report,'start-cxl requires a fresh --campaign-run and peer retired.json')
            local=preparation.read_proof(work/'retired.json',env['CALLOSUM_CONTROL_TOKEN'])
            peer=preparation.read_proof(args.peer_report,env['CALLOSUM_CONTROL_TOKEN'])
            require({local.get('node'),peer.get('node')}==set(spec['pair'].values()) and
                    all(r.get('status')=='RETIRED_NOT_RECLAIMED' for r in (local,peer)) and
                    local['fingerprint']==peer['fingerprint'] and local['frozen']==peer['frozen'],
                    'both old participants must be retired before fresh startup')
            from callosum_lab import process_stamp
            require(process_stamp(local['agent_exit']['pid'])!=local['agent_exit']['stamp'],
                    'old local participant is still running')
            require(os.environ.get('CALLOSUM_DESTRUCTIVE_ACK')=='1',
                    'start-cxl needs explicit exclusive-range authorization; prepare/init do not authorize DAX writes')
            from callosum_lab import main as lab_main
            source=Path(__file__).resolve().parents[1]; campaign=Path(args.campaign_run).resolve()
            claim=Path(local['campaign'])/'successor.json'
            intended=dict(campaign=str(campaign),fingerprint=c['fingerprint'])
            if claim.exists():
                require(json.loads(claim.read_text())==intended,
                        'old retirement proof already assigned to a different successor; do not delete the claim')
            else:
                with claim.open('x') as stream: json.dump(intended,stream)
            if not (campaign/'setup.json').exists():
                preparation.adopt_build(source,receipt,campaign,env)
            input_hash=hashlib.sha256(Path(args.config).read_bytes()).hexdigest()
            # Canonical launcher constructs child environments from this process.
            # Apply the same reviewed transport knobs used by the DRAM cases.
            for key in tuple(os.environ):
                if key.startswith('MC_'): os.environ.pop(key)
            os.environ.update({k:v for k,v in env.items() if k.startswith('MC_')})
            transition_possible=True
            code=lab_main(['--action','node','--root',str(source.parent),'--run-dir',str(campaign),
                '--node-id',args.node,'--config',str(topology),'--token-file',str(private),
                '--python',env['PYTHON_BIN'],'--skip-apt'])
            if code==0:
                require(hashlib.sha256(Path(args.config).read_bytes()).hexdigest()==input_hash,
                        'matrix config changed during startup; services retained; no configuration overwritten')
                spec['runtimes'][args.node]={'setup_run':str(campaign)}
                preparation.archive(Path(args.config).resolve().parent,Path(args.config).name,spec)
            return code
        require(args.peer_report,'run prepare on both hosts, exchange readiness.json, then supply --peer-report; no service started')
        peer=preparation.verify_peer(args.peer_report,env['CALLOSUM_CONTROL_TOKEN'],c,spec,args)
        local=preparation.inspect(args.config,args,work,load)
        require(local['status']=='PREPARED_NOT_TESTED',f'local preparation BLOCKED; inspect {work}/readiness.json')
        require(sys.platform=='linux','matrix execution requires the Linux lab; portable tests are separate')
        # Raise only this process's soft limit within the existing hard limit.
        # Children inherit it. Preparation already checked the hard limit.
        soft,hard=resource.getrlimit(resource.RLIMIT_MEMLOCK)
        needed=local['memlock']['required']
        if soft!=resource.RLIM_INFINITY and soft<needed:
            resource.setrlimit(resource.RLIMIT_MEMLOCK,(needed,hard))
        os.environ['CALLOSUM_CONTROL_TOKEN']=env['CALLOSUM_CONTROL_TOKEN']
        root.mkdir(parents=True,exist_ok=True)
        out=Path(tempfile.mkdtemp(prefix=f'matrix-{args.node}-',dir=root))
        runner=Runner(out); runner.remote_mutation_possible=False
        (out/'local-readiness.json').write_text(json.dumps(local,indent=2))
        (out/'peer-readiness.json').write_text(json.dumps(peer,indent=2))
        public=dict(spec); public['token_file']='[private file path omitted]'
        (out/'experiment.json').write_text(json.dumps(dict(action=args.action,node=args.node,
            sizes=args.sizes,unit=args.unit,repetitions=args.repetitions,backings=args.backings,
            page_modes=args.page_modes,config=public,
            config_sha256=hashlib.sha256(Path(args.config).read_bytes()).hexdigest(),
            topology_sha256=hashlib.sha256(topology.read_bytes()).hexdigest(),
            driver_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()),indent=2))
        (receive if args.action=='receive' else run)(spec,c,args,topology,receipt,private,env,out,runner)
        return 0
    except Exception as error:
        failure=dict(status='FAIL',stage=runner.stage if runner else phase,error=str(error),
                     evidence=str(runner.run_dir) if runner else (str(work) if work and phase!='configuration' else None),
                     owned_children_started=list(runner.children) if runner else [],
                     rdma_work_possible=bool(runner and runner.remote_mutation_possible),
                     owned_service_transition_possible=transition_possible,
                     hugepage_pool_change_possible=host_resources_changed,
                     action=('Retain owned processes, mappings and journals; inspect transition evidence. No automatic reclaim.'
                             if transition_possible or runner and (runner.children or runner.remote_mutation_possible) else
                             'No service or WRITE started. A partial NUMA HugeTLB change may remain; inspect hugepage-provision.json. No rollback/shrink.'
                             if host_resources_changed else
                             'No new service or WRITE started. Correct configuration/resources and rerun.'))
        if runner: (runner.run_dir/'failure.json').write_text(json.dumps(failure,indent=2))
        print(json.dumps(failure,indent=2),file=sys.stderr)
        return 1
    finally:
        if runner: runner.log.close()


if __name__=='__main__': sys.exit(main())
