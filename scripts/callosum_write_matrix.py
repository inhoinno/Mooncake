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
    require(required_fields<=set(spec) and set(spec)<=required_fields|{'transport_env','fresh_session','preparation_dir','workflow'},
            'unknown/missing matrix fields')
    require(spec['schema_version']==1,'matrix schema_version must be 1')
    topology=resolve(base,spec['topology_file']); c=config(topology)
    require(c['mode']=='rdma','matrix requires RDMA; no TCP/file fallback')
    pair=spec['pair']; nodes={n['id']:n for n in c['nodes']}
    require(set(pair)=={'source','receiver'} and all(n in nodes for n in pair.values()),'invalid pair')
    require(nodes[pair['source']]['pool']!=nodes[pair['receiver']]['pool'] and
            nodes[pair['source']]['can_send'] and nodes[pair['receiver']]['can_receive'],'ineligible cross-pool pair')
    require(args.node in pair.values(),'node not in the configured pair')
    if spec.get('workflow'):
        require(spec['workflow'].get('node')==args.node and type(spec['workflow'].get('bound')) is bool,
                'workflow must identify this host-local node and its binding state')
    if args.action in ('receive','run') and not (args.action=='run' and spec.get('workflow')):
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
    require(args.node in spec['runtimes'] and set(spec['runtimes'])<=set(pair.values()),
            'this node needs a host-local runtime receipt; exchange signed readiness for peer provenance')
    receipt=resolve(base,spec['runtimes'][args.node]['setup_run'])
    require(set(spec['runtimes'][args.node])=={'setup_run'},'runtime needs setup_run')
    from callosum_benchmark_prepare import runtime_receipt
    setup=runtime_receipt(receipt)
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
                     'Compare matching sender-loop scopes, not unlike trial control paths. '
                     'GPU-ready, helper-selection and recomputation metrics: NOT_APPLICABLE.\n')
    (out/'summary.json').write_text(json.dumps(dict(status='PASS',evidence_class='TRANSFER_ONLY',
        results=rows,report=report,cxl_page_modes_are_not_forced_4k_vs_2m=True,
        metric_applicability={name:'NOT_APPLICABLE' for name in
                              ('gpu_ready_latency','helper_selection','recomputation')},
        gpu_delivery_proven=False,serving_proven=False),indent=2))
    runner.say((out/'report.md').read_text())


def launch_fresh(path,args,runner):
    """Use only the new config's generated inputs; keep canonical safety gates."""
    from callosum_lab import main as lab_main
    spec,c,topology,receipt,private,env,_=load(path,args)
    require(spec.get('fresh_session'),'prepare requires a config produced by init')
    require(not (receipt/'topology.json').exists() and not (receipt/'control.token').exists(),
            'This NEW session already attempted startup. Preserve its evidence; fresh creates a different session, '
            'but never reclaims live or uncertain DMA resources.')
    require(os.environ.get('CALLOSUM_DESTRUCTIVE_ACK')=='1',
            'fresh startup requires CALLOSUM_DESTRUCTIVE_ACK=1 for the approved exclusive extent')
    for key in tuple(os.environ):
        if key.startswith('MC_'): os.environ.pop(key)
    os.environ.update({k:v for k,v in env.items() if k.startswith('MC_')})
    runner.stage='fresh-canonical-node-start'
    # The canonical launcher validates access, limits and port ownership before
    # mapping. No old-service kill, extent enlargement or DMA reclamation here.
    code=lab_main(['--action','node','--root',str(Path(__file__).resolve().parents[2]),
        '--run-dir',str(receipt),'--node-id',args.node,'--config',str(topology),
        '--token-file',str(private),'--python',env['PYTHON_BIN'],'--skip-apt'])
    if code==0:
        (receipt/'cxl-started.json').write_text(json.dumps(dict(node=args.node,
            session_id=spec['fresh_session']['id'],status='LOCAL_START_RETURNED')))
        from callosum_benchmark_prepare import archive
        archive(receipt,'fresh-state.json',dict(status='LOCAL_SERVICES_STARTED',node=args.node,
            session_id=spec['fresh_session']['id'],campaign=str(receipt)))
    runner.say(json.dumps(dict(status='PASS' if code==0 else 'BLOCKED',
        scope='TRANSFER_ONLY',stage='fresh-startup',campaign=str(receipt),config=str(Path(path).resolve()),
        next_action='prepare' if code==0 else 'inspect retained startup evidence; no automatic restart',
        performance_baseline='NOT_RUN'),indent=2))
    return code


def peer_inputs(args,remote_config,ssh_options):
    """Only prepare calls this function, with an operator-supplied SSH target."""
    from callosum_lab import remote_command
    require(args.peer,'prepare requires --peer user@IP; no address is inferred or contacted automatically')
    code='''import json,sys
from pathlib import Path
p=Path(sys.argv[1]); c=json.loads(p.read_text())
assert c.get('workflow'), 'Run init on this peer first'
private=Path(c['token_file'])
assert private.stat().st_mode & 0o077 == 0, 'Token is not private'
value=dict(session=c['fresh_session'],workflow=c['workflow'],
    topology=json.loads(Path(c['topology_file']).read_text()),
    machine_id=Path('/etc/machine-id').read_text().strip())
if c['workflow']['node']==c['fresh_session']['controller']:
    value['token']=private.read_text().strip()
print(json.dumps(value))'''
    # This response may contain the private token. Do not use Runner.call or log it.
    return json.loads(subprocess.check_output(remote_command(args.peer,
        ['python3','-c',code,str(remote_config)],options=ssh_options),text=True,timeout=120))


def bind_peer(path,args,ssh_options):
    """Bind two fresh LOCAL initializations; never replace a live credential."""
    import callosum_benchmark_prepare as preparation
    spec,c,topology,receipt,private,env,_=load(path,args)
    remote_root=Path(args.remote_root or str(Path(__file__).resolve().parents[2]))
    remote_config=Path(args.peer_config or remote_root/Path(path).name)
    require(remote_config.is_absolute(),'--peer-config/--remote-root must be absolute')
    bundle=peer_inputs(args,remote_config,ssh_options)
    peer_node=next(n for n in spec['pair'].values() if n!=args.node)
    local_machine=Path('/etc/machine-id').read_text().strip()
    require(bundle['workflow']['node']==peer_node and bundle['machine_id']!=local_machine,
            'peer is not the configured OTHER physical node')
    require(bundle['topology']==json.loads(topology.read_text()),'approved peer topology differs; no extent is changed')
    session=bundle['session']; current=spec['fresh_session']
    require(session['controller']==current['controller'] and session['software']==current['software'] and
            session['software']==preparation.software() and session['profile']==current['profile'],
            'peer source/profile/controller differs; synchronize inputs and rerun init where needed')
    require(0<=time.time_ns()-session['created_unix_ns']<24*3600*10**9,
            'peer init is older than 24 hours or its clock is ahead; inspect clocks before a new init')
    started=(receipt/'topology.json').exists() or (receipt/'control.token').exists()
    if args.node!=current['controller']:
        require(bundle['workflow'].get('bound') is True,'controller init is not available')
        secret=bundle.get('token')
        require(isinstance(secret,str) and len(secret)>=32,'invalid controller token')
        if spec['workflow'].get('bound'):
            require(session==current and secret==env['CALLOSUM_CONTROL_TOKEN'],
                    'controller session changed; do not rebind retained services; use init for new local inputs')
        else:
            require(not started,'local campaign already started; shared credential cannot be changed')
            # Keep the local probe token intact. Only an unstarted fresh session
            # may acquire this new shared-token file. Repeated calls compare it.
            shared=receipt/'shared.control.token'
            if shared.exists():
                require(shared.stat().st_mode & 0o077==0 and shared.read_text().strip()==secret,
                        'existing handoff token differs; no credential overwritten')
            else:
                with os.fdopen(os.open(shared,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as stream:
                    stream.write(secret+'\n')
            spec['token_file']=str(shared); spec['fresh_session']=session
    elif bundle['workflow'].get('bound'):
        require(session==current,'peer is already bound to a different session; initialize the peer for this experiment')
    spec['workflow'].update(peer_ssh=args.peer,peer_config=str(remote_config),bound=True)
    preparation.archive(Path(path).resolve().parent,Path(path).name,spec)
    preparation.archive(receipt,'fresh-state.json',dict(status='BOUND_TO_CONTROLLER',node=args.node,
        session_id=spec['fresh_session']['id'],campaign=str(receipt),local_campaign_started=started))
    return load(path,args)


def exchange_readiness(spec,c,args,work,secret,ssh_options=()):
    """Retrieve the intended peer report over SSH; never choose a latest run."""
    from callosum_lab import remote_command
    import callosum_benchmark_prepare as preparation
    require(sys.platform=='linux','readiness exchange requires the authorized Linux operator host')
    workflow=spec['workflow']; peer=workflow['peer_ssh']
    require(peer and workflow['node']==args.node,'prepare must record this node and authorized peer SSH target')
    code='''import json,sys
from pathlib import Path
p=Path(sys.argv[1]); c=json.loads(p.read_text())
r=Path(c['preparation_dir'])/'readiness.json'
print(json.dumps(dict(session=c['fresh_session'],workflow=c['workflow'],
    report=json.loads(r.read_text()) if r.exists() else None)))'''
    deadline=time.monotonic()+c['lab_run'].get('wait_seconds',1800)
    expected=next(n for n in spec['pair'].values() if n!=args.node)
    while True:
        raw=subprocess.check_output(remote_command(peer,['python3','-c',code,workflow['peer_config']],
            options=(*ssh_options,'-o','BatchMode=yes','-o','ConnectTimeout=5')),text=True,timeout=15)
        bundle=json.loads(raw)
        require(bundle['workflow']['node']==expected,'wrong peer node in readiness handoff')
        if not bundle['workflow'].get('bound'):
            require(time.monotonic()<deadline,'peer has not run prepare for this session')
            time.sleep(1); continue
        require(bundle['session']==spec['fresh_session'],
                'peer configuration is bound to another session; initialize the peer for this experiment')
        report=bundle['report']
        if report and report.get('status')=='PREPARED_NOT_TESTED':
            preparation.archive(work,'peer-readiness.json',report)
            return preparation.verify_peer(work/'peer-readiness.json',secret,c,spec,args)
        require(not report or report.get('status')!='BLOCKED',
                f'peer preparation is BLOCKED: {report.get("issues") if report else ""}')
        require(time.monotonic()<deadline,'peer readiness timed out; run prepare on both hosts; services are retained')
        time.sleep(1)


def prepare_session(path,args,work,runner,ssh_options=()):
    """One prepare stage: cold checks, canonical startup, smoke, peer proof."""
    import callosum_benchmark_prepare as preparation
    spec,c,topology,receipt,private,env,_=load(path,args)
    cold=preparation.inspect(path,args,work,load,live=False)
    require(cold['status']=='INPUTS_CHECKED_NOT_STARTED',
            f'local prerequisites BLOCKED: {cold["issues"]}; evidence={work}/readiness.json')
    soft,hard=resource.getrlimit(resource.RLIMIT_MEMLOCK)
    needed=cold['memlock']['required']
    if soft!=resource.RLIM_INFINITY and soft<needed:
        resource.setrlimit(resource.RLIMIT_MEMLOCK,(needed,hard))
    require(sys.platform=='linux','prepare requires the Linux operator host; no remote access from this Mac')
    require(args.peer,'prepare requires --peer user@IP; no peer IP is inferred')
    runner.stage='prepare-explicit-peer-handoff'
    spec,c,topology,receipt,private,env,_=bind_peer(path,args,ssh_options)
    marker=receipt/'cxl-started.json'
    if not marker.exists():
        runner.services_may_be_live=True
        runner.remote_mutation_possible=True  # Canonical startup includes RDMA correctness WRITEs.
        require(launch_fresh(path,args,runner)==0,
                'canonical startup failed; preserve campaign evidence; do not start a second owner')
    else:
        stamp=json.loads(marker.read_text())
        require(stamp['node']==args.node and stamp['session_id']==spec['fresh_session']['id'],
                'startup marker does not belong to this session/node')
        runner.services_may_be_live=True  # Existing owners remain lifetime-relevant on failure.
    os.environ['CALLOSUM_CONTROL_TOKEN']=env['CALLOSUM_CONTROL_TOKEN']
    deadline=time.monotonic()+c['lab_run'].get('wait_seconds',1800)
    runner.stage='prepare-bidirectional-correctness'
    while True:
        status=rpc(c['master'],'status',timeout=5)
        # The controller publishes summary.json only after both directions,
        # checksum/publication/local reuse and the trace gate pass.
        if args.node==spec['fresh_session']['controller']:
            summary=receipt/'summary.json'
            require(summary.exists() and json.loads(summary.read_text()).get('status')=='PASS',
                    'controller bidirectional smoke/trace did not pass; inspect campaign evidence')
        if status.get('topology_verified') and len(status.get('operations',[]))>=2 and all(
                o.get('state') in ('COMMITTED','DRAINED_RETAINED') for o in status['operations']):
            break
        require(time.monotonic()<deadline,'controller smoke is incomplete; retain services and inspect its evidence')
        time.sleep(1)
    report=preparation.inspect(path,args,work,load)
    require(report['status']=='PREPARED_NOT_TESTED',
            f'live preparation BLOCKED: {report["issues"]}; evidence={work}/readiness.json')
    runner.stage='prepare-peer-readiness'
    peer=exchange_readiness(spec,c,args,work,env['CALLOSUM_CONTROL_TOKEN'],ssh_options)
    result=dict(status='PREPARED_NOT_TESTED',stage='prepare',node=args.node,
                session_id=spec['fresh_session']['id'],peer_node=peer['node'],
                evidence=str(work),next_action='run',hardware_performance='NOT_RUN')
    preparation.archive(work,'prepare.json',result)
    preparation.archive(receipt,'fresh-state.json',result)
    runner.say(json.dumps(result,indent=2))


def prepare_connected(path,args,work,runner):
    """One operator SSH authorization, reused only inside this prepare call."""
    from callosum_lab import remote_command
    require(args.peer,'prepare requires --peer user@IP; init/run do not contact SSH')
    with tempfile.TemporaryDirectory(prefix='callosum-ssh-') as directory:
        control=str(Path(directory)/'control')
        options=('-o','ControlMaster=auto','-o','ControlPersist=yes','-o','ControlPath='+control)
        try:
            prepare_session(path,args,work,runner,options)
        finally:
            if Path(control).exists():
                # Close only this invocation's SSH control connection, not lab
                # services or another operator's shared connection.
                try:
                    subprocess.run(remote_command(args.peer,[],options=('-S',control,'-O','exit')),
                        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=5)
                except (OSError,subprocess.TimeoutExpired): pass


def apply_session_profile(args):
    """init fixes the plan; prepare/run inherit it without repeated shell vars."""
    profile={}
    if args.action!='init' and Path(args.config).is_file():
        spec=json.loads(Path(args.config).read_text())
        profile=spec.get('fresh_session',{}).get('profile',{})
    defaults=dict(sizes=[1,2,4,8,16,32,64,128],unit='MB',repetitions=3,
                  backings=['local_numa','cxl'],page_modes=['native','huge2MB'])
    # IO_* supplies initialization defaults, not a second mutable profile.
    # Build/CTest children inherit the operator's environment too. Once a session
    # exists, only explicit CLI options may be compared with its fixed profile.
    if not profile:
        if args.sizes is None and 'IO_SIZES' in os.environ:
            args.sizes=[int(n) for n in os.environ['IO_SIZES'].split()]
        if args.unit is None and 'IO_UNIT' in os.environ: args.unit=os.environ['IO_UNIT']
    for key,default in defaults.items():
        value=getattr(args,key)
        if value is not None and profile:
            require(value==profile[key],f'{key} differs from init; use init for a new independent experiment')
        setattr(args,key,value if value is not None else profile.get(key,default))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['init','prepare','run','provision-hugepages','retire','start-cxl','stop-receivers','receive'])
    parser.add_argument('--config',required=True)
    parser.add_argument('--node',required=True)
    parser.add_argument('--work-dir',help='Preparation/evidence directory; defaults to runs/benchmark-prep/<node>')
    parser.add_argument('--peer-report',help='Signed readiness.json from the other physical host')
    parser.add_argument('--topology',help='init: existing approved topology, never generated device ownership')
    parser.add_argument('--setup-run',help='init: existing local successful build receipt')
    parser.add_argument('--peer-setup-run',help='Legacy optional peer path; peer identity is checked through signed readiness')
    parser.add_argument('--token-file',help='init: existing shared private token file')
    parser.add_argument('--create-token',action='store_true',help='init: create a missing private token once on the controller; never overwrite')
    parser.add_argument('--source',help='init: explicit source ID if absent from lab_run')
    parser.add_argument('--receiver',help='init: explicit receiver ID if absent from lab_run')
    parser.add_argument('--peer',help='prepare ONLY: operator-supplied OTHER host user@IP; never inferred')
    parser.add_argument('--remote-root',help='prepare: peer checkout root; defaults to this checkout root')
    parser.add_argument('--peer-config',help='prepare: absolute peer matrix config; defaults to remote-root/config basename')
    parser.add_argument('--generate-only',action='store_true',help='init: input fixture only; no build or readiness claim')
    parser.add_argument('--jobs',type=int,default=4,help='init: build parallelism')
    parser.add_argument('--skip-apt',action='store_true',help='init: refuse missing OS packages instead of installing them')
    parser.add_argument('--campaign-run',help='retire: old campaign; start-cxl: fresh campaign directory')
    parser.add_argument('--ack-exclusive-campaign',action='store_true',
                        help='retire: operator stopped canonical benchmark initiators; no unrelated/direct writers')
    parser.add_argument('--sizes',nargs='+',type=int)
    parser.add_argument('--unit',choices=['MB','MiB'])
    parser.add_argument('--repetitions',type=int)
    parser.add_argument('--backings',nargs='+',choices=['cxl','local_numa'])
    parser.add_argument('--page-modes',nargs='+',choices=['native','huge2MB'])
    args=parser.parse_args(); runner=None; phase='configuration'; work=None
    transition_possible=False; host_resources_changed=False
    try:
        apply_session_profile(args)
        require(not args.create_token or args.action=='init','--create-token is only valid with init')
        require(len(set(args.backings))==len(args.backings) and len(set(args.page_modes))==len(args.page_modes),
                'duplicate backing or page mode')
        import callosum_benchmark_prepare as preparation
        require(not (args.peer or args.remote_root or args.peer_config) or args.action=='prepare',
                'SSH options belong only to prepare; init and run are host-local')
        require(args.jobs>0,'--jobs must be positive')
        require(not args.generate_only or (args.action=='init' and not args.setup_run),
                '--generate-only requires init without a historical --setup-run')
        if args.action=='init' and not args.setup_run:
            root=Path(args.config).resolve().parent/'runs'
            root.mkdir(parents=True,exist_ok=True)
            # No old config/receipt/env is read to choose this directory.
            runner=Runner(Path(tempfile.mkdtemp(prefix='write-session-',dir=root)))
            runner.remote_mutation_possible=False
            preparation.fresh_configuration(args.config,args,runner)
            if args.generate_only: return 0
            spec=json.loads(Path(args.config).read_text()); work=Path(spec['preparation_dir'])
            runner.stage='init-prerequisites'
            report=preparation.inspect(args.config,args,work,load,live=False)
            runner.say(json.dumps(report,indent=2))
            return 0 if report['status']=='INPUTS_CHECKED_NOT_STARTED' else 1
        configured_work=None
        if not args.work_dir and Path(args.config).is_file():
            try: configured_work=json.loads(Path(args.config).read_text()).get('preparation_dir')
            except (ValueError,OSError): pass  # The input gate reports invalid JSON.
        work=Path(args.work_dir or configured_work or (Path(__file__).resolve().parents[2]/'runs/benchmark-prep'/args.node)).resolve()
        if args.action=='init':
            print(json.dumps(preparation.initialize(args.config,args),indent=2)); return 0
        if args.action in ('prepare','provision-hugepages'):
            phase=args.action
            if args.action=='prepare' and Path(args.config).is_file() and json.loads(
                    Path(args.config).read_text()).get('workflow'):
                runner=Runner(work); runner.remote_mutation_possible=False
                prepare_connected(args.config,args,work,runner)
                return 0
            if args.action=='provision-hugepages':
                require(work.is_dir(),
                        'Run prepare as the benchmark account first. Privileged provisioning must not create its preparation directory.')
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
        if spec.get('workflow') and args.action=='run':
            require((work/'prepare.json').is_file(),'run prepare on both hosts before run')
            prepared=json.loads((work/'prepare.json').read_text())
            require(spec['workflow'].get('bound') and prepared.get('status')=='PREPARED_NOT_TESTED' and
                    prepared.get('node')==args.node and prepared.get('session_id')==spec['fresh_session']['id'],
                    'prepare receipt belongs to a different or failed session/node; rerun prepare')
            args.peer_report=str(work/'peer-readiness.json')
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
        is_receiver=args.action=='receive' or (spec.get('workflow') and args.node==spec['pair']['receiver'])
        if spec.get('workflow') and not is_receiver and 'local_numa' in args.backings:
            runner.stage='run-wait-receiver'
            deadline=time.monotonic()+c['lab_run'].get('wait_seconds',1800)
            while True:
                try:
                    for mode in args.page_modes: receiver_health(spec,c,args,mode)
                    break
                except (OSError,RuntimeError):
                    require(time.monotonic()<deadline,'receiver not ready; run the same run command on the configured receiver')
                    time.sleep(1)
        if is_receiver and 'local_numa' not in args.backings:
            runner.say('[READY] CXL-only targets already started by prepare; run on the source performs the sweep')
        else:
            (receive if is_receiver else run)(spec,c,args,topology,receipt,private,env,out,runner)
        return 0
    except Exception as error:
        transition_possible=transition_possible or bool(runner and runner.services_may_be_live)
        if phase=='prepare' and work:
            # Do not leave an older PASS visible after a failed refresh.
            try:
                spec=json.loads(Path(args.config).read_text())
                private=resolve(Path(args.config).resolve().parent,spec['token_file'])
                latest=work/'readiness.json'
                current=json.loads(latest.read_text()) if latest.is_file() else dict(node=args.node,issues=[])
                if current.get('status')!='BLOCKED':
                    current.update(status='BLOCKED',created_unix_ns=time.time_ns())
                    current['issues'].append(dict(check='prepare',error=str(error)))
                    preparation.save_readiness(work,current,private.read_text().strip())
                if spec.get('workflow'):
                    preparation.archive(work,'prepare.json',dict(status='BLOCKED',node=args.node,
                        session_id=spec['fresh_session']['id'],error=str(error)))
            except (OSError,ValueError,KeyError,NameError): pass
        failure=dict(status='FAIL',stage=runner.stage if runner else phase,error=str(error),
                     issues=getattr(error,'issues',[]),
                     initialization_side_effects=getattr(error,'initialization_side_effects',None),
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
