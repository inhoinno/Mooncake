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
from callosum_transport_benchmark import block_bytes
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
    require(health['node_id']==spec['pair']['receiver'] and health['peer_id']==spec['pair']['source'] and
            health['fingerprint']==c['fingerprint'] and health['state']=='IDLE' and
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
    numa=spec['numa_nodes'].get(args.node)
    if numa is None: numa=int((Path('/sys/class/infiniband')/node['rdma_device']/'device/numa_node').read_text())
    require(numa>=0,'NIC NUMA is unknown; configure a reviewed numa_nodes override')
    size=max(block_bytes(n,args.unit) for n in args.sizes)
    # Each mode uses a separate child process; RLIMIT_MEMLOCK is per process.
    required=max(allocation_geometry(size,m) for m in args.page_modes)
    soft,_=resource.getrlimit(resource.RLIMIT_MEMLOCK)
    require(soft==resource.RLIM_INFINITY or soft>=required,
            f'each receiver process needs memlock >= {required} bytes')
    if 'huge2MB' in args.page_modes: hugepage_capacity(numa,size)
    for mode in args.page_modes:
        for port in (spec['control_ports'][mode],spec['te_ports'][mode]):
            with socket.socket() as probe: probe.bind((urlparse(node['url']).hostname,port))
    children=[]
    try:
        for mode in args.page_modes:
            runner.stage='receiver-start-'+mode
            command=dram_command(spec,args,topology,receipt,private,out/mode,mode,'receiver',env)
            with (out/f'{mode}.log').open('x') as log:
                child=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
            children.append(child)
            runner.children[str(child.pid)]=child
            (out/'owned-pids.json').write_text(json.dumps([p.pid for p in children]))
            deadline=time.monotonic()+60
            while True:
                require(child.poll() is None,f'{mode} receiver exited; inspect {out}/{mode}.log')
                try:
                    receiver_health(spec,c,args,mode)
                    break
                except (OSError,RuntimeError):
                    require(time.monotonic()<deadline,f'{mode} readiness timed out; retain child and inspect its log')
                    time.sleep(.2)
        runner.say(f'[READY] receivers={args.page_modes}; evidence={out}; Ctrl-C requests safe owned shutdown')
        while all(p.poll() is None for p in children): time.sleep(.5)
        raise RuntimeError('a receiver exited; inspect the owned receiver logs')
    except KeyboardInterrupt:
        for child in children:
            if child.poll() is None: child.send_signal(signal.SIGTERM)
        for child in children:
            try: child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                runner.say(f'[RETAINED] pid={child.pid}; active/quarantined target; no SIGKILL')
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
    rows=[]
    for backing in args.backings:
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
    parser.add_argument('action',choices=['receive','run'])
    parser.add_argument('--config',required=True)
    parser.add_argument('--node',required=True)
    parser.add_argument('--sizes',nargs='+',type=int)
    parser.add_argument('--unit',choices=['MB','MiB'],default=os.environ.get('IO_UNIT','MiB'))
    parser.add_argument('--repetitions',type=int,default=3)
    parser.add_argument('--backings',nargs='+',choices=['cxl','local_numa'],default=['cxl','local_numa'])
    parser.add_argument('--page-modes',nargs='+',choices=['native','huge2MB'],default=['native','huge2MB'])
    args=parser.parse_args(); runner=None
    try:
        if args.sizes is None:
            args.sizes=[int(n) for n in os.environ.get('IO_SIZES','1 2 4 8 16 32 64 128').split()]
        require(len(set(args.backings))==len(args.backings) and len(set(args.page_modes))==len(args.page_modes),
                'duplicate backing or page mode')
        spec,c,topology,receipt,private,env,root=load(args.config,args)
        require(sys.platform=='linux','matrix execution requires the Linux lab; portable tests are separate')
        os.environ['CALLOSUM_CONTROL_TOKEN']=env['CALLOSUM_CONTROL_TOKEN']
        root.mkdir(parents=True,exist_ok=True)
        out=Path(tempfile.mkdtemp(prefix=f'matrix-{args.node}-',dir=root))
        runner=Runner(out); runner.remote_mutation_possible=False
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
        failure=dict(status='FAIL',stage=runner.stage if runner else 'configuration',error=str(error),
                     evidence=str(runner.run_dir) if runner else None,
                     owned_children_started=list(runner.children) if runner else [],
                     rdma_work_possible=bool(runner and runner.remote_mutation_possible),
                     action=('Retain owned processes and existing mappings/journals. No automatic reclaim.'
                             if runner and (runner.children or runner.remote_mutation_possible) else
                             'No new service or WRITE started. Correct configuration/resources and rerun.'))
        if runner: (runner.run_dir/'failure.json').write_text(json.dumps(failure,indent=2))
        print(json.dumps(failure,indent=2),file=sys.stderr)
        return 1
    finally:
        if runner: runner.log.close()


if __name__=='__main__': sys.exit(main())
