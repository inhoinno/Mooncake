"""Preparation for the existing WRITE matrix. Never treats timeout as DMA drain."""
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import resource
import secrets
import select
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import time
from types import SimpleNamespace
from urllib.parse import urlparse

from callosum_runtime import config, rpc, require, RUNTIME_SHA256, IMPLEMENTATION_SHA256
from callosum_dram_benchmark import allocation_geometry, cpulist, source_identity
from callosum_transport_benchmark import block_bytes, TOTAL_BYTES
from callosum_lab import SOURCE_FILES, digest, process_stamp


def signed(value,secret):
    value=dict(value); value.pop('signature',None)
    value['signature']=hmac.new(secret.encode(),json.dumps(value,sort_keys=True).encode(),hashlib.sha256).hexdigest()
    return value


def read_proof(path,secret):
    value=json.loads(Path(path).read_text())
    require(hmac.compare_digest(str(value.get('signature','')),signed(value,secret)['signature']),
            'peer evidence signature differs; use the same private token and unmodified report')
    return value


def archive(work,name,value):
    work=Path(work); work.mkdir(parents=True,exist_ok=True)
    old=work/name
    if old.exists():
        (work/f'{name}.{time.time_ns()}.previous').write_bytes(old.read_bytes())
    temporary=work/(name+'.'+secrets.token_hex(4)+'.tmp')
    with temporary.open('x') as stream: json.dump(value,stream,indent=2)
    temporary.replace(old)


def plan(c,spec,args):
    return hashlib.sha256(json.dumps(dict(fingerprint=c['fingerprint'],pair=spec['pair'],
        sizes=args.sizes,unit=args.unit,repetitions=args.repetitions,backings=sorted(args.backings),
        page_modes=sorted(args.page_modes),control_ports=spec['control_ports'],te_ports=spec['te_ports'],
        transport_env=spec.get('transport_env',{})),sort_keys=True).encode()).hexdigest()


def software():
    files=['callosum_runtime.py','callosum_write_matrix.py','callosum_write_mini.py',
           'callosum_benchmark_prepare.py','callosum_benchmark_report.py',
           'callosum_dram_benchmark.py','callosum_transport_benchmark.py']
    result={f:digest(Path(__file__).with_name(f)) for f in files}
    result.update({f:digest(Path(__file__).resolve().parents[1]/f) for f in SOURCE_FILES if not f.startswith('scripts/')})
    return result


def huge_snapshot(node,sysroot=Path('/sys')):
    local=sysroot/f'devices/system/node/node{node}/hugepages/hugepages-2048kB'
    global_root=sysroot/'kernel/mm/hugepages/hugepages-2048kB'
    total=int((local/'nr_hugepages').read_text()); free=int((local/'free_hugepages').read_text())
    reserved=int((global_root/'resv_hugepages').read_text()); global_free=int((global_root/'free_hugepages').read_text())
    return dict(node=node,total_pages=total,free_pages=free,global_reserved_pages=reserved,
                global_free_pages=global_free,available_pages=min(free,max(0,global_free-reserved)),
                control_file=str(local/'nr_hugepages'))


def provision(node,needed,work,sysroot=Path('/sys')):
    """Grow only this NUMA node by the missing capacity. Never shrink on errors."""
    require(type(node) is int and node>=0 and type(needed) is int and needed>=0,'invalid HugeTLB requirement')
    before=huge_snapshot(node,sysroot)
    target=before['total_pages']+max(0,needed-before['available_pages'])
    record=dict(status='PLANNED',before=before,required_free_pages=needed,target_total_pages=target,
                changed=False,reclaimed=False)
    archive(work,'hugepage-provision.json',record)
    if target!=before['total_pages']:
        with Path(before['control_file']).open('w') as stream: stream.write(str(target))
        record['changed']=True
    after=huge_snapshot(node,sysroot); record['after']=after
    record['status']='PASS' if after['available_pages']>=needed and after['total_pages']>=target else 'PARTIAL'
    archive(work,'hugepage-provision.json',record)
    require(record['status']=='PASS',
            f'partial NUMA HugeTLB allocation: available={after["available_pages"]}, needed={needed}; '
            'no service started; provision record retained; no rollback/shrink performed')
    return record


def inspect(path,args,work,loader):
    """Collect independent failures without starting a service or mapping a device."""
    result=dict(status='BLOCKED',scope='READ_ONLY_PREPARATION',node=args.node,issues=[],
                services_started=False,devices_mapped=False,created_unix_ns=time.time_ns(),software=software())
    def check(name,operation):
        try:
            value=operation(); result[name]=value
            return value
        except Exception as error:
            detail=str(error)
            if isinstance(error,subprocess.CalledProcessError) and error.stderr:
                detail+='; '+(error.stderr.decode(errors='replace') if isinstance(error.stderr,bytes) else error.stderr)[-2000:]
            result['issues'].append(dict(check=name,error=detail))
    path=Path(path).resolve()
    spec=check('matrix_config',lambda:json.loads(path.read_text()))
    secret=None
    if spec:
        def required_file(field):
            value=(path.parent/spec[field]).resolve()
            require(value.is_file(),f'missing {field}: {value}')
            return value
        topology=check('topology_path',lambda:str(required_file('topology_file')))
        private=check('token_path',lambda:str(required_file('token_file')))
        if private:
            def read_secret():
                value=Path(private).read_text().strip()
                require(len(value)>=32 and Path(private).stat().st_mode & 0o077==0,'token needs >=32 characters and 0600')
                return value
            try: secret=read_secret()
            except Exception as error: result['issues'].append(dict(check='token',error=str(error)))
        check('setup_path',lambda:str((path.parent/spec['runtimes'][args.node]['setup_run']/'setup.json').resolve(strict=True)))
    loaded=check('configuration',lambda:loader(path,SimpleNamespace(**dict(vars(args),action='prepare'))))
    if loaded:
        spec,c,topology,receipt,private,env,root=loaded
        result['plan']=plan(c,spec,args); result['fingerprint']=c['fingerprint']
        result['pair']=spec['pair']; result['role']='receiver' if args.node==spec['pair']['receiver'] else 'initiator'
        result['effective']=dict(sizes=args.sizes,unit=args.unit,repetitions=args.repetitions,
            size_bytes=[block_bytes(n,args.unit) for n in args.sizes],bytes_per_trial=TOTAL_BYTES,
            configured_qps=16,warmup_blocks=0,concurrent_pairs=1)
        node=next(n for n in c['nodes'] if n['id']==args.node)
        result['endpoint']={k:node[k] for k in ('id','pool','url','path','te_endpoint','rdma_device')}
        # Do not expose unknown fields or token values from configuration/receipts.
        result.pop('matrix_config',None)
        if sys.platform!='linux':
            result['issues'].append(dict(check='platform',error='Linux lab host required; no remote probes issued from this platform'))
            result.pop('configuration',None)
            if secret: result=signed(result,secret)
            archive(work,'readiness.json',result)
            return result
        check('platform',lambda:require(sys.platform=='linux','host preparation requires Linux') or sys.platform)
        check('machine',lambda:dict(machine_id=Path('/etc/machine-id').read_text().strip(),
                                    boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip()))
        source=Path(__file__).resolve().parents[1]
        check('git',lambda:dict(head=subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip(),
            branch=subprocess.check_output(['git','-C',str(source),'branch','--show-current'],text=True).strip(),
            tracked_changes=subprocess.check_output(['git','-C',str(source),'status','--short','--untracked-files=no'],text=True)))
        def initiators():
            conflicts=[]
            for entry in Path('/proc').iterdir():
                if not entry.name.isdigit() or int(entry.name)==os.getpid(): continue
                try:
                    argv=(entry/'cmdline').read_bytes().decode(errors='replace').split('\0')
                    names=[Path(a).name for a in argv]
                    active=('callosum_write_matrix.py' in names and 'run' in argv or
                            'callosum_write_mini.py' in names or
                            'callosum_dram_benchmark.py' in names and 'sender' in argv)
                    if active: conflicts.append(int(entry.name))
                except (FileNotFoundError,PermissionError,ProcessLookupError): continue
            require(not conflicts,f'other benchmark initiators still running: PIDs {conflicts}; stop admission through their owners, not by clearing ports')
            return dict(known_initiators=[],scope='readable /proc entries; not proof that external writers are absent')
        check('conflicting_initiators',initiators)
        def identity():
            addresses=json.loads(subprocess.check_output(['ip','-j','addr'],text=True))
            local={a['local'] for n in addresses for a in n.get('addr_info',[])}
            selected=[urlparse(node['url']).hostname,node['te_endpoint'].rsplit(':',1)[0]]
            require(all(socket.gethostbyname(host) in local for host in selected),
                    f'NODE_ID={args.node} control/TE address is not assigned to this host; do not swap roles by host name')
            return sorted(local)
        check('local_addresses',identity)
        def numa_info():
            base=Path('/sys/class/infiniband')/node['rdma_device']
            selected=spec['numa_nodes'].get(args.node)
            if selected is None: selected=int((base/'device/numa_node').read_text())
            require(selected>=0,'NIC NUMA unknown; configure a reviewed override')
            cpus=cpulist((Path('/sys/devices/system/node')/f'node{selected}/cpulist').read_text())
            require(cpus & os.sched_getaffinity(0),'NIC-local CPUs excluded by cpuset')
            allowed=next(s.split(':',1)[1] for s in Path('/proc/self/status').read_text().splitlines() if s.startswith('Mems_allowed_list:'))
            require(selected in cpulist(allowed),'NIC-local NUMA node excluded by memory cpuset')
            active=[p.name for p in (base/'ports').iterdir() if 'ACTIVE' in (p/'state').read_text()]
            require(active,'selected HCA has no active port')
            return dict(node=selected,active_ports=active,cpus=sorted(cpus & os.sched_getaffinity(0)))
        numa=check('numa',numa_info)
        window=max(block_bytes(n,args.unit) for n in args.sizes)
        if numa and 'local_numa' in args.backings and 'native' in args.page_modes:
            def ordinary_memory():
                entries=Path(f'/sys/devices/system/node/node{numa["node"]}/meminfo').read_text().splitlines()
                free=int(next(s for s in entries if 'MemFree:' in s).split()[-2])*1024
                reused=False
                if args.node==spec['pair']['receiver']:
                    try:
                        from callosum_write_matrix import receiver_health
                        os.environ['CALLOSUM_CONTROL_TOKEN']=secret
                        receiver_health(spec,c,args,'native'); reused=True
                    except Exception: pass
                required=0 if reused else window
                require(free>=required,f'NIC-local NUMA free DRAM {free} below native window {required}; no remote-NUMA fallback')
                return dict(node=numa['node'],free_bytes=free,required_bytes=required,reused=reused,reserved=False)
            check('ordinary_memory',ordinary_memory)
        needed=allocation_geometry(window,'huge2MB')//2**21 if 'huge2MB' in args.page_modes and 'local_numa' in args.backings else 0
        result['hugepages_required_for_new_buffer']=needed
        if needed:
            def huge_permission():
                effective=int(next(s.split(':')[1] for s in Path('/proc/self/status').read_text().splitlines()
                                   if s.startswith('CapEff:')).strip(),16)
                group=int(Path('/proc/sys/vm/hugetlb_shm_group').read_text())
                require(os.geteuid()==0 or effective & (1<<14) or group in os.getgroups(),
                        'HugeTLB permission missing: use the lab-approved privileged account or HugeTLB group; provisioning pages does not grant allocation permission')
                return dict(uid=os.geteuid(),hugetlb_group=group)
            check('hugetlb_permission',huge_permission)
        soft,hard=resource.getrlimit(resource.RLIMIT_MEMLOCK)
        required=max(allocation_geometry(window,m) for m in args.page_modes)
        if 'cxl' in args.backings: required=max(required,c['pools'][node['pool']]['capacity'])
        result['memlock']=dict(soft=soft,hard=hard,required=required)
        if soft!=resource.RLIM_INFINITY and soft<required and hard!=resource.RLIM_INFINITY and hard<required:
            result['issues'].append(dict(check='memlock',error=f'hard limit {hard} below {required}; an authorized OS/session limit change is required'))
        if numa and needed:
            pages=check('hugepages',lambda:huge_snapshot(numa['node']))
            # Existing matching receiver MRs consume pages but must not reserve them twice.
            reused=False
            if args.node==spec['pair']['receiver']:
                try:
                    from callosum_write_matrix import receiver_health
                    os.environ['CALLOSUM_CONTROL_TOKEN']=secret
                    receiver_health(spec,c,args,'huge2MB'); reused=True
                except Exception: pass
            result['reused_huge_receiver']=reused
            if pages and not reused and pages['available_pages']<needed:
                result['issues'].append(dict(check='hugepages_capacity',error=f'NUMA {numa["node"]}: {pages["available_pages"]} unreserved/free pages; need {needed}',
                    next_action='provision-hugepages on this node, then repeat prepare'))
        def output_check():
            root.mkdir(parents=True,exist_ok=True)
            probe=root/('.prepare-'+secrets.token_hex(8))
            with probe.open('x') as stream: stream.write('output access probe')
            probe.unlink()  # Only this invocation's private probe; no evidence removed.
            return str(root)
        check('output',output_check)
        def ports_check():
            from callosum_write_matrix import receiver_health
            records={}
            for mode in args.page_modes:
                compatible=False
                if args.node==spec['pair']['receiver']:
                    try:
                        os.environ['CALLOSUM_CONTROL_TOKEN']=secret
                        receiver_health(spec,c,args,mode); compatible=True
                    except Exception: pass
                for port in (spec['control_ports'][mode],spec['te_ports'][mode]):
                    if compatible:
                        records[str(port)]='COMPATIBLE_RECEIVER_REUSE'
                    else:
                        with socket.socket() as probe: probe.bind((urlparse(node['url']).hostname,port))
                        records[str(port)]='AVAILABLE'
            return records
        check('ports',ports_check)
        if 'cxl' in args.backings:
            def dax_access():
                st=os.stat(node['path'])
                require(stat.S_ISCHR(st.st_mode) and os.access(node['path'],os.R_OK|os.W_OK),
                        'DAX path is not an accessible character device')
                base=Path(f'/sys/dev/char/{os.major(st.st_rdev)}:{os.minor(st.st_rdev)}')
                size=int((base/'size').read_text().strip(),0); alignment=int((base/'align').read_text().strip(),0)
                p=c['pools'][node['pool']]
                require(p.get('map_offset',0)+p['capacity']<=size and p.get('map_offset',0)%alignment==0
                        and p['capacity']%alignment==0,'configured extent exceeds real device or alignment')
                return dict(path=node['path'],device_bytes=size,alignment_bytes=alignment,declared_owner=p['owner'])
            check('dax_access',dax_access)
            needed_destination=4096+sum(math.ceil(block_bytes(n,args.unit)/4096)*4096 for n in args.sizes)*args.repetitions*len(args.page_modes)+2**21
            needed_source=4096+math.ceil(window/4096)*4096*len(args.page_modes)+2**21
            result['planned_arena_bytes']=dict(source=needed_source,destination=needed_destination)
            destination=c['pools'][next(n for n in c['nodes'] if n['id']==spec['pair']['receiver'])['pool']]
            if c.get('max_inflight_bytes',64*1024**2)<window:
                result['issues'].append(dict(check='cxl_budget',error=f'live startup budget must cover {window} bytes; do not edit a live MR manifest'))
            if destination['capacity']<needed_destination:
                result['issues'].append(dict(check='cxl_arena',error=f'destination capacity {destination["capacity"]} below planned {needed_destination}; exclusive range ownership must be established before enlargement'))
            os.environ['CALLOSUM_CONTROL_TOKEN']=secret
            status=check('cxl_status',lambda:rpc(c['master'],'status',timeout=5))
            for member in c['nodes']:
                health=check('agent_'+member['id'],lambda member=member:rpc(member['url'],'health',timeout=5))
                if health:
                    if health.get('abi_version')!=2 or health.get('configured_qps')!='16':
                        result['issues'].append(dict(check='transport_'+member['id'],error='ABI2 and 16 configured QPs required'))
                    options=spec.get('transport_env',{}).get(member['id'],{})
                    expected={k:str(options[k]) if options.get(k) is not None else None for k in ('MC_MTU','MC_GID_INDEX')}
                    if health.get('transport_env')!=expected:
                        result['issues'].append(dict(check='transport_settings_'+member['id'],
                            error='live MTU/GID environment differs from the matrix; use reviewed settings and matching startup, not a live relabel'))
                    if health.get('transport_request_timing_version')!=1:
                        result['issues'].append(dict(check='timing_'+member['id'],error='live agent does not advertise timing v1',next_action='retire owned campaign; start current matching runtime'))
                    if health.get('runtime_sha256')!=RUNTIME_SHA256 or health.get('implementation_sha256')!=IMPLEMENTATION_SHA256:
                        result['issues'].append(dict(check='source_'+member['id'],error='running Python runtime differs from this checkout; native engine age is not established by this check'))
                    if 'huge2MB' in args.page_modes:
                        pages=health.get('mr_pages') or {}
                        if not (pages.get('device_alignment_bytes')==2**21 and pages.get('kernel_page_size_bytes')==2**21
                                and pages.get('virtual_alignment_2MiB')):
                            result['issues'].append(dict(check='dax_geometry_'+member['id'],error='live 2 MiB DAX geometry evidence missing'))
            if status:
                if status.get('fingerprint')!=c['fingerprint']:
                    result['issues'].append(dict(check='cxl_identity',error='live campaign configuration differs'))
                versions=status.get('benchmark_request_timing_versions',{})
                if versions.get(spec['pair']['source'])!=1:
                    result['issues'].append(dict(check='coordinator_timing',error='coordinator timing registry is missing/incompatible; compare direct agent health above'))
                if not status.get('topology_verified'):
                    result['issues'].append(dict(check='topology_gate',error='existing authorized CXL topology/smoke gate not complete'))
                if any(o.get('state') not in ('COMMITTED','DRAINED_RETAINED') for o in status.get('operations',[])):
                    result['issues'].append(dict(check='lifetime',error='nonterminal/unknown operation; retain owners; no retry/retirement'))
                pool=next(n['pool'] for n in c['nodes'] if n['id']==spec['pair']['receiver'])
                free=status.get('remaining_capacity_bytes',{}).get(pool)
                if free is None or free<needed_destination:
                    result['issues'].append(dict(check='remaining_arena',error=f'coordinator reports {free} free destination bytes; plan needs {needed_destination}; no unsafe reclaim'))
                source_pool=next(n['pool'] for n in c['nodes'] if n['id']==spec['pair']['source'])
                free_source=status.get('remaining_capacity_bytes',{}).get(source_pool)
                if free_source is None or free_source<needed_source:
                    result['issues'].append(dict(check='remaining_source_arena',error=f'coordinator reports {free_source} free source bytes; plan needs {needed_source}; no unsafe reclaim'))
        probe='''import json,hashlib,pathlib,os
import mooncake.engine as e
import callosum_relay_native as n
t=e.TransferEngine()
assert n.ABI_VERSION==t.relay_abi_version()==2
assert hasattr(t,'relay_write_batch_drained') and hasattr(t,'relay_rdma_available')
assert pathlib.Path(n.__file__).resolve().parent==pathlib.Path(os.environ['CALLOSUM_NATIVE_BUILD']).resolve()
assert pathlib.Path(e.__file__).resolve()==pathlib.Path(os.environ['CALLOSUM_EXPECT_ENGINE']).resolve()
print(json.dumps({str(pathlib.Path(m.__file__).resolve()):hashlib.sha256(pathlib.Path(m.__file__).read_bytes()).hexdigest() for m in (e,n)}))'''
        def origins():
            build=json.loads((receipt/'setup.json').read_text())['build']
            selected=dict(env,CALLOSUM_EXPECT_ENGINE=str(Path(build)/'python/mooncake/engine.so'))
            return json.loads(subprocess.check_output([env['PYTHON_BIN'],'-c',probe],env=selected,text=True,stderr=subprocess.PIPE))
        check('module_origins',origins)
    result.pop('matrix_config',None); result.pop('configuration',None)
    # configuration is a tuple with an environment containing a secret. Never serialize it.
    if not result['issues']: result['status']='PREPARED_NOT_TESTED'
    if secret: result=signed(result,secret)
    archive(work,'readiness.json',result)
    return result


def verify_peer(path,secret,c,spec,args):
    peer=read_proof(path,secret)
    require(peer.get('status')=='PREPARED_NOT_TESTED' and peer.get('plan')==plan(c,spec,args) and
            peer.get('software')==software(),'peer not prepared for the same plan/source; rerun prepare on both hosts')
    require(peer.get('node')==next(n for n in spec['pair'].values() if n!=args.node),'wrong peer readiness node')
    require(time.time_ns()-peer['created_unix_ns']<3600*10**9,'preparation evidence older than one hour; inspect again')
    require(peer.get('machine',{}).get('machine_id')!=Path('/etc/machine-id').read_text().strip(),
            'peer report is from the same physical host')
    return peer


def initialize(path,args):
    """Create a per-host matrix configuration from an already approved topology."""
    path=Path(path).resolve()
    require(not path.exists(),'matrix config exists; preserve it and use prepare')
    require(args.topology and args.setup_run and args.peer_setup_run and args.token_file,
            'init requires --topology, --setup-run, --peer-setup-run and --token-file')
    topology=Path(args.topology).resolve(); c=config(topology)
    launch=c.get('lab_run',{})
    source=args.source or launch.get('source'); receiver=args.receiver or launch.get('requester')
    pair=dict(source=source,receiver=receiver)
    require(source!=receiver and args.node in pair.values() and all(n in {p['id'] for p in c['nodes']} for n in pair.values()),
            'select --source/--receiver or supply the approved lab_run IDs')
    local=Path(args.setup_run).resolve(); peer=Path(args.peer_setup_run).resolve()
    require((local/'setup.json').is_file(),'local setup receipt missing')
    private=Path(args.token_file).resolve()
    require(private.is_file(),'existing shared private token required; do not generate one independently per host')
    value=dict(schema_version=1,topology_file=str(topology),token_file=str(private),
        output_root=str(path.parents[0]/'runs/write-matrix'),pair=pair,
        runtimes={args.node:dict(setup_run=str(local)),
                  next(n for n in pair.values() if n!=args.node):dict(setup_run=str(peer))},
        numa_nodes={},control_ports=dict(native=19201,huge2MB=19202),te_ports=dict(native=19401,huge2MB=19402),
        transport_env={n:dict(MC_MTU=None,MC_GID_INDEX=None) for n in pair.values()})
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x') as stream: json.dump(value,stream,indent=2)
    return dict(status='CONFIG_CREATED_NOT_VALIDATED',config=str(path),services_started=False,devices_mapped=False)


def adopt_build(source,old_run,new_run,env):
    """Reuse an explicit ABI2 artifact, not claim a full rebuild from current HEAD."""
    source,old_run,new_run=map(Path,(source,old_run,new_run))
    old=json.loads((old_run/'setup.json').read_text())
    require(old['status']=='PASS','old build receipt must be PASS')
    native_files=[f for f in SOURCE_FILES if not f.startswith('scripts/')]
    require(all(old.get('source_hashes',{}).get(f)==digest(source/f) for f in native_files),
            'native contract source differs; use the existing lab prepare/build action in a fresh campaign')
    require(not (new_run/'setup.json').exists() and not (new_run/'topology.json').exists(),
            'new campaign must be fresh; do not overwrite a receipt/journal')
    # No TE.initialize, mapping or registration. Preserve actual module origins.
    code='''import json,pathlib,hashlib,os
import mooncake.engine as e
import callosum_relay_native as n
t=e.TransferEngine()
assert t.relay_abi_version()==n.ABI_VERSION==2
assert hasattr(t,'relay_write_batch_drained') and hasattr(t,'relay_rdma_available')
assert pathlib.Path(e.__file__).resolve()==pathlib.Path(os.environ['CALLOSUM_EXPECT_ENGINE']).resolve()
assert pathlib.Path(n.__file__).resolve().parent==pathlib.Path(os.environ['CALLOSUM_NATIVE_BUILD']).resolve()
print(json.dumps({str(pathlib.Path(m.__file__).resolve()):hashlib.sha256(pathlib.Path(m.__file__).read_bytes()).hexdigest() for m in (e,n)}))'''
    origins=json.loads(subprocess.check_output([env['PYTHON_BIN'],'-c',code],
        env=dict(env,CALLOSUM_EXPECT_ENGINE=str(Path(old['build'])/'python/mooncake/engine.so')),text=True))
    new=dict(old,adopted_from=str(old_run.resolve()),adoption_scope='VERIFIED_EXISTING_ABI2_NOT_REBUILT',
             adopted_modules=origins,source_hashes={f:digest(source/f) for f in SOURCE_FILES})
    new_run.mkdir(parents=True,exist_ok=True)
    with (new_run/'setup.json').open('x') as stream: json.dump(new,stream,indent=2)
    return new


def signal_owned(marker):
    """PID-bound termination. Caller supplies positive drain evidence separately."""
    marker=Path(marker); record=json.loads(marker.read_text()); pid=record['pid']
    require(record.get('stamp') is not None and process_stamp(pid)==record['stamp'],'PID ownership/stamp changed; no signal')
    owner=Path(f'/proc/{pid}').stat().st_uid
    require(os.geteuid()==0 or (marker.stat().st_uid==owner and os.geteuid()==owner),'marker/process ownership differs')
    command=Path(f'/proc/{pid}/cmdline').read_bytes().decode().split('\0')
    require(record['state_dir'] in command and record['config'] in command,'owned command no longer matches')
    require(hasattr(os,'pidfd_open') and hasattr(signal,'pidfd_send_signal'),'PID-bound signalling unavailable; retain process')
    fd=os.pidfd_open(pid,0)
    try:
        require(process_stamp(pid)==record['stamp'],'PID changed before signalling')
        signal.pidfd_send_signal(fd,signal.SIGTERM)
        poll=select.poll(); poll.register(fd,select.POLLIN)
        require(poll.poll(5000),'owned process did not exit; retain other owners; no SIGKILL')
    finally: os.close(fd)
    return dict(pid=pid,stamp=record['stamp'],status='OWNED_PROCESS_EXITED')


def frozen_journal(path):
    """Read only after the coordinator has exited. A journal is not modified."""
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)
    try:
        operations=[dict(id=i,state=s) for i,s in db.execute('SELECT id,state FROM operations')]
        require(all(o['state'] in ('COMMITTED','DRAINED_RETAINED') for o in operations),
                'nonterminal/quarantined journal: keep MR owners; caller timeout or coordinator exit is not DMA drain')
        members={i:json.loads(d) for i,d in db.execute('SELECT id,descriptor FROM members')}
        return dict(operations=operations,members=members)
    finally: db.close()


def retire(old_run,node,secret,work,peer_report=None,exclusive=False):
    """Retire only a proved, exclusive canonical TRANSFER_ONLY campaign.

    Control-master exit seals canonical dispatch. It does not establish DMA drain.
    Terminal native-drained journal records and matching live agent epochs do.
    Other/direct users of these arenas are outside this bounded retirement contract.
    """
    require(exclusive,'retire requires --ack-exclusive-campaign: stop benchmark initiators first; unrelated/direct writers are not authorized')
    old_run=Path(old_run).resolve(); work=Path(work)
    old=config(old_run/'topology.json'); controller=old['lab_run']['controller']
    previous=work/'retired.json'
    if previous.exists():
        proof=read_proof(previous,secret)
        require(proof['fingerprint']==old['fingerprint'] and proof['node']==node,'old retirement proof differs')
        require(process_stamp(proof['agent_exit']['pid'])!=proof['agent_exit']['stamp'],'retired process is still alive')
        return proof
    require(hmac.compare_digest((old_run/'control.token').read_text().strip(),secret),'old campaign token differs')
    authority=json.loads((old_run/'authorization.json').read_text())
    require(authority.get('confirmed') is True and authority['config_sha256']==digest(old_run/'topology.json'),
            'old exclusive-extent authorization missing or changed')
    old_build=json.loads((old_run/'setup.json').read_text())
    source=Path(__file__).resolve().parents[1]
    contracts=['mooncake-integration/transfer_engine/transfer_engine_py.cpp',
               'mooncake-integration/pool_relay/relay_batch.h']
    require(all(old_build.get('source_hashes',{}).get(f)==digest(source/f) for f in contracts),
            'old native drain contract is not the verified ABI2 implementation; retain owners for explicit drain review')
    os.environ['CALLOSUM_CONTROL_TOKEN']=secret
    seal=work/'coordinator-sealed.json'
    if node==controller:
        if not seal.exists():
            status=rpc(old['master'],'status',timeout=5)
            require(status['fingerprint']==old['fingerprint'] and all(o['state'] in ('COMMITTED','DRAINED_RETAINED')
                    for o in status.get('operations',[])),'old coordinator busy/unknown; retain all owners')
            # Archive before signalling. The coordinator has no payload MR.
            archive(work,'status-before-retirement.json',status)
            summary=json.loads((old_run/'summary.json').read_text())
            require(summary.get('status')=='PASS' and summary.get('config_sha256')==digest(old_run/'topology.json'),
                    'matching successful canonical smoke/ownership evidence required')
            exited=signal_owned(old_run/'master.process.json')
            archive(work,'coordinator-sealed.json',signed(dict(node=node,fingerprint=old['fingerprint'],
                    controller_exit=exited,created_unix_ns=time.time_ns()),secret))
        proof=read_proof(seal,secret)
        require(process_stamp(proof['controller_exit']['pid'])!=proof['controller_exit']['stamp'],
                'coordinator has not exited')
        frozen=frozen_journal(old_run/'state/master.sqlite')
        proof.update(frozen=frozen,canonical_dispatch_sealed=True)
        archive(work,'coordinator-sealed.json',signed(proof,secret))
    else:
        require(peer_report,'retire the controller first, then supply its signed --peer-report retired.json')
        proof=read_proof(peer_report,secret)
        require(proof.get('canonical_dispatch_sealed') and proof['fingerprint']==old['fingerprint']
                and proof['node']==controller,'controller retirement proof differs')
        from callosum_lab import port_in_use
        require(not port_in_use(old['master']),'a coordinator is still listening; do not retire its MR owners')
        frozen=proof['frozen']
    # Stop agents only after canonical journal completion, never based on process death/TTL.
    require(set(frozen['members'])=={n['id'] for n in old['nodes']},'frozen membership incomplete')
    require(all(m.get('abi_version')==2 for m in frozen['members'].values()),'unknown native completion contract')
    require(all(o['state'] in ('COMMITTED','DRAINED_RETAINED') for o in frozen['operations']),'unknown frozen operation')
    member=next(n for n in old['nodes'] if n['id']==node)
    approved=work/'agent-drain-approved.json'
    marker=old_run/f'{node}.process.json'
    if approved.exists():
        approval=read_proof(approved,secret)
        require(approval['frozen']==frozen,'drain approval differs from frozen journal')
        record=approval['process']
        if process_stamp(record['pid'])!=record['stamp']:
            exited=dict(pid=record['pid'],stamp=record['stamp'],status='EXIT_AFTER_POSITIVE_DRAIN_APPROVAL')
        else: exited=signal_owned(marker)
    else:
        require(rpc(member['url'],'health',timeout=5)==frozen['members'][node],'agent epoch/source differs from frozen membership')
        dispatch=rpc(member['url'],'dispatch_status',timeout=5)
        require(all(v=='DRAINED' for v in dispatch.values()),'source dispatcher is not drained')
        archive(work,'agent-drain-approved.json',signed(dict(frozen=frozen,process=json.loads(marker.read_text())),secret))
        exited=signal_owned(marker)
    proof.update(node=node,campaign=str(old_run),agent_exit=exited,status='RETIRED_NOT_RECLAIMED',reclaimed=False,
                 created_unix_ns=time.time_ns())
    proof=signed(proof,secret); archive(work,'retired.json',proof)
    return proof


def stop_receivers(campaign,spec,c,args):
    """Seal targets before signalling only recorded owned receiver processes."""
    campaign=Path(campaign).resolve(); results=[]
    require(campaign.is_dir(),'receiver evidence directory missing')
    node=next(n for n in c['nodes'] if n['id']==args.node)
    for mode in args.page_modes:
        marker=campaign/f'{mode}.process.json'
        if not marker.exists():
            results.append(dict(mode=mode,status='NO_OWNED_PROCESS_MARKER')); continue
        record=json.loads(marker.read_text())
        require(record['mode']==mode,'owned process mode differs')
        if process_stamp(record['pid'])!=record['stamp']:
            prior=campaign/f'{mode}-sealed.json'
            require(prior.exists() and json.loads(prior.read_text()).get('epoch')==record.get('epoch'),
                    'owner no longer running without seal proof; process death is not drain')
            results.append(dict(mode=mode,status='PREVIOUSLY_SEALED_EXIT')); continue
        url=f'http://{urlparse(node["url"]).hostname}:{spec["control_ports"][mode]}'
        health=rpc(url,'health',timeout=5)
        require(health['node_id']==args.node and health['fingerprint']==c['fingerprint'] and
                health['source_identity']==source_identity() and record.get('epoch')==health['epoch'],
                'receiver identity/source/epoch differs; no signal')
        sealed=rpc(url,'seal',timeout=5)
        require(sealed.get('sealed') and sealed.get('epoch')==health['epoch'],'receiver not sealed')
        archive(campaign,f'{mode}-sealed.json',sealed)
        results.append(dict(mode=mode,exit=signal_owned(marker)))
    archive(campaign,'receiver-cleanup.json',dict(results=results,reclaimed_hugepage_pool=False))
    return dict(results=results,reclaimed_hugepage_pool=False)
