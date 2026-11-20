"""Fixed-byte sender-WRITE loop. Source initialization and checksums are outside it."""
import time
import random
import hashlib
import stat
import threading
import os
import json
from abc import ABC, abstractmethod
from pathlib import Path

SIZES = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
TOTAL_BYTES = 8 * 1024**3
ACCESS_PATTERNS=('overwrite','sequential','uniform_random')
MIXED_DEVICE = '/dev/cxl_mixed'
MIXED_WRITE = 'direct_cxl_mixed_write'


def mixed_device_info(path):
    """Inventory the driver's character device, without opening or mapping it."""
    path = Path(path)
    if str(path) != MIXED_DEVICE:
        raise ValueError(f'mixed CXL requires {MIXED_DEVICE}')
    info = path.lstat()
    if not stat.S_ISCHR(info.st_mode):
        raise ValueError(f'{path} must be a character device, not a symlink or file')
    return info


class AccessStrategy(ABC):
    def __init__(self,block,window,seed=0):
        if type(block) is not int or type(window) is not int or not 0<block<=window:
            raise ValueError('block must fit the authorized access window')
        if type(seed) is not int or not 0<=seed<2**64:
            raise ValueError('random seed must be an unsigned 64-bit integer')
        self.block,self.window,self.slots=block,window,window//block

    @abstractmethod
    def offset(self,index): pass


class OverwriteStrategy(AccessStrategy):
    def offset(self,index): return 0


class SequentialStrategy(AccessStrategy):
    def offset(self,index): return (index % self.slots)*self.block


class UniformRandomStrategy(AccessStrategy):
    def __init__(self,block,window,seed=0):
        super().__init__(block,window,seed)
        self.rng=random.Random(seed)

    def offset(self,index): return self.rng.randrange(self.slots)*self.block


def access_strategy(pattern,block,window=None,seed=0):
    strategies=dict(overwrite=OverwriteStrategy,sequential=SequentialStrategy,
                    uniform_random=UniformRandomStrategy)
    if pattern not in strategies: raise ValueError('unknown access pattern')
    return strategies[pattern](block,block if window is None else window,seed)


def written_ranges(samples,window):
    intervals=[]
    for sample in samples:
        offset=sample.get('source_offset_bytes',0); target=sample.get('destination_offset_bytes',0)
        length=sample['request_bytes']
        if type(offset) is not int or offset!=target or offset<0 or length<=0 or offset+length>window:
            raise ValueError('WRITE sample exceeds its authorized window')
        intervals.append((offset,offset+length))
    merged=[]
    for begin,end in sorted(intervals):
        if merged and begin<=merged[-1][1]: merged[-1][1]=max(end,merged[-1][1])
        else: merged.append([begin,end])
    return merged


def final_window_digest(memory,window,ranges):
    """Expected final image after zeroing a target and copying matching offsets."""
    digest=hashlib.sha256(); cursor=0; zeros=bytes(1024**2)
    for begin,end in [*ranges,[window,window]]:
        if not cursor<=begin<=end<=window: raise ValueError('invalid checksum coverage')
        while cursor<begin:
            size=min(len(zeros),begin-cursor); digest.update(zeros[:size]); cursor+=size
        while cursor<end:
            size=min(len(zeros),end-cursor); digest.update(memory[cursor:cursor+size]); cursor+=size
    return digest.hexdigest()


def block_bytes(size, unit):
    if size not in SIZES or unit not in ('MB', 'MiB'):
        raise ValueError('declare block size 1..512 powers of two and MB or MiB')
    return size * (10**6 if unit == 'MB' else 1024**2)


def write_loop(write, source, destination, block, total, limit, samples_out=None,*,
               access_pattern='overwrite',window_bytes=None,random_seed=0):
    if min(source, destination, block, total, limit) <= 0:
        raise ValueError('invalid transport window')
    # Reuse one exclusively reserved block window only after each native call
    # returns drained. No concurrent writer or visible cache object uses it.
    completed = calls = logical_blocks = 0
    strategy=access_strategy(access_pattern,block,window_bytes,random_seed)
    samples = samples_out if samples_out is not None else []
    if samples or (total + block - 1)//block > 100000:
        raise ValueError('request sample buffer must be empty and bounded')
    wall_start = time.time_ns()
    start = time.monotonic_ns()
    while completed < total:
        length = min(block, total - completed)
        offset=strategy.offset(logical_blocks)
        pos = 0
        request_start = time.monotonic_ns()
        request_calls = 0
        status = 'PASS'
        try:
            while pos < length:
                n = min(limit, length - pos)
                request_calls += 1
                actual = write(source + offset + pos, destination + offset + pos, n)
                if actual != n:
                    raise RuntimeError('short transport WRITE; preserve reserved window')
                completed += n
                pos += n
                calls += 1
        except BaseException:
            status = 'FAILED_OR_UNKNOWN'
            raise
        finally:
            request_end = time.monotonic_ns()
            samples.append(dict(request_index=logical_blocks, request_bytes=length,
                source_offset_bytes=offset,destination_offset_bytes=offset,
                sender_start_offset_ns=request_start-start, sender_end_offset_ns=request_end-start,
                write_request_e2e_ns=request_end-request_start, native_calls=request_calls, status=status))
        logical_blocks += 1
    return _write_result(samples,completed,logical_blocks,calls,block,limit,strategy,
                         access_pattern,random_seed,start,wall_start)


def _write_result(samples, completed, logical_blocks, calls, block, limit, strategy,
                  access_pattern, random_seed, start, wall_start, elapsed=None, wall_end=None):
    return dict(bytes=completed, logical_blocks=logical_blocks, native_calls=calls,
                sender_write_loop_ns=time.monotonic_ns()-start if elapsed is None else elapsed,
                block_bytes=block, max_request_bytes=limit, requests=samples,
                access_pattern=access_pattern,window_bytes=strategy.window,offset_slots=strategy.slots,
                addressable_window_bytes=strategy.slots*block,unused_window_tail_bytes=strategy.window%block,
                degenerate_one_slot=strategy.slots==1,
                random_seed=random_seed,address_selection_scope='included in loop; excluded from per-request timer',
                sender_loop_start_monotonic_ns=start,
                sender_start_unix_ns=wall_start,sender_end_unix_ns=time.time_ns() if wall_end is None else wall_end,
                request_timing_version=1,
                latency_scope='sender logical WRITE call start through drain; excludes control and checksum',
                instrumentation='offset selection (including random RNG), two monotonic reads and in-memory samples; no loop file I/O')


def pipeline_write_loop(engine, segment, source, destination, block, total, samples, *,
                        owner, mode, depth=4, byte_limit=256<<20, descriptors=1,
                        instrumentation='off',access_pattern='overwrite',window_bytes=None,
                        random_seed=0,source_backing='source',destination_backing='destination',
                        source_offset=0,destination_offset=0,ready=None,drained=None):
    """One Python/native crossing; native serialization and pipeline share work."""
    if mode not in ('serialization','batched') or samples or min(block,total)<=0 or total>TOTAL_BYTES:
        raise ValueError('invalid native pipeline mode/volume/sample buffer')
    strategy=access_strategy(access_pattern,block,window_bytes,random_seed)
    generated=time.monotonic_ns()
    lengths=[min(block,total-i) for i in range(0,total,block)]
    if len(lengths)>100000:
        raise ValueError('native request schedule exceeds bound')
    offsets=[strategy.offset(i) for i in range(len(lengths))]
    sources=[source+offset for offset in offsets]
    destinations=[destination+offset for offset in offsets]
    source_offsets=[source_offset+offset for offset in offsets]
    destination_offsets=[destination_offset+offset for offset in offsets]
    generated=time.monotonic_ns()-generated
    clock_probe=engine.relay_clock_probe() if hasattr(engine,'relay_clock_probe') else None
    wall,call_begin=time.time_ns(),time.monotonic_ns()
    cpu_begin=time.process_time_ns()
    gates={}
    def timed_gate(name,callback):
        begin=time.monotonic_ns()
        if callback:callback()
        gates[name]=time.monotonic_ns()-begin
    result=engine.relay_write_pipeline(segment,sources,destinations,lengths,
        source_offsets,destination_offsets,source_backing,destination_backing,
        mode,depth,byte_limit,descriptors,instrumentation,owner,
        lambda:timed_gate('ready',ready),lambda:timed_gate('drained',drained))
    cpu_ns=time.process_time_ns()-cpu_begin
    call_end=time.monotonic_ns()
    start,duration=result['start_ns'],result['duration_ns']
    if not call_begin<=start<start+duration<=call_end:
        raise RuntimeError('native and Python CLOCK_MONOTONIC domains do not match')
    seen=set()
    for request,operation,begin,end,success in sorted(result['samples']):
        samples.append(dict(request_index=request,request_bytes=lengths[request],
            source_offset_bytes=offsets[request],destination_offset_bytes=offsets[request],
            operation_id=operation,sender_start_offset_ns=begin,sender_end_offset_ns=end,
            write_request_e2e_ns=end-begin,native_calls=int(operation not in seen),
            status='PASS' if success else 'FAILED_OR_UNKNOWN'))
        seen.add(operation)
    if not result['success'] or result['bytes']!=total or len(samples)!=len(lengths):
        raise RuntimeError('native pipeline failed after drain; do not publish')
    admission=dict(result.get('admission_samples') or [])
    for sample in samples:
        sample['admission_blocked_ns']=admission.get(sample['request_index'])
    operations={s['operation_id']:(s['sender_start_offset_ns'],s['sender_end_offset_ns']) for s in samples}
    intervals=sorted(operations.values());covered=0;cursor=0
    for begin,end in intervals:
        covered+=max(0,end-max(cursor,begin));cursor=max(cursor,end)
    row=_write_result(samples,total,len(lengths),result['operations'],block,block,strategy,
        access_pattern,random_seed,start,wall+(start-call_begin),duration,wall+(start-call_begin)+duration)
    row.update(submission_mode=mode,pipeline_depth=1 if mode=='serialization' else depth,
        address_schedule_sha256=hashlib.sha256(json.dumps([offsets,lengths],separators=(',',':')).encode()).hexdigest(),
        sender_pid=os.getpid(),sender_thread_id=threading.get_native_id(),
        native_ready_gate_ns=gates.get('ready'),native_drain_gate_ns=gates.get('drained'),
        clock_probe=clock_probe,
        pipeline_max_bytes=byte_limit,descriptors_per_submit=1 if mode=='serialization' else descriptors,
        pipeline_instrumentation=instrumentation,pipeline_statistics={k:v for k,v in result.items()
            if k not in ('samples','trace','admission_samples')},pipeline_trace=result['trace'],binding_calls=1,
        operation_submit_start_gaps_ns=[b[0]-a[0] for a,b in zip(intervals,intervals[1:])],
        software_operation_idle_ns=duration-covered,
        idle_scope='no retained operation in measured span; not RNIC starvation or wire idle',
        schedule_generation_ns=generated,binding_call_elapsed_ns=call_end-call_begin,
        sender_process_cpu_ns=result.get('process_cpu_ns'),binding_call_process_cpu_ns=cpu_ns,
        native_call_scope='engine operation submissions; attributed to first descriptor',
        latency_scope='logical request submit through containing operation gather/drain',
        measurement_interval='native CLOCK_MONOTONIC: first admission through final drain; excludes schedule generation',
        address_selection_scope='identical prebuilt schedule for native serialization and batched')
    return row


def diagnostic_write_loop(engine, segment, source, destination, block, total, limit, samples,
                          *, access_pattern, window_bytes, random_seed, driver):
    """Same scatter/drain primitive, with Python or C++ driving the block loop."""
    if not 0 < block <= limit or not 0 < total <= TOTAL_BYTES or driver not in ('python','native'):
        raise ValueError('invalid diagnostic WRITE budget/driver')
    phases = []
    def submit(src, dst, length):
        phases.append(engine.relay_write_block_profiled(segment,src,dst,length))
        return length
    strategy = access_strategy(access_pattern,block,window_bytes,random_seed)
    schedule_ns = 0
    if driver == 'python':
        result = write_loop(submit,source,destination,block,total,limit,samples,
                            access_pattern=access_pattern,window_bytes=window_bytes,random_seed=random_seed)
    else:
        wall_start, start = time.time_ns(), time.monotonic_ns()
        lengths = [min(block,total-i) for i in range(0,total,block)]
        offsets = [strategy.offset(i) for i in range(len(lengths))]
        src, dst = ([base + offset for offset in offsets] for base in (source,destination))
        schedule_ns = time.monotonic_ns()-start
        native_samples = engine.relay_write_blocks_diagnostic(segment,src,dst,lengths)
        elapsed, wall_end = time.monotonic_ns()-start, time.time_ns()
        if len(native_samples)!=len(lengths):
            raise RuntimeError('native diagnostic request sample count mismatch')
        for i,(offset,length,(begin,end,*phase)) in enumerate(zip(offsets,lengths,native_samples)):
            phases.append(phase)
            samples.append(dict(request_index=i,request_bytes=length,
                source_offset_bytes=offset,destination_offset_bytes=offset,
                sender_start_offset_ns=begin,sender_end_offset_ns=end,
                write_request_e2e_ns=end-begin,native_calls=1,status='PASS'))
        result = _write_result(samples,total,len(lengths),len(lengths),block,limit,strategy,
                               access_pattern,random_seed,start,wall_start,elapsed,wall_end)
        result['native_schedule_build_ns'] = schedule_ns
    previous_end = 0
    diagnostics = []
    if len(phases)!=len(samples):
        raise RuntimeError('diagnostic phase sample count mismatch')
    for req,(check,submit_ns,wait) in zip(samples,phases):
        begin, end = req['sender_start_offset_ns'],req['sender_end_offset_ns']
        if not (previous_end <= begin < end <= result['sender_write_loop_ns']-schedule_ns and
                min(check,submit_ns,wait)>=0 and check+submit_ns+wait<=end-begin):
            raise RuntimeError('invalid diagnostic phase timing')
        diagnostics.append(dict(request_index=req['request_index'],check_ns=check,
            submit_ns=submit_ns,wait_ns=wait,between_calls_ns=begin-previous_end,
            in_call_residual_ns=end-begin-check-submit_ns-wait))
        previous_end = end
    result.update(diagnostic_driver=driver, diagnostic_requests=diagnostics,
                  diagnostic_request_clock=('native steady-clock offsets within binding'
                                            if driver=='native' else 'sender monotonic offsets'),
                  diagnostic_loop_tail_ns=result['sender_write_loop_ns']-previous_end-schedule_ns,
                  instrumentation='diagnostic only: native phase clocks and in-memory samples')
    return result


def rdma_snapshot(engine, sysroot=Path('/sys')):
    """Timestamp one sender observation; port counters include unrelated traffic."""
    start = time.monotonic_ns()
    contexts = engine.relay_rdma_snapshot()
    for row in contexts:
        root = sysroot/'class/infiniband'/row['device']/'ports'/str(row['port'])
        try:
            # Standard InfiniBand port counters are in units of four octets.
            row['port_tx_counter_bytes'] = 4*int((root/'counters/port_xmit_data').read_text())
        except (OSError,ValueError):
            row['port_tx_counter_bytes'] = None
    return dict(begin_ns=start,end_ns=time.monotonic_ns(),contexts=contexts)


def rdma_inventory(engine, sysroot=Path('/sys'), procroot=Path('/proc')):
    """Read-only placement/linkage evidence, never called by the timed sampler."""
    observed = rdma_snapshot(engine,sysroot)
    observed['native_trace_contexts']=engine.relay_rdma_snapshot(True)
    def read(path):
        try:
            return path.read_text().strip()
        except OSError:
            return None
    for row in observed['contexts']:
        device = sysroot/'class/infiniband'/row['device']/'device'
        port = sysroot/'class/infiniband'/row['device']/'ports'/str(row['port'])
        row['nic_numa_node'] = read(device/'numa_node')
        row['port'] = dict(number=row['port'], **{k:read(port/k) for k in
                           ('rate','state','phys_state','link_layer')})
        resolved = device.resolve()
        row['pcie_ancestry'] = [dict(path=str(p), **{k:read(p/k) for k in
                                  ('current_link_speed','current_link_width','max_link_speed',
                                   'max_link_width','vendor','device')})
                                for p in (resolved,*resolved.parents)
                                if (p/'current_link_speed').is_file()][:8]
        row['port_counters'] = {p.name:read(p) for folder in ('counters','hw_counters')
                                for p in sorted((port/folder).glob('*'))[:128] if p.is_file()}
    tasks = []
    for task in sorted((procroot/'self/task').glob('*'))[:256]:
        status = read(task/'status')
        if status:
            keys = ('Name','State','Cpus_allowed_list','Mems_allowed_list',
                    'voluntary_ctxt_switches','nonvoluntary_ctxt_switches')
            tasks.append(dict(tid=task.name, **{k:v.strip() for k,sep,v in
                (line.partition(':') for line in status.splitlines()) if sep and k in keys},
                schedstat=read(task/'schedstat')))
    observed['threads'] = tasks
    maps = read(procroot/'self/maps') or ''
    libraries = sorted({line.split()[-1] for line in maps.splitlines()
        if len(line.split())>=6 and line.split()[-1].startswith('/') and
        any(name in line.split()[-1] for name in ('engine.so','libibverbs','libmlx5','callosum_relay_native'))})
    observed['loaded_transport_libraries'] = {}
    for name in libraries:
        try:
            observed['loaded_transport_libraries'][name] = hashlib.sha256(Path(name).read_bytes()).hexdigest()
        except OSError:
            observed['loaded_transport_libraries'][name] = None
    observed['backend'] = 'legacy_rdma_required_by_relay'
    observed['scope'] = 'outside transfer loop; port counters include unrelated traffic'
    return observed


def rdma_trial_progress(before, after, payload_bytes):
    """Keep outstanding time per context; it overlaps submit/wait and other rails."""
    names = lambda snapshot: {(x['device'],x['port']):x for x in snapshot['contexts']}
    first,last = names(before),names(after)
    if not first or first.keys()!=last.keys():
        return dict(status='CONTEXTS_CHANGED')
    rows = []
    for key,start in first.items():
        end = last[key]
        if not start['tracked'] or not end['tracked']:
            return dict(status='TRACKING_DISABLED')
        if (start['pending_write_wrs'] or end['pending_write_wrs'] or
                start.get('pending_write_bytes',0) or end.get('pending_write_bytes',0)):
            return dict(status='OBSERVATION_BOUNDARY_NOT_IDLE')
        old,new = start['qp_completed_bytes'],end['qp_completed_bytes']
        if (end['completed_write_bytes']<start['completed_write_bytes'] or
                end['write_outstanding_ns']<start['write_outstanding_ns'] or
                any(new.get(q,-1)<n for q,n in old.items())):
            return dict(status='COUNTERS_CHANGED')
        qp = {q:n-old.get(q,0) for q,n in new.items() if n>old.get(q,0)}
        rows.append(dict(device=key[0],port=key[1],active_write_qps=len(qp),qp_completed_bytes=qp,
            rdma_completed_write_bytes=end['completed_write_bytes']-start['completed_write_bytes'],
            tracked_outstanding_ns=end['write_outstanding_ns']-start['write_outstanding_ns'],
            qp_counters_truncated=end['qp_counters_truncated']))
        if end.get('diagnostic_version',1)>=2:
            def deltas(old, new):
                fields = set(old) | set(new)
                if any(new.get(k,0)<old.get(k,0) for k in fields):
                    raise ValueError('diagnostic counters reset')
                return {k:new.get(k,0)-old.get(k,0) for k in fields}
            try:
                qps = {q:deltas(start.get('qp_statistics',{}).get(q,{}),counts)
                       for q,counts in end.get('qp_statistics',{}).items()}
                workers = {w:deltas(start.get('poll_workers',{}).get(w,{}),counts)
                           for w,counts in end.get('poll_workers',{}).items()}
                registration = deltas(start.get('registration',{}),end.get('registration',{}))
            except ValueError:
                return dict(status='COUNTERS_CHANGED')
            rows[-1].update(qp_statistics=qps,poll_workers=workers,registration_delta=registration,
                qp_outstanding_before=start.get('qp_outstanding'),qp_outstanding_after=end.get('qp_outstanding'),
                registration_during_trial=bool(registration.get('calls',0) or
                                               registration.get('deregister_calls',0)),
                peak_pending_write_wrs=end.get('peak_pending_write_wrs'),
                peak_pending_write_bytes=end.get('peak_pending_write_bytes'),
                peak_scope='process lifetime high-water mark, not a per-trial maximum')
            if (sum(q['posted_bytes'] for q in qps.values())!=rows[-1]['rdma_completed_write_bytes'] or
                    any(q['unknown_wrs'] or q['rejected_wrs'] or q['completion_errors'] or
                        q['posted_wrs']!=q['completed_wrs'] for q in qps.values())):
                rows[-1]['posting_accounting_incomplete']=True
    full = (sum(x['rdma_completed_write_bytes'] for x in rows)==payload_bytes and
            all(not x['qp_counters_truncated'] and not x.get('posting_accounting_incomplete') and
                sum(x['qp_completed_bytes'].values())==x['rdma_completed_write_bytes'] for x in rows))
    return dict(status='ACCOUNTED' if full else 'INCOMPLETE',contexts=rows)


def sampled_slice_latencies(inventory,begin,end):
    """Join bounded native events only within this sender CLOCK_MONOTONIC trial."""
    from callosum_benchmark_report import percentile
    samples=[];unmatched=0;overflow=False
    for context in inventory.get('native_trace_contexts',[]):
        queued={};posted={}
        overflow |= context.get('slice_trace_overflow',False)
        for ns,request,offset,length,qp,kind in context.get('slice_trace',[]):
            if not begin<=ns<=end: continue
            key=(request,offset)
            if kind==0: queued[key]=ns
            elif kind==1: posted[key]=(ns,qp)
            elif kind==2:
                q=queued.pop(key,None);p=posted.pop(key,None)
                if q is None or p is None or not q<=p[0]<=ns or p[1]!=qp:
                    unmatched+=1;continue
                samples.append(dict(device=context['device'],port=context['port'],request_index=request-1,
                    slice_offset_bytes=offset,bytes=length,qp=qp,
                    enqueue_to_prepost_ns=p[0]-q,prepost_to_cq_observed_ns=ns-p[0]))
    distributions={}
    for key in ('enqueue_to_prepost_ns','prepost_to_cq_observed_ns'):
        values=[row[key] for row in samples]
        distributions[key]=dict(count=len(values),p50=percentile(values,.5),p95=percentile(values,.95),
                                 p99=percentile(values,.99))
    return dict(samples=samples,distributions=distributions,unmatched_completions=unmatched,
                trace_overflow=overflow,clock='sender CLOCK_MONOTONIC',
                scope='sampled slices; prepost includes doorbell call; CQ observation includes polling; not wire latency')


def sample_rdma(engine, stop, samples):
    # Bound observer storage and do not print or write files in the loop.
    while not stop.is_set() and len(samples)<4096:
        try:
            samples.append(rdma_snapshot(engine))
        except Exception as error:
            samples.append(dict(error=str(error)))
            return
        stop.wait(0.005)
def dax_numa_evidence(path, hca, sysroot=Path('/sys'), *, require_remote=True):
    """Check device backing locality, not the caller's CPU binding."""
    dax = Path(path).name
    if Path(hca).name != hca or hca in ('', '.', '..'):
        raise ValueError('invalid HCA name')
    def read_node(file):
        try:
            return int(file.read_text())
        except (FileNotFoundError,ValueError):
            if require_remote:
                raise
            return -1
    dax_node = read_node(sysroot/'bus/dax/devices'/dax/'target_node')
    nic_node = read_node(sysroot/'class/infiniband'/hca/'device/numa_node')
    if require_remote and (min(dax_node,nic_node)<0 or dax_node==nic_node):
        raise ValueError(f'{path} target_node={dax_node}, {hca} numa_node={nic_node}: '
                         'select an inventoried NUMA-remote source DAX device')
    return dict(path=path, hca=hca, dax_target_node=dax_node, nic_numa_node=nic_node)
