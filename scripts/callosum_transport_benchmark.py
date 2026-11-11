"""Fixed-byte sender-WRITE loop. Source initialization and checksums are outside it."""
import time
import random
import hashlib
from abc import ABC, abstractmethod

SIZES = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
TOTAL_BYTES = 8 * 1024**3
ACCESS_PATTERNS=('overwrite','sequential','uniform_random')


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
    return dict(bytes=completed, logical_blocks=logical_blocks, native_calls=calls,
                sender_write_loop_ns=time.monotonic_ns()-start,
                block_bytes=block, max_request_bytes=limit, requests=samples,
                access_pattern=access_pattern,window_bytes=strategy.window,offset_slots=strategy.slots,
                addressable_window_bytes=strategy.slots*block,unused_window_tail_bytes=strategy.window%block,
                degenerate_one_slot=strategy.slots==1,
                random_seed=random_seed,address_selection_scope='included in loop; excluded from per-request timer',
                sender_start_unix_ns=wall_start,sender_end_unix_ns=time.time_ns(),
                request_timing_version=1,
                latency_scope='sender logical WRITE call start through drain; excludes control and checksum',
                instrumentation='offset selection (including random RNG), two monotonic reads and in-memory samples; no loop file I/O')
