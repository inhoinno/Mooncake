"""Fixed-byte sender-WRITE loop. Source initialization and checksums are outside it."""
import time

SIZES = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
TOTAL_BYTES = 8 * 1024**3


def block_bytes(size, unit):
    if size not in SIZES or unit not in ('MB', 'MiB'):
        raise ValueError('declare block size 1..512 powers of two and MB or MiB')
    return size * (10**6 if unit == 'MB' else 1024**2)


def write_loop(write, source, destination, block, total, limit, samples_out=None):
    if min(source, destination, block, total, limit) <= 0:
        raise ValueError('invalid transport window')
    # Reuse one exclusively reserved block window only after each native call
    # returns drained. No concurrent writer or visible cache object uses it.
    completed = calls = logical_blocks = 0
    samples = samples_out if samples_out is not None else []
    if samples or (total + block - 1)//block > 100000:
        raise ValueError('request sample buffer must be empty and bounded')
    wall_start = time.time_ns()
    start = time.monotonic_ns()
    while completed < total:
        length = min(block, total - completed)
        pos = 0
        request_start = time.monotonic_ns()
        request_calls = 0
        status = 'PASS'
        try:
            while pos < length:
                n = min(limit, length - pos)
                request_calls += 1
                actual = write(source + pos, destination + pos, n)
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
                sender_start_offset_ns=request_start-start, sender_end_offset_ns=request_end-start,
                write_request_e2e_ns=request_end-request_start, native_calls=request_calls, status=status))
        logical_blocks += 1
    return dict(bytes=completed, logical_blocks=logical_blocks, native_calls=calls,
                sender_write_loop_ns=time.monotonic_ns()-start,
                block_bytes=block, max_request_bytes=limit, requests=samples,
                sender_start_unix_ns=wall_start,sender_end_unix_ns=time.time_ns(),
                request_timing_version=1,
                latency_scope='sender logical WRITE call start through drain; excludes control and checksum',
                instrumentation='two monotonic reads and in-memory sample append per logical request; no loop file I/O')
