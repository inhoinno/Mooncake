"""Fixed-byte sender-WRITE loop. Source initialization and checksums are outside it."""
import time

SIZES = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512)
TOTAL_BYTES = 8 * 1024**3


def block_bytes(size, unit):
    if size not in SIZES or unit not in ('MB', 'MiB'):
        raise ValueError('declare block size 1..512 powers of two and MB or MiB')
    return size * (10**6 if unit == 'MB' else 1024**2)


def write_loop(write, source, destination, block, total, limit):
    if min(source, destination, block, total, limit) <= 0:
        raise ValueError('invalid transport window')
    # Reuse one exclusively reserved block window only after each native call
    # returns drained. No concurrent writer or visible cache object uses it.
    completed = calls = logical_blocks = 0
    start = time.monotonic_ns()
    while completed < total:
        length = min(block, total - completed)
        pos = 0
        while pos < length:
            n = min(limit, length - pos)
            actual = write(source + pos, destination + pos, n)
            if actual != n:
                raise RuntimeError('short transport WRITE; preserve reserved window')
            completed += n
            pos += n
            calls += 1
        logical_blocks += 1
    return dict(bytes=completed, logical_blocks=logical_blocks, native_calls=calls,
                sender_write_loop_ns=time.monotonic_ns()-start,
                block_bytes=block, max_request_bytes=limit)
