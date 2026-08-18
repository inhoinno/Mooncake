# Mooncake Store Benchmarks

This directory contains benchmark tools for Mooncake Store internals.

## CacheLib Buffer Allocator Microbenchmark

`cachelib_allocator_bench` isolates `CachelibBufferAllocator`, the logical
allocator Mooncake uses over a mounted client memory segment. It creates one
real anonymous DRAM mapping, aligns it to CacheLib's slab boundary, and allocates
configurable fixed-size objects from that fixed address space. It does not
start Mooncake Master, publish replicas, perform metadata RPCs, or transfer KV
payloads; those are intentionally outside this allocator-only measurement.

Build and run the four-check preflight:

```bash
cmake --build build --target cachelib_allocator_bench -j$(nproc)
./build/mooncake-store/benchmarks/cachelib_allocator_bench --self_test
```

Or configure, build, preflight, and run the default one-million-object case in
one command on the Linux lab box:

```bash
bash scripts/run_cachelib_allocator_bench.sh
```

The script accepts `CACHELIB_BENCH_NUM_OBJECTS`,
`CACHELIB_BENCH_OBJECT_SIZE`, `CACHELIB_BENCH_POOL_SIZE_BYTES`, and
`CACHELIB_BENCH_TOUCH_MEMORY=1`.

Run the requested one-million-object baseline:

```bash
./build/mooncake-store/benchmarks/cachelib_allocator_bench \
  --num_objects=1000000 \
  --object_size=4096 \
  --pool_size_bytes=8589934592
```

By default the mapping reserves a real virtual DRAM address range but the
benchmark measures only allocator bookkeeping. Add `--touch_memory` to write
every byte and include page commitment/DRAM initialization in the result. The
output reports successful/failed objects, allocation latency and rate, logical
bandwidth, deallocation rate, and the allocator's final requested-byte count.

Interpret the timing fields separately. `mean_ns`, `p50_ns`, and `p99_ns` time
only the `CachelibBufferAllocator::allocate()` call. `elapsed_ms`,
`objects_per_second`, and `logical_gbps` cover the complete allocation loop and
therefore include `memset` when `--touch_memory` is enabled. Consequently, a
touched run can retain nearly identical allocator percentiles while reporting
much lower end-to-end throughput. The touched logical GB/s is a single-threaded
first-write rate over anonymous DRAM, including allocation and page commitment;
it is not an isolated DRAM read/write result and is not CXL bandwidth.

## Allocation Strategy Benchmark

`allocation_strategy_bench` evaluates Store allocation behavior across segment
counts, replica counts, allocation strategies, and workload patterns.

Build the benchmark from an existing CMake build directory:

```bash
cmake --build build --target allocation_strategy_bench -j$(nproc)
```

### Size-Class Churn Fragmentation Benchmark

The `size_class_churn` workload measures fragmentation under mixed-size
KVCache-like allocation pressure. It pre-fills the simulated cluster when
`--prefill_pct` is set, then repeatedly allocates objects from weighted size
classes. On allocation failure it randomly evicts a fraction of live objects and
retries.

When prefill is enabled, the prefill attempt cap is auto-derived from target
utilization, total cluster capacity, weighted average object size, and replica
count, with a 5000-attempt minimum for small cases.

This is an allocation-strategy-layer benchmark. It complements the existing
`dsa` workload by adding explicit fragmentation sampling and configurable
weighted size-class patterns. It is not a replacement for `allocator_bench`,
which remains the low-level `OffsetAllocator` microbenchmark.

Run a small local validation:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn \
  --segment_capacity=1024 \
  --num_allocations=10000 \
  --prefill_pct=70
```

Run a larger baseline:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn \
  --segment_capacity=1024 \
  --num_allocations=100000 \
  --prefill_pct=80
```

Supported size-class patterns:

- `kv_mixed`: 4KB at 70%, 256KB at 20%, and 3.12MB at 10%.
- `dsa_pair`: 3.12MB KV pages at 50% and 643KB indexer entries at 50%.
- `all`: run both patterns.

Key output columns:

- `Throughput`, `Avg(ns)`, `P50(ns)`, `P90(ns)`, and `P99(ns)` measure
  allocation performance.
- `Frag_avg`, `Frag_p50`, `Frag_p90`, and `Frag_p99` summarize sampled
  fragmentation ratios.
- `LargestFreeMB` shows the final largest contiguous free region.
- `Evictions` counts fail-triggered eviction rounds during measurement.
- `Full/Partial/Fail/Total` reports allocation outcomes. Only results with
  `result->size() == replica_num` count as full success; shorter replica
  results are counted as partial allocations.

Fragmentation is computed per `OffsetBufferAllocator` and then averaged by free
space:

```text
1 - largest_free_region / total_free_space
```

The weighted average avoids treating free space in different Store segments as
one mergeable region. `LargestFreeMB` still reports the final largest contiguous
free region across all segments.

The benchmark also prints a one-line `Prefill summary`, `Fragmentation summary`,
and `Size-class breakdown` after each result row, so reviewers can read the
actual prefill utilization, fragmentation, and per-size-class latency numbers
without manually deriving them from the table.
