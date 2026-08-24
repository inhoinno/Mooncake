# CXL/RDMA/GPU research baseline and evidence ladder

This document explains what the CPU bootstrap PASS proves, what remains untested, and
how to build a defensible performance baseline for accelerating LLM serving with local
CXL and remote RDMA KV-cache sources.

The governing design remains `../../DESIGN.md`. This document is an experiment runbook,
not a replacement for ownership, publication, or connector semantics.

## 1. Interpretation of the 23-test bootstrap PASS

Evidence captured on commit `d198975e`:

```text
USE_CXL=ON  WITH_STORE=ON  USE_CUDA=OFF
23/23 PASS, all labeled t0
```

The correct conclusion is:

> The CPU build, CXL backend model, file-backed Store integration, GPU path-selection
> policy, and dependency-light one/two-node protocol harnesses are internally
> consistent. The output contains no physical CXL, CUDA, RDMA, or serving-performance
> measurement.

### 1.1 Exact coverage

| Output group | What executed | What the PASS establishes | What it does not establish |
|---|---|---|---|
| `gate=cpu_cache` | CMake cache inspection | Store and CXL compiled while CUDA/GPU transports were disabled | GPU code generation, CUDA runtime, GPU visibility |
| `cxl_pool_backend_{preflight,functional,failure,status}` | Dependency-free FakeTraCT backend tests | Config validation, file mapping, reserve/commit/abort/lookup behavior, bounds/failure cleanup, safe status fields | `/dev/dax`, physical CXL latency/bandwidth, confidential TraCT |
| `gpu_transfer_*` | Pure `SelectGpuReadPath()` and environment-parser tests | CPU destinations stay host-addressable; CXL GPU targets select `cxl_cuda_copy`; network GPU targets stage by default; GDR and traces require explicit valid opt-in | No CUDA pointer, `cudaMemcpyAsync`, NIC, memory registration, or copied byte |
| `gpu_multipath_{preflight,functional,failure,status}` | Policy tests plus `CxlAwareAllocationStrategy` tests | Explicit CXL requests route to CXL; normal network placement excludes CXL; invalid trace settings fail closed | No simultaneous CXL+RDMA transfer and no bandwidth aggregation |
| `cxl_allocation_endpoint_test` | Allocator-manager unit test | A CXL allocation descriptor retains the mounted Transfer Engine endpoint and clears it at final removal | Endpoint reachability or transport traffic |
| `cxl_client_single_process_test` | In-process Master + Store over a regular file | Real single and batch Store PUT/GET path, publication, checksum, and CxlTransport copy logic | It uses `MC_CXL_PROVIDER=faketract`, `MC_CXL_BACKEND_KIND=file`; no DAX or second process/node |
| `todo15_shared_cxl_*` | In-memory fake Store protocol tests | Two-role ordering, bidirectional single/batch accounting, corruption blocking verification, bounded status | No two physical nodes, shared CXL mapping, Master RPC, or payload path |
| `todo15_one_node_cxl_*` | In-memory fake Store harness tests | Native configuration contract, full-pool ownership rule, harness PUT/GET/remove logic, corruption cleanup, safe summary | The harness did not open `/dev/dax` or import a CUDA build |
| artifact PASS | CPU wheel staging | CPU `store.so`, `engine.so`, Master, and wheel were produced for CPython 3.10 | CUDA-enabled ABI or hardware execution |

### 1.2 Two easy-to-misread lines

`cxl_pool_backend_devdax_test` appears in the later status listing, but it is test 13 and
does not appear among the 23 executed tests. The bootstrap command excludes the
`hardware` label, so this output is not a device-DAX PASS.

The nine `gpu` results contain repeated policy cases under different gate names. They
are useful fail-closed regression gates, but nine PASS lines are not nine data-path
measurements. The previously unlabelled `multipath_placement_test` is now included in
the `multisource` T0 gate so stable hash selection, both-source use, invalid-config
rejection, and explicit opt-in are no longer omitted.

## 2. Research motivation

The serving question is not simply whether CXL or RDMA can copy bytes. It is:

> Can a KV cache that is not resident in GPU HBM be found and materialized into the
> consumer GPU faster and more efficiently than recomputing the cached prefix, while
> preserving capacity, correctness, and tail latency under concurrency?

CXL and RDMA address different resource limits:

- CXL expands rack-local capacity and permits coherent local sharing.
- RDMA reaches KV held by another memory owner or rack.
- Host DRAM is the safe v1 rendezvous between a NIC and a GPU when GDR is unavailable.
- A mixed CXL+RDMA batch can use independent links for different KV objects. The current
  implementation is per-object path diversity; it does not stripe one block across all
  links.

For a model without layout padding, approximate KV bytes for a cached prefix as:

```text
KV_bytes = cached_tokens × 2 × layers × kv_heads × head_dim × element_bytes
```

The measured connector byte count is authoritative because model layouts, block
padding, quantization, and parallelism can change this estimate.

Remote reuse helps latency only when:

```text
metadata_lookup + allocation/publication + data_fetch + GPU_materialization
    < prefill_compute_saved
```

This inequality, evaluated at p50 and p99 under realistic concurrency, is the main
serving result. Raw GB/s explains the result but is not the result by itself.

## 3. Paths that must be measured separately

| ID | Path | Purpose |
|---|---|---|
| P0 | pinned local DRAM → GPU | Host-to-device copy-engine/PCIe ceiling |
| P1 | pinned CXL devdax → GPU | Physical CXL-to-GPU ceiling |
| P2 | remote DRAM → RDMA → registered local DRAM | NIC/fabric and Transfer Engine ceiling |
| P3 | remote DRAM → RDMA → host staging → GPU | Mandatory v1 remote-GPU path |
| P4 | local CXL + remote RDMA/staging → one GPU | Current multipath per-object concurrency |
| P5 | remote memory → GPUDirect RDMA → GPU | Optional candidate; report failure if unsupported, never infer fallback |
| P6 | remote CXL → source bounce → RDMA → destination bounce/CXL or GPU | Final rack-to-rack Callosum path |

Do not combine these into one number. Each removes or adds a stage and answers a
different bottleneck question.

## 4. The host-DRAM/GPU/NIC experiment

The most useful near-term systems experiment is the consumer-side PCIe/NUMA contention
and overlap test:

```text
remote NIC ---> registered host bounce ring ---> GPU
                         ^                         ^
                  RDMA writes/fills         CUDA H2D reads
```

Run these five cases with the same bytes, NUMA node, GPU, NIC, MTU, and process pinning:

1. H2D alone from pinned local DRAM.
2. RDMA receive/read alone into registered local DRAM.
3. H2D and RDMA concurrently on independent buffers to expose shared root-complex and
   memory-controller contention.
4. Correct double-buffer/ring pipeline: NIC fills slot N+1 while the GPU copies completed
   slot N. Never let NIC and GPU access the same slot without completion ownership.
5. GDR candidate, only when GPU memory registration is actually supported.

For an object of size `S`, a strictly serial staged path is bounded by:

```text
B_serial = S / (S / B_RDMA + S / B_H2D)
```

An ideal steady-state double-buffer pipeline approaches:

```text
B_pipeline <= min(B_RDMA, B_H2D)
```

Report overlap efficiency as:

```text
overlap_efficiency = B_pipeline / min(B_RDMA, B_H2D)
```

This directly motivates bounce-buffer pipelining: if the measured Store path is near
`B_serial` and the independent concurrent test retains both isolated rates, software
serialization—not the hardware links—is the next optimization target.

## 5. Controlled matrix

Use a compact correctness matrix before a long sweep:

| Factor | Correctness gate | Performance sweep |
|---|---|---|
| Object bytes | 4 KiB, 64 KiB, 1 MiB, 16 MiB - 4 KiB | 64 KiB, 1 MiB, 4 MiB, 16 MiB - 4 KiB |
| Batch objects | 1, 4 | 1, 4, 8, 16, 32 |
| Consumers | 1 | 1, 2, 4, 8 |
| Bounce slots | 1 | 1, 2, 4, 8 |
| Warmup | 1 | at least 3, excluded from timing |
| Timed duration | one exact transfer | 10–30 seconds, at least 5 repetitions |
| WSS | deterministic unique objects | greater than LLC; disjoint per consumer |
| Placement | one named source per key | identical keys/placement across compared cases |

The CXL object limit is currently `kMaxSliceSize`, so an exact 16 MiB Store object is
not an apples-to-apples row. RDMA can use larger multi-slice objects, but compare the
common size range first.

Record with every datapoint:

- commit, build flags, Python/CUDA/driver versions;
- GPU/NIC/CXL device identities and `nvidia-smi topo -m`;
- CPU and memory NUMA binding (`numactl -H`, process affinity);
- RDMA device, GID, link state, RoCE netdev, MTU, and NIC counter deltas;
- CXL path, pool ID, mapped/owned ranges, and backend provider;
- object bytes, WSS, batch, concurrency, warmup, checksum mode;
- elapsed time, decimal GB/s, p50/p95/p99 latency, CPU use, and failures;
- internal RDMA-to-staging, staging-to-GPU, and unattributed time;
- GPU copy-engine utilization and available platform CXL/uncore counters.

If a counter is unavailable, record `unavailable`; do not replace it with an inferred
claim.

## 6. Staged TODO and exit gates

### R0 — preserve the current T0 gate

```bash
cd Mooncake-dev
bash scripts/bootstrap_todo1_lab.sh
```

Exit: the T0 suite passes and now includes `multipath_placement_test`. This is a software
regression gate only.

### R1 — native one-node Store correctness

Follow `docs/CXL_TODO15_ONE_NODE_RUNBOOK.md` Part 2:

```bash
bash scripts/run_todo15_one_node_cxl.sh unit
bash scripts/run_todo15_one_node_cxl.sh preflight
bash scripts/run_todo15_one_node_cxl.sh run
```

Exit: the hardware run uses `MC_CXL_PROVIDER=mooncake`, opens `/dev/dax`, completes eight
PUT/GET/checksum validations, removes all keys, and writes a PASS summary. This closes
native correctness, not bandwidth.

### R2 — raw DRAM and CXL to GPU ceilings

Build the existing raw tool on the CUDA host:

```bash
nvcc -O3 -o /tmp/cxl_gpu_bw scripts/cxl_gpu_bw.cu -lpthread
```

Run pinned DRAM and devdax with identical mapped size, block size, GPU, duration, and
threads. Sweep threads `1 2 4 8` and include `--pin 0` only as a pageable-copy control:

```bash
/tmp/cxl_gpu_bw --src dram --dev-size 34359738368 \
  --block 16773120 --threads 4 --seconds 10 --gpu 0 --pin 1

/tmp/cxl_gpu_bw --src devdax --dev /dev/daxX.Y --dev-size 34359738368 \
  --block 16773120 --threads 4 --seconds 10 --gpu 0 --pin 1
```

Exit: byte-correct setup, `pinned=true`, five stable repetitions, and separate P0/P1
ceilings. A pageable result is not physical CXL bandwidth.

### R3 — Store-level CXL to GPU scaling

The existing `scripts/run_cxl_gpu_scaling.sh` measures one Master and 1/2/4/8 Store
consumers with disjoint working sets. Current source forces
`MC_CXL_PROVIDER=faketract`; therefore it measures a real devdax CXL payload path but not
native Mooncake partition ownership. Before making the production-path claim, add and
validate native writer/reader partition assignment and report the selected provider in
every JSON result.

Exit: aggregate throughput is computed as total bytes over one shared wall interval,
not by summing rates with different intervals; each client uses disjoint keys; result
includes provider and owned range.

### R4 — distributed RDMA staged baseline

Use `docs/TODOEXTRA_RDMA_SCALING_PAPER_BASELINE.md` and
`scripts/run_dram_rdma_distributed.sh`, not the loopback smoke script. The existing
profiled APIs already expose:

```text
transfer_to_staging_ns
staging_to_gpu_ns
```

Exit: at least two physical hosts, expected distinct source endpoints,
`source_protocols=["rdma"]`, NIC counter delta, exact byte counts, and
`gpu_path_selected="rdma_host_staged"`.

### R5 — mixed CXL plus RDMA into one GPU

Follow `../../docs/MULTIPATH_TIER1_RUNBOOK.md` Step 3 and run all four cases in
`scripts/gpu_multipath_store_test.py`.

Exit: one `batch_get_into` returns one CXL object and one RDMA object into two CUDA
tensors; both checksums pass; logs show both:

```text
protocol=cxl  path=cxl_cuda_copy
protocol=rdma path=rdma_host_staged
```

NIC counters must increase for the RDMA object. This is path-diversity correctness, not
yet a speedup claim.

### R6 — mixed-path throughput and overlap

Add a timed mixed-path harness using fixed placement and disjoint GPU buffers. Compare:

- CXL-only;
- staged-RDMA-only;
- sequential CXL then RDMA;
- one mixed batch;
- double-buffered staged RDMA plus CXL.

Report:

```text
mixed_efficiency = B_mixed / (B_CXL_isolated + B_RDMA_staged_isolated)
mixed_speedup = B_mixed / max(B_CXL_isolated, B_RDMA_staged_isolated)
```

Exit: repeated checksums, per-path byte counters, a common timing interval, p99, and a
measured speedup with confidence intervals. If no speedup appears, stage timings and
topology counters must identify whether the bottleneck is PCIe, host memory, GPU copy
engines, NIC, or software serialization.

### R7 — LLM-serving baseline

Only after R1–R6, use `MooncakeStoreConnector` with a real model and compare:

1. no external KV reuse (recompute baseline);
2. local DRAM Store hit;
3. local CXL hit;
4. remote staged-RDMA hit;
5. mixed CXL+RDMA hit;
6. GDR candidate, if independently validated.

Sweep cached prefix length, hit ratio, request concurrency, batch size, and P/D roles.
Report:

- p50/p95/p99 time to first token;
- inter-token latency and request/token throughput;
- scheduler queue and connector lookup time;
- bytes requested, fetched, and materialized into GPU;
- prefix-hit and useful-token ratios;
- transfer/GPU-copy time and recompute time avoided;
- GPU SM/copy-engine utilization, CPU, memory, and NIC utilization;
- failure/fallback rate and cache publication/reuse behavior.

Exit: remote/local reuse lowers TTFT or raises sustainable request throughput versus
recompute at the same correctness and SLO. A microbenchmark GB/s improvement without a
serving improvement is an engineering diagnostic, not the project result.

### R8 — rack-to-rack and multi-link scaling

After two-node native shared-CXL passes, measure the full v1 bounce path and then the
TODO 8 scheduler:

```text
source CXL -> source bounce -> RDMA -> destination bounce -> CXL/GPU
```

Scale source nodes and NICs only after a one-NIC result is attributed. Per-object hash
placement must not be described as striping or saturation-aware scheduling. The final
exit gate is aggregate delivered bandwidth above the best single link while a throttled
link causes measurable load shifting without corruption.

## 7. Immediate priority order

1. Rerun bootstrap and confirm the newly labelled placement test appears.
2. Run native one-node `/dev/dax` correctness.
3. Measure pinned DRAM→GPU and pinned CXL→GPU raw ceilings.
4. Run distributed RDMA→host and RDMA→host→GPU, capturing the existing phase timings.
5. Complete the mixed CXL+RDMA correctness gate with NIC counters.
6. Implement the bounce-ring overlap benchmark before changing the production path.
7. Run vLLM only after every underlying path has a trustworthy ceiling and provenance.
