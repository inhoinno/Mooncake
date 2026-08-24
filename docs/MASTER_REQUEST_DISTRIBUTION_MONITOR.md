# Master-side request distribution evidence

## What this measures

Mooncake Master now exposes three complementary per-segment signals at
`http://MASTER:METRICS_PORT/metrics`:

| Metric | Meaning |
|---|---|
| `segment_total_capacity_bytes{segment=...}` | registered source population |
| `segment_allocated_bytes{segment=...}` | current object-byte population |
| `master_get_advertised_replicas_total{segment=...,protocol=...}` | readable replicas returned by single or batch GET metadata lookups |
| `master_get_advertised_bytes_total{segment=...,protocol=...}` | object bytes represented by those advertised replicas |

The last two metrics count every readable replica advertised. In the TODO
Extra setup each object has one replica, so the per-segment delta maps exactly
to the source location of each requested object.

This is not a wire-byte counter. After metadata lookup, the client performs
RDMA, staged RDMA-to-GPU, or GDR directly and the Master does not observe those
payload bytes. Use `gpu-*-staged.log` (`path=rdma_host_staged`) or
`gpu-*-gdr.log` (`path=rdma_gpu_direct`) alongside this artifact to prove the
physical data path.

## Build and deploy

The metric code is in the Master binary, so rebuild and restage it on m3:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
PY=/home/labuser/venv/bin/python3
PYVER="$($PY -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

MOONCAKE_USE_CUDA=ON MOONCAKE_BUILD_DIR=build-gpu-multipath \
MOONCAKE_BUILD_JOBS=8 MOONCAKE_CMAKE_ARGS="-DPython3_EXECUTABLE=$PY" \
PYTHON_BIN="$PY" PYTHON_VERSION="$PYVER" PATH="$(dirname "$PY"):$PATH" \
  bash scripts/build_todo1_overlay.sh
```

For the isolated GDR tree, use the GDR environment and build directory from
`TODOEXTRA_GDR_SCALING_PAPER_BASELINE.md`, then refresh
`envs/todoextra-gdr/mooncake-wheel`.

## Automatic TODO Extra setup

The `master` role starts the observer by default. Add the expected segment
count to the existing staged or GDR Master command:

```bash
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_METADATA_PORT=18080 \
  TODOEXTRA_METRICS_PORT=19004 \
  TODOEXTRA_EXPECT_SOURCES=4 \
  TODOEXTRA_MONITOR_DISTRIBUTION=1 \
  TODOEXTRA_MONITOR_INTERVAL=2 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-master \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh master
```

For GDR, change `PYTHON_BIN`, `MOONCAKE_BUILD_DIR`, and
`MOONCAKE_PACKAGE_ROOT` exactly as documented in the GDR runbook. No monitor
logic changes with the data path.

Artifacts on m3:

```text
/tmp/todoextra-master/master-distribution.jsonl
/tmp/todoextra-master/master-distribution-summary.json
```

The JSONL file is the timeline. The summary file is atomically refreshed with
the newest successful snapshot. Each distribution reports total, active
segments, per-segment value/share, coefficient of variation (CV), and
max/min-active ratio.

## Manual monitor for any LLM or agent workload

The observer is not tied to the benchmark key format. Start it on the Master
node before launching vLLM, an agent workload, or another Mooncake client:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
/home/labuser/venv/bin/python3 scripts/monitor_master_distribution.py \
  --metrics-url http://127.0.0.1:19004/metrics \
  --expected-segments 4 --interval 2 \
  --jsonl /tmp/llm-run/master-distribution.jsonl \
  --summary-json /tmp/llm-run/master-distribution-summary.json
```

Run the workload, then stop the observer with Ctrl-C. Preserve its JSONL,
Master log, workload configuration, and client GPU-path trace in one run
directory.

## Acceptance test

1. Start Master + observer and four source clients.
2. Run the existing multi-key `prep` command.
3. Confirm `expected_segments_ready=true`, four registered segments, and four
   active population segments.
4. Run staged `consumer`, then repeat with GDR in its isolated environment.
5. Confirm GET-advertisement deltas increase on all four segments and
   `protocol=rdma` is the only protocol in raw `/metrics` output.
6. Confirm the matching client trace is staged or GDR as intended.

Quick inspection:

```bash
curl -fsS http://127.0.0.1:19004/metrics | \
  grep -E 'segment_(allocated|total_capacity)_bytes|master_get_advertised'

/home/labuser/venv/bin/python3 -m json.tool \
  /tmp/todoextra-master/master-distribution-summary.json
```

For the deterministic four-source baseline, balanced preparation should show
25% of object bytes on each segment. A balanced read workload should approach
25% of advertised GET bytes per segment and CV near zero. A skewed LLM trace
is not automatically a failure: it is evidence of workload locality. Report
both the placement distribution and GET-advertisement distribution rather
than collapsing them into one number.

## Paper-safe claim

Use this wording:

> Master metadata shows that object population and requested replica
> advertisements span N registered RDMA segments during the workload; client
> path traces separately verify staged RDMA or GPUDirect RDMA payload movement.

Do not claim that the Master measured NIC bytes or that every advertised
replica was physically read. Exact link utilization requires client/NIC
telemetry (for example per-client transfer counters plus `perfquery`/NIC
counters), which remains separate from this metadata evidence.
