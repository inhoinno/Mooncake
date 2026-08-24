# TODO Extra: four-source RDMA scaling baseline

## Paper question

Measure whether grouping the same objects by source into Mooncake batch GET
calls improves application-visible `RDMA -> host DRAM -> GPU` throughput.

| Per-source WSS | Sources | Objects/source | Total WSS | Single calls/iteration | Batch calls/iteration |
|---:|---:|---:|---:|---:|---:|
| 8 GiB | 4 | 8 | 32 GiB | 32 | 4 (8 objects/call) |
| 16 GiB | 4 | 16 | 64 GiB | 64 | 4 (16 objects/call) |
| 32 GiB | 4 | 32 | 128 GiB | 128 | 4 (32 objects/call) |

Each measured case runs three iterations. The single and batch cases use the
same keys, replica locations, GPU buffers, object size, MTU, Master, and source
processes. `TODOEXTRA_WARMUP=1` performs one unmeasured warmup before timing.

Run every WSS at these object sizes:

```text
16 MiB, 32 MiB, 64 MiB, 128 MiB, 512 MiB, 1024 MiB
```

The resulting object and API-call matrix is:

| Per-source WSS | Block | Objects/source | Total objects | Single calls/iteration | Batch calls/iteration |
|---:|---:|---:|---:|---:|---:|
| 8 GiB | 16 MiB | 512 | 2,048 | 2,048 | 4 |
| 8 GiB | 32 MiB | 256 | 1,024 | 1,024 | 4 |
| 8 GiB | 64 MiB | 128 | 512 | 512 | 4 |
| 8 GiB | 128 MiB | 64 | 256 | 256 | 4 |
| 8 GiB | 512 MiB | 16 | 64 | 64 | 4 |
| 8 GiB | 1024 MiB | 8 | 32 | 32 | 4 |
| 16 GiB | 16 MiB | 1,024 | 4,096 | 4,096 | 4 |
| 16 GiB | 32 MiB | 512 | 2,048 | 2,048 | 4 |
| 16 GiB | 64 MiB | 256 | 1,024 | 1,024 | 4 |
| 16 GiB | 128 MiB | 128 | 512 | 512 | 4 |
| 16 GiB | 512 MiB | 32 | 128 | 128 | 4 |
| 16 GiB | 1024 MiB | 16 | 64 | 64 | 4 |
| 32 GiB | 16 MiB | 2,048 | 8,192 | 8,192 | 4 |
| 32 GiB | 32 MiB | 1,024 | 4,096 | 4,096 | 4 |
| 32 GiB | 64 MiB | 512 | 2,048 | 2,048 | 4 |
| 32 GiB | 128 MiB | 256 | 1,024 | 1,024 | 4 |
| 32 GiB | 512 MiB | 64 | 256 | 256 | 4 |
| 32 GiB | 1024 MiB | 32 | 128 | 128 | 4 |

## Topology and validity limitation

```text
                         Master on m3
                    192.168.5.43:50051
                              |
      m1 source       m2 source       m3 source       m4 source
   192.168.5.41    192.168.5.42    192.168.5.43    192.168.5.44
       :50200          :50200          :50200          :50200
          \               |               |              /
                           m3 consumer GPU
```

The requested topology includes m3 as both source and consumer. Consequently,
one quarter of the logical sources is co-located with the consumer. The result
is valid for Mooncake four-source scheduling efficiency, but it is **not** a
four-remote-NIC result. A paper claim about four remote RDMA servers requires a
fifth consumer host, or moving the consumer off m1-m4.

Every result must report:

```text
source_segment_count = 4
source_protocols      = ["rdma"]
gpu_path_selected     = "rdma_host_staged"
```

The Master's `rpc protocol=tcp` log is control-plane RPC and does not invalidate
the data-path result.

## Step 0 — build and network checks on all four hosts

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
cmake --build build-gpu-multipath -j8 --target store
cp build-gpu-multipath/mooncake-integration/store.*.so mooncake-wheel/mooncake/store.so
ibdev2netdev
ip -br addr
```

Verify that `mlx5_0` is the HCA associated with `192.168.5.x`. Keep
`MC_MTU=1024` unless the RoCE netdev MTU and verbs path MTU have both been
validated at a larger value.

## Step 1 — Master on m3

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_METADATA_PORT=18080 \
  TODOEXTRA_METRICS_PORT=19004 \
  TODOEXTRA_LEASE_TTL=24h \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-scaling-master \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh master
```

Leave this terminal running.

## Step 2 — one source process on each m1-m4

Use a 40 GiB segment so the same source session has headroom for the largest
32 GiB dataset. Run the matching command on each host and leave it running.

### m1

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.41 TODOEXTRA_SOURCE_COUNT=1 \
  TODOEXTRA_SOURCE_BASE_PORT=50200 TODOEXTRA_SEGMENT_GIB=40 \
  TODOEXTRA_RDMA_MTU=1024 TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-source \
  RDMA_DEVICE_NAME=mlx5_0 MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh source
```

### m2

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.42 TODOEXTRA_SOURCE_COUNT=1 \
  TODOEXTRA_SOURCE_BASE_PORT=50200 TODOEXTRA_SEGMENT_GIB=40 \
  TODOEXTRA_RDMA_MTU=1024 TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-source \
  RDMA_DEVICE_NAME=mlx5_0 MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh source
```

### m3

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.43 TODOEXTRA_SOURCE_COUNT=1 \
  TODOEXTRA_SOURCE_BASE_PORT=50200 TODOEXTRA_SEGMENT_GIB=40 \
  TODOEXTRA_RDMA_MTU=1024 TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-source \
  RDMA_DEVICE_NAME=mlx5_0 MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh source
```

### m4

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.44 TODOEXTRA_SOURCE_COUNT=1 \
  TODOEXTRA_SOURCE_BASE_PORT=50200 TODOEXTRA_SEGMENT_GIB=40 \
  TODOEXTRA_RDMA_MTU=1024 TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-source \
  RDMA_DEVICE_NAME=mlx5_0 MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh source
```

All four terminals must print `1 RDMA source clients READY` before prep.

## Step 3 — prep and consume one scale on m3

Set `PER_SOURCE_GIB` to `8`, `16`, or `32`, and `BLOCK_MIB` to one of
`16 32 64 128 512 1024`. The shell computes the object count and batch width.
Four unique endpoints are expanded into contiguous per-source key groups, so
each batch call addresses exactly one source.

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
PER_SOURCE_GIB=8
BLOCK_MIB=16
OBJECTS_PER_SOURCE=$((PER_SOURCE_GIB * 1024 / BLOCK_MIB))
TOTAL_OBJECTS=$((4 * OBJECTS_PER_SOURCE))
RUN_ID="rdma-4src-${PER_SOURCE_GIB}gib-${BLOCK_MIB}mib"
COMMON_ENV=(
  PYTHON_BIN=/home/labuser/venv/bin/python3
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata
  TODOEXTRA_LOCAL_IP=192.168.5.43
  TODOEXTRA_EXPECT_SOURCES=4
  TODOEXTRA_SOURCE_ENDPOINTS=192.168.5.41:50200,192.168.5.42:50200,192.168.5.43:50200,192.168.5.44:50200
  TODOEXTRA_OBJECTS_PER_SOURCE="$OBJECTS_PER_SOURCE"
  TODOEXTRA_OBJECT_COUNT="$TOTAL_OBJECTS"
  TODOEXTRA_BLOCK_MIB="$BLOCK_MIB"
  TODOEXTRA_BATCH_GROUP_SIZE="$OBJECTS_PER_SOURCE"
  TODOEXTRA_KEY="$RUN_ID"
  TODOEXTRA_ITERATIONS=3
  TODOEXTRA_WARMUP=1
  TODOEXTRA_GPU_ID=0
  TODOEXTRA_RDMA_MTU=1024
  TODOEXTRA_CLEANUP_AFTER=1
  TODOEXTRA_OUT_DIR="/tmp/$RUN_ID"
  RDMA_DEVICE_NAME=mlx5_0
  MOONCAKE_BUILD_DIR=build-gpu-multipath
)

sudo -E env "${COMMON_ENV[@]}" bash scripts/run_dram_rdma_distributed.sh prep
sudo -E env "${COMMON_ENV[@]}" bash scripts/run_dram_rdma_distributed.sh consumer
```

Repeat for every `(PER_SOURCE_GIB, BLOCK_MIB)` pair. `CLEANUP_AFTER=1` removes
the dataset only after all result JSON files have been written, preventing WSS
from accumulating in the 40 GiB source segments. If a consumer fails before
cleanup, run the same environment with the `cleanup` role before continuing:

```bash
sudo -E env "${COMMON_ENV[@]}" bash scripts/run_dram_rdma_distributed.sh cleanup
```

For paper-quality isolation, restart Master and sources for every datapoint.
This is particularly important for the 16 MiB cases, which create as many as
8,192 objects and can leave substantial finished-task and metadata history even
after object removal.

For the 128 GiB case, verify m3 has at least 128 GiB free GPU memory plus
framework headroom and approximately 129 GiB available registered host memory.

## Exact API counts

For the 8 GiB/source, 1 GiB/object case and three measured iterations:

```text
single: 32 get_into_profiled calls/iteration × 3 = 96 measured API calls
batch:   4 batch_get_into_profiled calls/iteration × 3 = 12 measured API calls
         each batch call contains 8 keys from one source
```

At 16 and 32 GiB/source, single calls grow to 192 and 384 total. Batch remains
12 measured calls; only objects per batch grow to 16 and 32.

The JSON fields `api_calls`, `api_calls_per_iteration`, `objects_per_call`, and
`batch_group_count` are the authoritative call-count evidence. C++ emits one
`gpu_read_path` trace per object inside a batch, so trace-line count is not API
call count.

## Results and paper metrics

Each scale writes:

```text
/tmp/rdma-4src-8gib/gpu-single-staged.json
/tmp/rdma-4src-8gib/gpu-batch-staged.json
```

Compare:

- end-to-end `GBps` and `sec`;
- `staged_breakdown.rdma_to_host`;
- `staged_breakdown.host_to_gpu`;
- `unattributed_sec`;
- batch speedup = `batch.GBps / single.GBps`;
- efficiency = measured aggregate GB/s divided by the four-source raw RDMA
  ceiling measured separately with `ib_write_bw` under the same MTU and object
  concurrency.

Do not call this GPUDirect RDMA. This gate intentionally measures:

```text
remote/source DRAM -> RDMA -> registered m3 host staging -> CUDA copy -> GPU
```
