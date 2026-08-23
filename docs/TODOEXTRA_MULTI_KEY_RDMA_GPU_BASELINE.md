# TODO Extra: multi-key RDMA-to-GPU baselines

This gate compares the same set of objects and GPU destinations using two API
patterns:

1. sequential single GET: `get_into(key_i, gpu_ptr_i, size)` once per key;
2. true batch GET: `batch_get_into(keys, gpu_ptrs, sizes)` once for all keys.

Prep deliberately creates each object with a separate `put_from`. Mooncake
places one replica on one source segment, so distribution is measured across
keys, not by expecting one object to be striped across four segments.

For four 8 GiB objects, provide at least four 8 GiB source segments, 32 GiB of
free GPU memory, and roughly 33 GiB of host staging capacity. Keep Master and
all sources alive through prep and consumer.

## Master on m3

```bash
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_METADATA_PORT=18080 TODOEXTRA_METRICS_PORT=19004 \
  TODOEXTRA_LEASE_TTL=24h MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh master
```

## Four logical source servers on m4

```bash
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.44 \
  TODOEXTRA_SOURCE_COUNT=4 TODOEXTRA_SOURCE_BASE_PORT=50200 \
  TODOEXTRA_SEGMENT_GIB=8 TODOEXTRA_OBJECT_COUNT=4 \
  TODOEXTRA_BLOCK_GIB=8 TODOEXTRA_KEY=todoextra-rdma-4x8g \
  TODOEXTRA_RDMA_MTU=1024 TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma \
  RDMA_DEVICE_NAME=mlx5_0 MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh source
```

To use four physical source nodes instead, run the source role once per node
with `TODOEXTRA_SOURCE_COUNT=1`, a unique `TODOEXTRA_LOCAL_IP`, and a
peer-reachable port. Do not include the GPU consumer as a source when the goal
is a pure remote-RDMA baseline; a local replica would contaminate the result.

## Four separate single PUTs from m3

```bash
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.43 TODOEXTRA_EXPECT_SOURCES=4 \
  TODOEXTRA_OBJECT_COUNT=4 TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma RDMA_DEVICE_NAME=mlx5_0 \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh prep
```

The generated keys are `todoextra-rdma-4x8g-0000` through `-0003`. Prep passes
only if their aggregate descriptors cover four distinct RDMA endpoints.

## Sequential-single and batch GPU GET on m3

Rebuild and restage first because the single-call profile API is new:

```bash
cmake --build build-gpu-multipath -j8 --target store
cp build-gpu-multipath/mooncake-integration/store.*.so mooncake-wheel/mooncake/store.so
```

Then run:

```bash
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.43 TODOEXTRA_EXPECT_SOURCES=4 \
  TODOEXTRA_OBJECT_COUNT=4 TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g TODOEXTRA_ITERATIONS=3 \
  TODOEXTRA_RDMA_MTU=1024 TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma \
  RDMA_DEVICE_NAME=mlx5_0 MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh consumer
```

Outputs:

- `dram.json`: sequential single GET into registered host DRAM;
- `gpu-single-staged.json`: four true `get_into` calls per iteration;
- `gpu-batch-staged.json`: one four-key `batch_get_into` per iteration;
- each GPU summary includes RDMA-to-host, host-to-GPU, end-to-end time,
  throughput, API-call count, and objects per call.

With the default `MC_STORE_RDMA_GPU_DIRECT=0`, “to GPU” means an actual GPU
destination using `RDMA -> registered host staging -> cuda copy -> GPU`. GDR is
an explicit candidate mode and has no host/GPU two-phase breakdown.
