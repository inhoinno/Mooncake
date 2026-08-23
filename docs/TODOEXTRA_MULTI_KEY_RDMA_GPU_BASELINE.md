# TODO Extra: 4 single GETs versus 1 batch GET to GPU

## Exact question

Given the same four remote RDMA objects and the same four GPU destination
buffers, is it faster to issue four separate Mooncake `get_into()` calls or one
Mooncake `batch_get_into()` call?

```text
Baseline A — sequential single GET
  get_into(key0, gpu0, X GiB)
  get_into(key1, gpu1, X GiB)
  get_into(key2, gpu2, X GiB)
  get_into(key3, gpu3, X GiB)
  API calls/iteration: 4; objects/call: 1; bytes/iteration: 4 × X GiB

Baseline B — one batch GET
  batch_get_into([key0,key1,key2,key3],
                 [gpu0,gpu1,gpu2,gpu3],
                 [X GiB,X GiB,X GiB,X GiB])
  API calls/iteration: 1; objects/call: 4; bytes/iteration: 4 × X GiB
```

Everything else is identical: keys, sizes, replica locations, GPU buffers,
warmup, iterations, RDMA device, MTU, Master, and metadata service. Commands
below use `X=8 GiB`, four objects, and three iterations: 32 GiB/iteration and
96 GiB total per baseline.

## Topology and requirements

```text
             m3 Master :50051 + metadata :18080
                              |
       m4:50200   m4:50201   m4:50202   m4:50203
        8 GiB      8 GiB      8 GiB      8 GiB   RDMA segments
             \        |        |        /
                         RDMA
                           |
                    m3 H200 GPU
              four 8 GiB destination buffers
```

This uses four logical Mooncake source servers on m4. Prep performs four
separate single `put_from()` calls and passes only if the four resulting object
descriptors collectively cover four distinct RDMA endpoints. Do not mount an
m3 source: a local replica would invalidate the remote-RDMA comparison.

Required on m3: at least 32 GiB free GPU memory and about 33 GiB host staging
capacity. Keep Master and all sources alive through prep and consumer. Every
command must use metadata port `18080`, not `8080`.

Confirm `mlx5_0` corresponds to the `192.168.5.x` interface on both machines:

```bash
ibdev2netdev
ip -br addr
```

## Step 0 — rebuild and verify the updated client on m3 and m4

Run on both machines:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
cmake --build build-gpu-multipath -j8 --target store
cp build-gpu-multipath/mooncake-integration/store.*.so \
  mooncake-wheel/mooncake/store.so

PYTHONPATH="$PWD/mooncake-wheel" /home/labuser/venv/bin/python3 - <<'PY'
from mooncake import store
s = store.MooncakeDistributedStore()
print("module:", store.__file__)
print("get_into_profiled:", hasattr(s, "get_into_profiled"))
print("batch_get_into_profiled:", hasattr(s, "batch_get_into_profiled"))
assert hasattr(s, "get_into_profiled")
assert hasattr(s, "batch_get_into_profiled")
PY
```

Both profile APIs must print `True`.

## Step 1 — Master on m3

Terminal 1:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_METADATA_PORT=18080 \
  TODOEXTRA_METRICS_PORT=19004 \
  TODOEXTRA_LEASE_TTL=24h \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-4x8g \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh master
```

Leave it running. `rpc protocol=tcp` is the control plane, not the object data
path; replica descriptors below must prove `protocol=rdma`.

## Step 2 — four source servers on m4

Terminal 2:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.44 \
  TODOEXTRA_SOURCE_COUNT=4 \
  TODOEXTRA_SOURCE_BASE_PORT=50200 \
  TODOEXTRA_SEGMENT_GIB=8 \
  TODOEXTRA_OBJECT_COUNT=4 \
  TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g \
  TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-4x8g \
  RDMA_DEVICE_NAME=mlx5_0 \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh source
```

Do not continue until this appears:

```text
[todoextra] 4 RDMA source clients READY on 192.168.5.44
```

## Step 3 — four single PUTs from m3

Terminal 3:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.43 \
  TODOEXTRA_EXPECT_SOURCES=4 \
  TODOEXTRA_OBJECT_COUNT=4 \
  TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g \
  TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-4x8g \
  RDMA_DEVICE_NAME=mlx5_0 \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh prep
```

This creates four objects with four separate calls:

```text
todoextra-rdma-4x8g-0000  8 GiB  ┐
todoextra-rdma-4x8g-0001  8 GiB  ├─ each: put_from(key, ptr, 8 GiB, config)
todoextra-rdma-4x8g-0002  8 GiB  │
todoextra-rdma-4x8g-0003  8 GiB  ┘
```

Required prep result:

```text
status=PASS
object_count=4
source_segment_count=4
source_protocols=["rdma"]
descriptor_bytes=34359738368
```

Stop if fewer than four distinct source endpoints are reported; that placement
cannot answer this experiment's four-source question.


### Trouble shooting from m3 ""PerfError(\"object spans 3 source segments; expected at least 4"
Results for prep in m3
```
E0822 18:30:43.207448 369003 real_client.cpp:6788] Object not found for key: todoextra-rdma-4x8g-0003
{"role": "prep", "status": "FAIL", "error": "PerfError(\"object spans 3 source segments; expected at least 4: {'replica_slices': 3, 'source_endpoints': ['192.168.5.44:50200', '192.168.5.44:50201', '192.168.5.44:50203'], 'source_segment_count': 3, 'source_protocols': ['rdma'], 'descriptor_bytes': 25769803776}\")"}
```

Ran command in m4
```
(venv) labuser@solab-m4:~/inho/Multipath/Mooncake-dev$ sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.44 \
  TODOEXTRA_SOURCE_COUNT=4 \
  TODOEXTRA_SOURCE_BASE_PORT=50200 \
  TODOEXTRA_SEGMENT_GIB=8 \
  TODOEXTRA_OBJECT_COUNT=4 \
  TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g \
  TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-4x8g \
  RDMA_DEVICE_NAME=mlx5_0 \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh source
[sudo] password for labuser:
[preflight] rdma_device=mlx5_0 MC_MTU=1024
[todoextra] 4 RDMA source clients READY on 192.168.5.44; Ctrl-C to stop
```

Prep command in m3
```
sudo -E env \
PYTHON_BIN=/home/labuser/venv/bin/python3 \
TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.43 \
  TODOEXTRA_EXPECT_SOURCES=4 \
  TODOEXTRA_OBJECT_COUNT=4 \
  TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g \
  TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-4x8g \
  RDMA_DEVICE_NAME=mlx5_0 \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh prep
```
prep
```

(venv) labuser@solab-m3:~/inho/Multipath/Mooncake-dev$ cd /home/labuser/inho/Multipath/Mooncake-dev              sudo -E env \                                                                                                      PYTHON_BIN=/home/labuser/venv/bin/python3 \                                                                      TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \                                                                    TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.43 \
  TODOEXTRA_EXPECT_SOURCES=4 \
  TODOEXTRA_OBJECT_COUNT=4 \
  TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g \
  TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-4x8g \
  RDMA_DEVICE_NAME=mlx5_0 \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh prep
[preflight] rdma_device=mlx5_0 MC_MTU=1024

```

## Step 4 — run both GET baselines on m3

Keep Terminals 1 and 2 running. In Terminal 3:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env \
  PYTHON_BIN=/home/labuser/venv/bin/python3 \
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.3.43:18080/metadata \
  TODOEXTRA_LOCAL_IP=192.168.5.43 \
  TODOEXTRA_EXPECT_SOURCES=4 \
  TODOEXTRA_OBJECT_COUNT=4 \
  TODOEXTRA_BLOCK_GIB=8 \
  TODOEXTRA_KEY=todoextra-rdma-4x8g \
  TODOEXTRA_ITERATIONS=3 \
  TODOEXTRA_GPU_ID=0 \
  TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-rdma-4x8g \
  RDMA_DEVICE_NAME=mlx5_0 \
  MOONCAKE_BUILD_DIR=build-gpu-multipath \
  bash scripts/run_dram_rdma_distributed.sh consumer
```

The consumer executes the same keys in this order:

```text
Reference: sequential single get_into() -> registered host DRAM
Baseline A: 4 sequential get_into() calls -> 4 GPU buffers
Baseline B: 1 batch_get_into() call       -> same 4 GPU buffers
```

Results:

```text
/tmp/todoextra-rdma-4x8g/dram.json
/tmp/todoextra-rdma-4x8g/gpu-single-staged.json  # Baseline A
/tmp/todoextra-rdma-4x8g/gpu-batch-staged.json   # Baseline B
```

## Step 5 — print the side-by-side result

Run on m3:

```bash
/home/labuser/venv/bin/python3 - <<'PY'
import json
from pathlib import Path

root = Path("/tmp/todoextra-rdma-4x8g")
single = json.loads((root / "gpu-single-staged.json").read_text())
batch = json.loads((root / "gpu-batch-staged.json").read_text())
s_e2e, b_e2e = single["to_gpu_single_staged"], batch["to_gpu_batch_staged"]
s_split, b_split = single["staged_breakdown"], batch["staged_breakdown"]

def row(name, e2e, split):
    return [name, e2e["api_calls"], e2e["objects_per_call"], e2e["sec"],
            e2e["GBps"], split["rdma_to_host"]["sec"],
            split["rdma_to_host"]["GBps"], split["host_to_gpu"]["sec"],
            split["host_to_gpu"]["GBps"], split["unattributed_sec"]]

headers = ["baseline", "calls", "obj/call", "e2e_s", "e2e_GBps",
           "rdma_s", "rdma_GBps", "gpu_s", "gpu_GBps", "other_s"]
rows = [row("4 x get_into", s_e2e, s_split),
        row("1 x batch_get", b_e2e, b_split)]
widths = [max(len(str(x)) for x in [h] + [r[i] for r in rows])
          for i, h in enumerate(headers)]
print("  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
print("  ".join("-" * w for w in widths))
for r in rows:
    print("  ".join(str(x).ljust(widths[i]) for i, x in enumerate(r)))

print(f"\nbatch throughput speedup: {b_e2e['GBps']/s_e2e['GBps']:.3f}x")
print(f"batch elapsed reduction: {(1-b_e2e['sec']/s_e2e['sec'])*100:.2f}%")
print("endpoints:", batch["source_endpoints"])
print("protocols:", batch["source_protocols"])
assert single["source_endpoints"] == batch["source_endpoints"]
assert single["source_protocols"] == batch["source_protocols"] == ["rdma"]
assert single["source_segment_count"] == batch["source_segment_count"] == 4
PY
```

With three iterations, the expected call counts are:

```text
baseline         calls  obj/call  transferred
---------------  -----  --------  -----------
4 x get_into     12     1         96 GiB
1 x batch_get    3      4         96 GiB
```

## Interpretation and validity

- `e2e_GBps` is the primary application-visible comparison.
- `rdma_GBps` measures remote RDMA into registered host staging.
- `gpu_GBps` measures host-to-GPU CUDA copies.
- `other_s` contains metadata, Python/pybind, scheduling, and final CUDA sync.
- A batch win only in `other_s` means fewer API/software costs, not faster RDMA.
- A batch win in `rdma_GBps` indicates useful concurrent scheduling across
  source endpoints.

Both files must report four identical source endpoints, `protocols=["rdma"]`,
and `gpu_path_selected="rdma_host_staged"`. Otherwise comparison is invalid.

“To GPU” does not mean GPUDirect RDMA here. Until GDR is separately validated,
both baselines intentionally use:

```text
m4 RDMA MR -> m3 registered host staging -> CUDA copy -> m3 GPU
```
