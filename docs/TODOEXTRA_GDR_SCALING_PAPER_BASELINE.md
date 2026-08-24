# TODO Extra: GPUDirect RDMA scaling paper baseline

This run uses the same Master-side population and GET-distribution observer as
the staged baseline. See `docs/MASTER_REQUEST_DISTRIBUTION_MONITOR.md`; only the
client path proof changes from `rdma_host_staged` to `rdma_gpu_direct`.

## Objective

Repeat the staged experiment from
`TODOEXTRA_RDMA_SCALING_PAPER_BASELINE.md`, but transfer each remote DRAM object
directly into its CUDA destination through GPUDirect RDMA:

```text
source DRAM -> source NIC -> RoCE/RDMA -> m3 NIC -> m3 GPU memory
```

There is no registered host bounce buffer and no host-to-GPU CUDA copy in this
experiment. A result is accepted only when:

```text
source_protocols  = ["rdma"]
gpu_path_selected = "rdma_gpu_direct"
trace              = path=rdma_gpu_direct
no trace contains    path=rdma_host_staged
```

The workload matrix is identical to the staged baseline:

- four source clients: m1, m2, m3, m4;
- 8, 16, and 32 GiB WSS per source;
- 16, 32, 64, 128, 512, and 1024 MiB objects;
- three measured iterations plus one unmeasured warmup;
- single: one `get_into_profiled()` per object;
- batch: one `batch_get_into_profiled()` per source per iteration.

## Interpretation limitation

m3 is both one source and the GPU consumer, so one of four logical source
segments is co-located. This is a valid Mooncake four-source scheduling test,
but not a four-remote-NIC GDR result. A strict four-remote-source paper result
requires a fifth GPU consumer host.

## Separate build and Python environment

This procedure intentionally uses:

```text
Python:        /home/labuser/venv-gdr
CMake build:   build-gdr-scaling
Staged package: envs/todoextra-gdr/mooncake-wheel
Results:       /tmp/gdr-4src-...
```

It does not reuse or overwrite the staged baseline at runtime.

## 1. Install native dependencies on m1-m4

Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  build-essential ca-certificates cmake curl git ninja-build patchelf \
  pkg-config python3 python3-dev python3-pip python3-venv unzip wget \
  libasio-dev libboost-all-dev libcurl4-openssl-dev libgflags-dev \
  libgoogle-glog-dev libgrpc++-dev libgrpc-dev libhiredis-dev \
  libibverbs-dev libjemalloc-dev libjsoncpp-dev libmsgpack-dev \
  libnuma-dev libprotobuf-dev libssl-dev libunwind-dev liburing-dev \
  libxxhash-dev libyaml-cpp-dev libzmq3-dev libzstd-dev \
  protobuf-compiler-grpc perftest rdma-core
```

Initialize pinned dependencies on every host:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
git submodule sync --recursive
git submodule update --init --recursive extern/pybind11 extern/yalantinglibs
test -f extern/pybind11/CMakeLists.txt
test -f extern/yalantinglibs/cmake/build.cmake
```

## 2. Install and verify CUDA on m3

Install the NVIDIA driver and CUDA toolkit appropriate to the H200 host. Then:

```bash
nvidia-smi
nvcc --version
nvidia-smi topo -m
nvidia-smi -q -d MEMORY | grep -A4 -i 'BAR1'
```

The GPU and `mlx5_0` should have a viable PCIe path. Record `nvidia-smi topo -m`
with the benchmark artifacts; topology materially affects GDR throughput.

Create the isolated Python environment on every host:

```bash
python3 -m venv /home/labuser/venv-gdr
/home/labuser/venv-gdr/bin/python3 -m pip install --upgrade \
  pip build setuptools wheel auditwheel 'patchelf>=0.17' numpy aiohttp
```

Install a CUDA-compatible PyTorch wheel into `venv-gdr` on m3, using the
PyTorch CUDA package index matching the installed platform. Verify:

```bash
/home/labuser/venv-gdr/bin/python3 - <<'PY'
import torch
print("torch", torch.__version__)
print("torch CUDA", torch.version.cuda)
print("device", torch.cuda.get_device_name(0))
assert torch.cuda.is_available()
x = torch.empty(16 * 1024 * 1024, dtype=torch.uint8, device="cuda")
print("CUDA pointer", hex(x.data_ptr()), "bytes", x.numel())
PY
```

## 3. Select and verify the GPU-memory registration mechanism

Mooncake supports two NVIDIA registration paths. Select exactly one on m3.

### Option A — nvidia-peermem (recommended first)

Install the peer-memory module supplied by the compatible NVIDIA/MLNX_OFED
stack, then:

```bash
sudo modprobe nvidia-peermem
lsmod | grep '^nvidia_peermem'
```

Use this environment in the experiment:

```text
TODOEXTRA_GDR_REGISTRATION=peermem
```

Mooncake translates it to `WITH_NVIDIA_PEERMEM=1` and registers CUDA memory
with the verbs MR path.

### Option B — CUDA DMA-BUF

Use this only when the driver, CUDA allocation, kernel, and mlx5 stack support
CUDA DMA-BUF export plus `ibv_reg_dmabuf_mr`:

```text
TODOEXTRA_GDR_REGISTRATION=dmabuf
```

Mooncake translates it to `WITH_NVIDIA_PEERMEM=0`. Do not select DMA-BUF merely
because `nvidia-peermem` is absent; prove it with the hardware test below.

## 4. RDMA and GDR hardware preflight

Run on every host:

```bash
ibv_devices
ibv_devinfo -d mlx5_0
ibdev2netdev
ip -br addr
```

Confirm `mlx5_0` maps to `192.168.5.x` and the same RoCE VLAN/fabric. Keep the
Mooncake baseline MTU at 1024 unless the netdev and verbs path MTUs are both
validated at a larger value.

First prove ordinary host-memory RDMA with the lab's known-good `ib_write_bw`
procedure. Then prove the direction used by Mooncake GET: remote host memory is
read directly into m3 GPU memory.

Example m1 -> m3 GDR-read check:

On m1 (host-memory source):

```bash
ib_read_bw -d mlx5_0 -F --report_gbits -p 18515
```

On m3 (GPU destination/client):

```bash
ib_read_bw -d mlx5_0 -F --report_gbits -p 18515 \
  --use_cuda=0 192.168.5.41
```

Repeat against `192.168.5.42` and `192.168.5.44`, using different ports if run
concurrently. Confirm the local `perftest --help` spelling of `--use_cuda`; old
perftest packages may not include CUDA support. Failure here blocks Mooncake
GDR testing—it is not a Mooncake Master problem.

## 5. Configure, build, test, and isolate the GDR package

Run on m1-m4:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
PY=/home/labuser/venv-gdr/bin/python3
PYVER="$($PY -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

MOONCAKE_USE_CUDA=ON \
MOONCAKE_BUILD_DIR=build-gdr-scaling \
MOONCAKE_BUILD_JOBS=8 \
MOONCAKE_CMAKE_ARGS="-DPython3_EXECUTABLE=$PY" \
PYTHON_BIN="$PY" PYTHON_VERSION="$PYVER" \
PATH="$(dirname "$PY"):$PATH" \
  bash scripts/build_todo1_overlay.sh

mkdir -p envs/todoextra-gdr
cp -a mooncake-wheel envs/todoextra-gdr/
```

Verify on every host:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
test -f build-gdr-scaling/CMakeCache.txt
test -f envs/todoextra-gdr/mooncake-wheel/mooncake/store.so
test -x envs/todoextra-gdr/mooncake-wheel/mooncake/mooncake_master
grep -E '^(USE_CUDA|WITH_STORE):BOOL=' build-gdr-scaling/CMakeCache.txt
```

Verify the profiled APIs on m3:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
PYTHONPATH="$PWD/envs/todoextra-gdr/mooncake-wheel" \
/home/labuser/venv-gdr/bin/python3 - <<'PY'
from mooncake import store
s = store.MooncakeDistributedStore()
print(store.__file__)
assert hasattr(s, "get_into_profiled")
assert hasattr(s, "batch_get_into_profiled")
PY
```

## 6. Start the Master on m3

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
sudo -E env \
  PYTHON_BIN=/home/labuser/venv-gdr/bin/python3 \
  MOONCAKE_PACKAGE_ROOT="$PWD/envs/todoextra-gdr/mooncake-wheel" \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_METADATA_PORT=18080 TODOEXTRA_METRICS_PORT=19004 \
  TODOEXTRA_LEASE_TTL=24h \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-gdr-master \
  MOONCAKE_BUILD_DIR=build-gdr-scaling \
  bash scripts/run_dram_rdma_distributed.sh master
```

Leave it running. `rpc protocol=tcp` is the metadata/control plane.

## 7. Start one 40 GiB RDMA source on m1-m4

Run this on each source host after setting `HOST_IP` exactly as shown:

```text
m1: HOST_IP=192.168.5.41
m2: HOST_IP=192.168.5.42
m3: HOST_IP=192.168.5.43
m4: HOST_IP=192.168.5.44
```

Command on each host:

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
HOST_IP=192.168.5.41  # change to this host's address
sudo -E env \
  PYTHON_BIN=/home/labuser/venv-gdr/bin/python3 \
  MOONCAKE_PACKAGE_ROOT="$PWD/envs/todoextra-gdr/mooncake-wheel" \
  TODOEXTRA_MASTER_ADDRESS=192.168.5.43:50051 \
  TODOEXTRA_METADATA_URL=http://192.168.5.43:18080/metadata \
  TODOEXTRA_LOCAL_IP="$HOST_IP" \
  TODOEXTRA_SOURCE_COUNT=1 TODOEXTRA_SOURCE_BASE_PORT=50200 \
  TODOEXTRA_SEGMENT_GIB=40 TODOEXTRA_RDMA_MTU=1024 \
  TODOEXTRA_OUT_DIR=/tmp/todoextra-gdr-source \
  RDMA_DEVICE_NAME=mlx5_0 MOONCAKE_BUILD_DIR=build-gdr-scaling \
  bash scripts/run_dram_rdma_distributed.sh source
```

All four processes must print `RDMA source clients READY`.

## 8. Prep and run one GDR datapoint on m3

Example: 8 GiB/source with 16 MiB objects. Change `PER_SOURCE_GIB` and
`BLOCK_MIB` for the other matrix points.

```bash
cd /home/labuser/inho/Multipath/Mooncake-dev
PER_SOURCE_GIB=8
BLOCK_MIB=16
OBJECTS_PER_SOURCE=$((PER_SOURCE_GIB * 1024 / BLOCK_MIB))
TOTAL_OBJECTS=$((4 * OBJECTS_PER_SOURCE))
RUN_ID="gdr-4src-${PER_SOURCE_GIB}gib-${BLOCK_MIB}mib"

COMMON_ENV=(
  PYTHON_BIN=/home/labuser/venv-gdr/bin/python3
  MOONCAKE_PACKAGE_ROOT="$PWD/envs/todoextra-gdr/mooncake-wheel"
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
  TODOEXTRA_DATA_PATH=gdr
  TODOEXTRA_GDR_REGISTRATION=peermem
  TODOEXTRA_CLEANUP_AFTER=1
  TODOEXTRA_OUT_DIR="/tmp/$RUN_ID"
  RDMA_DEVICE_NAME=mlx5_0
  MOONCAKE_BUILD_DIR=build-gdr-scaling
)

sudo -E env "${COMMON_ENV[@]}" \
  bash scripts/run_dram_rdma_distributed.sh prep
sudo -E env "${COMMON_ENV[@]}" \
  bash scripts/run_dram_rdma_distributed.sh consumer
```

For DMA-BUF, change only:

```bash
TODOEXTRA_GDR_REGISTRATION=dmabuf
```

Do not compare peermem and DMA-BUF datapoints as though they were the same
environment; record the registration mechanism with every result.

## 9. Run the complete matrix

Use fresh Master/source processes for every paper datapoint when practical:

```text
PER_SOURCE_GIB = 8, 16, 32
BLOCK_MIB      = 16, 32, 64, 128, 512, 1024
```

The 32 GiB/source case allocates 128 GiB of GPU destinations. Confirm sufficient
free H200 memory before running. The 16 MiB/32 GiB case creates 8,192 objects;
restart Master afterward to avoid metadata/task-history bias.

## 10. Required outputs and acceptance

Each result directory contains:

```text
dram.json                       # host-DRAM reference
gpu-single-gdr.json
gpu-single-gdr.log
gpu-batch-gdr.json
gpu-batch-gdr.log
gdr-comparison-summary.json
gdr-comparison-summary.txt
```

Display the compact result:

```bash
cat /tmp/gdr-4src-8gib-16mib/gdr-comparison-summary.txt
```

Regenerate it without rerunning traffic:

```bash
/home/labuser/venv-gdr/bin/python3 \
  scripts/summarize_dram_rdma_results.py --path gdr \
  --out-dir /tmp/gdr-4src-8gib-16mib
```

Acceptance checks:

```bash
grep -q 'path=rdma_gpu_direct' /tmp/gdr-4src-8gib-16mib/gpu-single-gdr.log
grep -q 'path=rdma_gpu_direct' /tmp/gdr-4src-8gib-16mib/gpu-batch-gdr.log
! grep -q 'path=rdma_host_staged' /tmp/gdr-4src-8gib-16mib/gpu-*-gdr.log
grep -E 'gpu_path=rdma_gpu_direct|batch throughput speedup' \
  /tmp/gdr-4src-8gib-16mib/gdr-comparison-summary.txt
```

For GDR, the staged phase breakdown is intentionally absent. The primary
metric is end-to-end bytes divided by elapsed time, because RDMA writes directly
into GPU memory.

## 11. Failure diagnosis

| Signature | Meaning / action |
|---|---|
| `ib_read_bw --use_cuda` fails | Fix driver, peer-memory/DMA-BUF, HCA/GPU topology, or perftest before Mooncake |
| `ibv_reg_mr` fails on CUDA pointer | `nvidia-peermem` path is not functional; verify module/driver/OFED compatibility |
| `Failed to retrieve dmabuf` | CUDA allocation/driver cannot export DMA-BUF; use a supported driver or peermem |
| `ibv_reg_dmabuf_mr` fails | Kernel/rdma-core/mlx5 DMA-BUF import is unsupported or mismatched |
| `path=rdma_host_staged` | Invalid GDR result; environment opt-in was absent or wrong binary was loaded |
| `TRANSFER_FAIL` / completion error | Check GID, MTU, RoCE routing, HCA/GPU P2P reachability, and registration logs |
| `CUDA out of memory` | Reduce WSS, stop other GPU jobs, or use a larger-memory consumer |
| summary refuses result | Preserve logs; transport/path/endpoints did not meet fail-closed acceptance |

Never report a failed GDR run using staged numbers. Run the staged baseline as
a separate experiment and compare its compact summary against the accepted GDR
summary afterward.
