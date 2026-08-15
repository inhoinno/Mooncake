# CXL TODO#1 build and validation runbook

This runbook covers the phase-one Mooncake CXL memory backend. It does not
claim rack-to-rack RDMA support and it does not replace TraCT's allocator.

## What owns what in TODO#1

```text
Mooncake Store Master
  -> CxlAllocationStrategy
  -> CachelibBufferAllocator over synthetic offsets       (temporary)

Mooncake Store client
  -> TransferEngine::installTransport("cxl")
  -> CxlTransport
  -> OpenCxlPoolBackendFromEnvironment()
       -> MC_CXL_PROVIDER=faketract -> file or /dev/dax mmap model
       -> MC_CXL_PROVIDER=tract     -> registered confidential adapter
  -> validated offset -> local memcpy to/from mapped CXL memory
```

The first path still decides Mooncake replica offsets. The second path maps and
accesses the bytes. `CxlPoolBackend::metadata_lookup()` and `alloc()` establish
the production TraCT boundary, but Mooncake Store does not call them for
placement yet. Until that later integration is implemented, never point the
FakeTraCT allocator and a live TraCT allocator at the same writable pool.

`CxlTransport` is a rack-local mapped-memory transport. It is not RDMA and does
not make a remote `/dev/dax` mapping reachable. Rack-to-rack CXL transfer must
compose this byte-plane endpoint with Mooncake's RDMA transport and the planned
DRAM bounce buffers.

## One-command build

Run this on Linux in an ABI-compatible environment, preferably the same image
used by the vLLM/LMCache launcher:

```bash
git submodule update --init --recursive extern/pybind11 extern/yalantinglibs
sudo apt-get install -y \
  libcurl4-openssl-dev libxxhash-dev libzstd-dev libmsgpack-dev \
  libboost-dev libnuma-dev libibverbs-dev libasio-dev
MOONCAKE_USE_CUDA=OFF bash scripts/build_todo1_overlay.sh
```

Use the final line by itself on subsequent CPU-only rebuilds. The default CUDA
build remains:

```bash
bash scripts/build_todo1_overlay.sh
```

The script creates `build-todo1/`, builds and installs the pinned
`extern/yalantinglibs` submodule into `build-todo1/_deps/` (no `sudo`), enables
CXL, Store, HTTP metadata, unit tests, and examples, runs every `todo1` CTest
target, and stages the full Python overlay at `mooncake-wheel/mooncake/`. CUDA
is enabled by default; use `MOONCAKE_USE_CUDA=OFF` only for a CPU-only
validation build. Additional CMake flags can be supplied through
`MOONCAKE_CMAKE_ARGS`. Set `MOONCAKE_YALANTINGLIBS_DIR` only when intentionally
using an existing installation; it must name the directory containing
`yalantinglibsConfig.cmake`.

The submodule source must be copied without broad `build*` exclusions:
`extern/yalantinglibs/cmake/build.cmake` is a tracked source helper, not a build
artifact. If a lab copy omitted it, restore the specific file with
`git -C extern/yalantinglibs restore --source=HEAD --worktree -- cmake/build.cmake`.
The build also fails preflight when `extern/pybind11` is incomplete or when
`curl-config` or `xxhash.h` is absent. `USE_HTTP=ON` requires the libcurl
development package, not only the `curl` command-line program. Mooncake Store
uses xxHash unconditionally for checksums; do not disable it for TODO#1.

Do not build the pybind extensions on macOS or an arbitrary host and mount them
into the Linux runtime image. Python, glibc/libstdc++, CUDA, ibverbs, and other
shared-library ABIs must match the launcher image.

## Provider selection

Phase one defaults to the explicit open model:

```bash
export MC_CXL_PROVIDER=faketract
export MC_CXL_POOL_ID=rack0-pool0
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_DEV_SIZE=68719476736
```

For the confidential implementation, its process initialization must call:

```cpp
RegisterCxlPoolBackendProvider("tract", &OpenTraCTCxlPoolBackend);
```

and deployment selects it with `MC_CXL_PROVIDER=tract`. A selected but
unregistered provider fails closed. It never falls back to FakeTraCT.

## Portable and device-DAX gates

After building:

```bash
ctest --test-dir build-todo1 --output-on-failure -L todo1
```

The four T0 tests independently cover preflight/provider selection, functional
allocation and checksum, failure cleanup, and status. The device-DAX test skips
unless `MC_CXL_DEV_PATH` names `/dev/dax*`; run it only against a dedicated
test extent whose contents may be modified.

## Minimal standalone CXL startup

The Mooncake master and CXL client must agree on the same path and capacity:

```bash
mooncake_master --port=50051 --enable_cxl=true \
  --allocation_strategy=cxl --cxl_path=/dev/dax0.0 \
  --cxl_size=68719476736

MC_CXL_PROVIDER=faketract MC_CXL_POOL_ID=rack0-pool0 \
MC_CXL_BACKEND_KIND=devdax MC_CXL_DEV_PATH=/dev/dax0.0 \
MC_CXL_DEV_SIZE=68719476736 \
mooncake_client --host=127.0.0.1 --port=50052 --protocol=cxl \
  --global_segment_size=0 --master_server_address=127.0.0.1:50051 \
  --metadata_server=P2PHANDSHAKE
```

Use a host-reachable address instead of `127.0.0.1` when another process must
contact the client. Confirm the actual DAX size from sysfs before launch.

## HBF overlay launcher audit

The HBF wrappers default to `hbf/src/Mooncake-dev`, not this checkout. Point
them at the staged package explicitly:

```bash
MOONCAKE_SRC_HOST=/path/to/callosum/Mooncake-dev \
MOONCAKE_PACKAGE_HOST=/path/to/callosum/Mooncake-dev/mooncake-wheel/mooncake \
bash /path/to/hbf/scripts/run_mooncake_overlay.sh config
```

Current HBF launchers are suitable for LMCache-to-Mooncake DRAM/RDMA testing,
but not yet for the TODO#1 CXL endpoint:

- `MOONCAKE_PROTOCOL` accepts only `rdma` or `tcp`, not `cxl`.
- `/dev/dax*` is not passed into master, store, or worker containers.
- `MC_CXL_PROVIDER` and `MC_CXL_*` are not propagated.
- `mooncake_master` is not started with `enable_cxl`, `cxl_path`, `cxl_size`,
  and `allocation_strategy=cxl`.
- The directory overlay replaces the entire installed `mooncake` package, so
  its pybind modules and service binaries must match the image ABI.
- `run_lmcache_mooncake_overlay.sh` overlays LMCache 0.4.4 and may copy
  `c_ops*.so` from the image; this is safe only when the Python/Torch/CUDA ABI
  matches that source tree.
- The custom Dynamo/vLLM worker paths have unrelated defaults and must exist or
  be disabled/overridden.

The current Mooncake source does expose `MooncakeDistributedStore.batch_get_into`,
so the HBF Mooncake API preflight is satisfied once the package imports.

## Debug signals

Search logs by structured component/event rather than private TraCT internals:

- `component=cxl_pool_backend`: provider, logical pool, mapping, allocation,
  lookup, commit/abort, and lifecycle status.
- `component=cxl_transport`: provider open, offset validation, and copy errors.
- `component=mooncake_store event=cxl_mount`: client mapping/capacity checks.
- `component=segment_manager event=cxl_mount`: master/client capacity mismatch.

Status intentionally omits payload bytes, raw pointers, private metadata nodes,
locks, and RDMA keys.
