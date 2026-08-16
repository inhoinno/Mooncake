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
bash scripts/bootstrap_todo1_lab.sh
```

This is the canonical fresh-lab and subsequent CPU rebuild command. It installs
the complete Ubuntu/Debian dependency manifest, initializes the pinned pybind11
and yalantinglibs submodules, creates `build-todo1-cpu/.venv`, passes that same
Python interpreter to CMake and wheel packaging, builds with at most 16 jobs by
default, runs all TODO#1 tests, and validates the staged overlay and repaired
wheel. It never writes to system Python, so Ubuntu's PEP 668 policy does not
require `--break-system-packages`.

Useful diagnostic and restricted modes are:

```bash
bash scripts/bootstrap_todo1_lab.sh --preflight
bash scripts/bootstrap_todo1_lab.sh --status
bash scripts/bootstrap_todo1_lab.sh --skip-apt --skip-submodules
```

Every invocation records a timestamped transcript in
`build-todo1-cpu/setup-logs/`; `latest.log` points to the newest one. A failed
run prints the relevant compiler/CMake lines plus diagnoses for the known lab
failures: incomplete submodule copies, local yalantinglibs packaging, missing
yaml-cpp/curl/xxHash headers, missing pybind11, Ninja invoked after a failed
configure, CUDA cache contamination, incorrect `venv activate`, missing `python`,
PEP 668, hidden Unicode in pasted environment assignments, and misleading
warning-only output.

The lower-level CPU build command remains available when dependencies,
submodules, and a private Python environment have already been prepared:

```bash
MOONCAKE_USE_CUDA=OFF MOONCAKE_BUILD_DIR=build-todo1-cpu \
MOONCAKE_BUILD_JOBS=16 PYTHON_BIN="$PWD/build-todo1-cpu/.venv/bin/python" \
bash scripts/build_todo1_overlay.sh
```

The default CUDA build remains:

```bash
bash scripts/build_todo1_overlay.sh
```

Keep CPU-only and CUDA configurations in separate build directories. The
bootstrap enforces this by using `build-todo1-cpu`. If an
existing cache unexpectedly compiles `device/p2p_device_transport.cpp` during
an `MOONCAKE_USE_CUDA=OFF` build, preserve it for inspection and start a clean
CPU graph without deleting anything:

```bash
MOONCAKE_USE_CUDA=OFF MOONCAKE_BUILD_DIR=build-todo1-cpu \
MOONCAKE_BUILD_JOBS=16 bash scripts/build_todo1_overlay.sh
```

The CPU wrapper explicitly disables `USE_NVMEOF`, `USE_MNNVL`,
`USE_VRAM_SEGMENT`, NCCL device/host, MUSA, and MACA because those cached
features can enable the GPU device transport or turn CUDA back on.

Wheel staging uses `python3` when the optional `python` compatibility command
is absent. Set `PYTHON_BIN=/absolute/path/to/python3` to force the interpreter;
it must match the Python ABI used by CMake for `engine.so` and `store.so`.

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
ctest --test-dir build-todo1-cpu --output-on-failure -L todo1 -LE hardware
```

The four T0 backend-contract tests independently cover preflight/provider
selection, functional allocation and checksum, failure cleanup, and status.
The endpoint-projection unit gate and Store integration gate add two more T0
CTest targets. The device-DAX test skips unless `MC_CXL_DEV_PATH` names
`/dev/dax*`; run it only against a dedicated test extent whose contents may be
modified. Hardware is excluded from the bootstrap and must be requested
explicitly:

```bash
MC_CXL_PROVIDER=faketract MC_CXL_BACKEND_KIND=devdax \
MC_CXL_POOL_ID=rack0-pool0 MC_CXL_TEST_DESTRUCTIVE=1 \
MC_CXL_DEV_PATH=/dev/dax0.0 MC_CXL_DEV_SIZE=<device-bytes> \
ctest --test-dir build-todo1-cpu --output-on-failure -V \
  -R '^cxl_pool_backend_devdax_test$'
```

### Single-process Mooncake Store integration

The shortest real Store integration starts an in-process Master, mounts a
file-backed FakeTraCT pool, runs Put/Get and BatchPut/BatchGet through
`CxlTransport`, and validates payload checksums:

```bash
bash scripts/run_todo1_single_cxl_test.sh
```

The runner resolves the binary relative to the repository, so it works from any
current directory. It creates a unique disposable `/tmp` file and the test
unlinks that file during teardown; no manual `truncate` is required. Override
`MOONCAKE_BUILD_DIR` when the binary is in another build tree. To request a
specific disposable regular file, set `MC_CXL_TEST_FILE=/tmp/randomfile`; the
file will be truncated and unlinked. Device nodes are rejected because this is
the T0 file-backed gate, not the destructive T2 devdax test.

The same two cases are registered as `cxl_client_single_process_test` with the
`todo1` CTest label. A TODO#1 bootstrap is not green unless this complete path
passes. In `P2PHANDSHAKE` mode, Mooncake keeps the logical segment name for
placement but publishes the dynamically bound Transfer Engine endpoint in the
replica descriptor; otherwise Put would connect to the logical port and fail
with `TRANSFER_FAIL`/`ECONNREFUSED`.

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
