# CXL Single-Node PUT/GET Runbook + TODO 1.5 / TODO 5-A Milestone

Operational runbook for the **Mooncake Store CXL memory backend**: how to build,
configure, and run single-node PUT/GET validation, and how that graduates into the
next lab milestone — **two-node shared-CXL, single-Master validation**.

- **Source of truth:** `DESIGN.md` (callosum root). §18 = TODO 1 (single-node CXL
  backend); §18A = TODO 1.5 (two-node shared CXL). Read those before changing anything.
- **Where the code lives:** the active Mooncake fork is the nested clone
  `Mooncake-dev/` (per `CLAUDE.md` rule 7, changes there are *not* tracked by the
  callosum parent). Every command below runs from **`Mooncake-dev/`** unless noted.
- **Backend rule:** CXL is a memory-device/allocator/transport boundary, **not** a
  `StorageBackendType::kCxl`. The transport is `CxlTransport`; the allocator path is
  `CxlAllocationStrategy` over a `CxlPoolBackend`. See `DESIGN.md` §3, §18.

---

## Part 0 — Prerequisites and platform

| Requirement | Value | Why |
|---|---|---|
| OS | Linux (Ubuntu/Debian apt host) | Store bindings + CXL transport are Linux-only; the scripts hard-fail on non-Linux. |
| Build dir | `build-todo1-cpu` (in-repo, default) | CPU-only cache with every CUDA/GPU trigger forced OFF; `USE_CXL=ON`, `WITH_STORE=ON`. |
| Python | ≥ 3.10 in `build-todo1-cpu/.venv` | Wheel packaging avoids PEP 668 system-pip failure. |
| T0 provider | `MC_CXL_PROVIDER=faketract` | Portable file-backed model for unit/integration development. |
| T2 provider | `MC_CXL_PROVIDER=mooncake` | Native mapping-only backend; Mooncake Master owns allocation and object metadata. |

There are two single-node gates. Part 1 is **T0 / software** over a disposable file and
FakeTraCT. Part 2 is the **T2 / native device-DAX** gate over the built-in Mooncake
provider. Neither is a bandwidth benchmark; both are correctness tests.

---

## Part 1 — Single-node PUT/GET (TODO 1, T0 software gate)

Topology: **one process.** An in-process Mooncake Master, one Store client, the
`CxlAllocationStrategy`, the Transfer Engine, and a file-backed CXL mapping. The
end-to-end put/get path exercised is `DESIGN.md` §3.2 / §3.3:

```
Client::Put -> MasterClient::PutStart -> CxlAllocationStrategy::Allocate
            -> AllocatedBuffer::change_to_cxl -> Client::TransferWrite
            -> MultiTransport selects cxl -> CxlTransport::submitTransfer(WRITE)
            -> memcpy into mapped CXL offset -> MasterClient::PutEnd
Client::Get -> GetReplicaList -> FindFirstCompleteReplica -> TransferRead
            -> CxlTransport::submitTransfer(READ) -> memcpy out -> checksum verify
```

### 1.1 Build the CPU-only CXL Store stack

One resumable command installs deps, initializes the pinned submodules, builds the
CPU overlay, runs the TODO 1 CTests, and repairs the wheel:

```bash
cd Mooncake-dev
bash scripts/bootstrap_todo1_lab.sh
```

Useful sub-modes (no apt/git/build side effects for the first two):

```bash
bash scripts/bootstrap_todo1_lab.sh --preflight   # deps/toolchain/headers only
bash scripts/bootstrap_todo1_lab.sh --status      # cache flags, TODO1 test list, artifacts
```

The build gate asserts `USE_CXL=ON`, `WITH_STORE=ON`, and all of
`USE_CUDA/USE_NVMEOF/USE_MNNVL/USE_VRAM_SEGMENT/USE_NCCL_DEVICE/USE_NCCL_HOST/USE_MUSA/USE_MACA = OFF`.
It produces `mooncake-wheel/mooncake/{engine.so,store.so,mooncake_master,mooncake_client}`
and a repaired wheel under `mooncake-wheel/dist/`.

### 1.2 Run the PUT/GET test — the one command

The shortest real Store → CXL path. Creates a disposable 1 GiB temp pool file,
runs checksum-enabled Basic + Batch Put/Get, and cleans up:

```bash
cd Mooncake-dev
bash scripts/run_todo1_single_cxl_test.sh
```

What it sets and runs (from the script — this is the authoritative invocation):

```
MC_CXL_PROVIDER=faketract            # open file-backed provider
MC_CXL_BACKEND_KIND=file             # not devdax
MC_CXL_POOL_ID=callosum-single-pool  # stable logical pool identity
MC_CXL_DEV_PATH=<mktemp /tmp file>   # disposable regular file (never /dev/*)
MC_CXL_DEV_SIZE=1073741824           # 1 GiB (override via env)
MOONCAKE_STORE_CHECKSUM=1            # exact checksum verification on
DEFAULT_KV_LEASE_TTL=1
  cxl_client_integration_test
    --protocol=cxl
    --cxl_device_name=<pool file>
    --cxl_device_size=1073741824
    --transfer_engine_metadata_url=P2PHANDSHAKE
    --gtest_filter=ClientIntegrationTestCxl.BasicPutGetOperations:ClientIntegrationTestCxl.BatchPutGetOperations
```

Override the pool size or supply your own disposable file:

```bash
MC_CXL_DEV_SIZE=$((8*1024*1024*1024)) bash scripts/run_todo1_single_cxl_test.sh
MC_CXL_TEST_FILE=/tmp/my-pool.bin      bash scripts/run_todo1_single_cxl_test.sh  # truncated+unlinked
```

**PASS line:** `[PASS] single-process Mooncake CXL Put/Get and BatchPut/BatchGet`.

> Regression guard baked into the binary: the CXL replica must carry Transfer
> Engine's dynamically bound P2P handshake endpoint, **not** the logical segment name
> (`localhost:17813`). A binary predating that fix fails with `TRANSFER_FAIL` /
> `ECONNREFUSED` because it dials a port with no handshake listener. If you see that,
> rebuild (§1.1). See `DESIGN.md` retrospect 2026-08-15.

### 1.3 Same gate through CTest

The bootstrap already runs these. To run them directly:

```bash
ctest --test-dir build-todo1-cpu -L todo1 --output-on-failure -LE hardware
```

Relevant registered tests (labels `cxl;todo1;t0`):

| CTest name | What it covers |
|---|---|
| `cxl_allocation_endpoint_test` | Allocation strategy uses the mounted transfer endpoint; clears it when the last allocator is removed (unit). |
| `cxl_client_single_process_test` | The Basic + Batch Put/Get integration path over a 1 GiB file-backed pool (same gtest filter as §1.2). |

### 1.4 The four-test law for TODO 1 (`DESIGN.md` §18.3 / §17)

Every TODO carries four independently runnable gates. For single-node CXL:

| Tier | Test | Runnable command |
|---|---|---|
| **Preflight** | feature/config/path/capacity/alignment validation, incl. absent device and malformed sizes | `bash scripts/bootstrap_todo1_lab.sh --preflight` |
| **Functional** | file-backed open → alloc → write → commit → lookup → checksum → close | `bash scripts/run_todo1_single_cxl_test.sh` (or `ctest -R cxl_client_single_process_test`) |
| **Failure/debug** | bounds, overflow, exhaustion, map failure, abort leaves no visible entry or leaked reservation | `ctest --test-dir build-todo1-cpu -L todo1 -LE hardware` (failure/cleanup cases) |
| **Status** | pool ID, backend kind, capacity, allocation state, commit/abort, terminal fields — no private metadata | `bash scripts/bootstrap_todo1_lab.sh --status` |

Report each as **PASS / FAIL / SKIP** (rule 9). Device-DAX mapping is deliberately a
separate T2 gate and reports **SKIP** on a file-backed host.

### 1.5 Optional — master-backed throughput probe

To exercise a real out-of-process `mooncake_master` + Store client with throughput
numbers (still file-backed, still T0 — no hardware-bandwidth claim):

```bash
cd Mooncake-dev
bash scripts/run_todo1_cxl_store_bench.sh
```

Key knobs (all validated; see script header): `CXL_BENCH_POOL_SIZE_BYTES` (8 GiB
default), `CXL_BENCH_NUM_OBJECTS`, `CXL_BENCH_VALUE_SIZE` (must be 512-byte aligned),
`CXL_BENCH_BATCH_SIZE`, `CXL_BENCH_MASTER_PORT`, `CXL_BENCH_METRICS_PORT` (must differ).

---

## Part 2 — Single-node native Mooncake CXL PUT/GET (T2 gate)

This is the required no-sharing-mode hardware check before adding Node 1. It runs one
out-of-process Master and one direct `MooncakeDistributedStore` client. The client maps
the complete `/dev/dax` region; the Master creates the `CachelibBufferAllocator` for
that advertised extent and owns object metadata, allocation, publication, and remove.

```
Mooncake Master
  object metadata + CxlAllocationStrategy + full-pool allocator
                         |
MooncakeDistributedStore (direct public Store API; no vLLM yet)
                         |
                    CxlTransport
                         |
                    /dev/daxX.Y
```

### 2.1 Safety and device setup

Use a **dedicated test DAX region**. The test writes deterministic payloads into the
device and is destructive with respect to existing data in that extent. It never
formats, truncates, unlinks, or resizes the device.

On the lab host, identify the device and its byte capacity:

```bash
daxctl list -u
ls -l /dev/dax*
cat /sys/bus/dax/devices/dax0.0/size   # replace dax0.0 with the selected device
```

The user running Mooncake must have read/write permission on the character device.
Use the lab's persistent udev/group policy; a temporary ACL may be used only if that is
the lab administrator's normal policy. Do not run the whole Store as root merely to
bypass device permissions.

Build and run the portable tests first:

```bash
cd Mooncake-dev
bash scripts/bootstrap_todo1_lab.sh
bash scripts/run_todo15_one_node_cxl.sh unit
```

The `unit` mode is T0 and does not open `/dev/dax`. It runs four focused tests:
preflight/full-pool ownership, functional single+batch PUT/GET/remove, corruption
failure/cleanup, and safe status output.

### 2.2 Native environment

Set the exact mapped capacity reported for the dedicated device. It must be at least
64 MiB and a positive multiple of 16 MiB because the Master's pinned CacheLib
allocator operates in 16 MiB slabs and this matrix exercises four allocation classes.

```bash
cd Mooncake-dev

export MC_CXL_TEST_DESTRUCTIVE=1
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_DEV_SIZE=<device-size-in-bytes>
export MC_CXL_POOL_ID=lab-rack0-pool0

# Optional; defaults shown.
export MOONCAKE_MASTER_ADDRESS=127.0.0.1:50051
export TODO15_ONE_LOCAL_ENDPOINT=127.0.0.1:50071
export TODO15_ONE_METRICS_PORT=19003
export TODO15_ONE_LOCAL_BUFFER_SIZE=268435456
export TODO15_ONE_RUN_ID=todo15-one-$(date +%Y%m%d-%H%M%S)
```

The launcher deliberately forces these native ownership values; do not override them
for the one-node baseline:

```
MC_CXL_PROVIDER=mooncake
MC_CXL_BACKEND_KIND=devdax
MC_CXL_MAP_OFFSET=0
MC_CXL_OWNED_OFFSET=0
MC_CXL_OWNED_SIZE=$MC_CXL_DEV_SIZE
MOONCAKE_STORE_CHECKSUM=1
```

`MC_CXL_PROVIDER=mooncake` is important. This provider maps and resolves CXL offsets,
but does not run FakeTraCT's private object allocator/index. Allocation remains in the
single Mooncake Master.

### 2.3 Preflight and the one-command hardware run

Preflight checks the OS, character device, permissions, capacity/alignment, endpoints,
staged bindings, and provider contract without mapping or writing the device:

```bash
bash scripts/run_todo15_one_node_cxl.sh preflight
```

Run the actual test:

```bash
bash scripts/run_todo15_one_node_cxl.sh run
```

The launcher starts only its own Master process, waits for RPC readiness, runs the
client, validates the JSON summary, and terminates only that recorded Master PID. Logs
and the summary are written under
`/tmp/callosum-todo15-one/$TODO15_ONE_RUN_ID/` by default.

### 2.4 Actual native call path

Single PUT/GET:

```
MooncakeDistributedStore.put
  -> RealClient::put -> Client::Put
  -> MasterClient::PutStart
  -> MasterService::PutStart
  -> CxlAllocationStrategy::Allocate
  -> Master-owned CachelibBufferAllocator (logical CXL offset, PROCESSING)
  -> Client::TransferWrite
  -> MultiTransport -> CxlTransport::submitTransfer(WRITE)
  -> local buffer copied into resolve(offset) on /dev/dax
  -> MasterClient::PutEnd -> replica COMPLETE

MooncakeDistributedStore.get
  -> RealClient::get -> Client::Get
  -> MasterClient::GetReplicaList (COMPLETE replica only)
  -> Client::TransferRead
  -> MultiTransport -> CxlTransport::submitTransfer(READ)
  -> resolve(offset) on the same local mapping -> destination buffer
  -> Store checksum + harness SHA-256 and exact-byte verification
```

The batch path uses `put_batch/get_batch` and the corresponding Master batch calls.
After verification, `remove(key, true)` asks the Master to remove metadata and reclaim
the allocation; the harness confirms every key is absent.

### 2.5 Matrix, expected result, and current 16 MiB limit

The test executes both single and batch APIs at **4 KiB, 64 KiB, 1 MiB, and
16 MiB - 4 KiB**. The final value is intentional: the actual source currently sends
the total object length from `MasterClient::PutStart`, and `MasterService::PutStart`
rejects a CacheLib object above `kMaxSliceSize = Slab::kSize - 16`. Therefore an exact
16 MiB object does not pass today even though the client buffer code can split input
slices. Do not report exact 16 MiB coverage until that allocation limitation changes.

Expected summary:

- 8 PUTs and 8 GETs: four sizes through single APIs plus four through batch APIs
- 8 SHA-256 and exact-byte validations
- 35,782,656 bytes written and 35,782,656 bytes read
- 8 removes and zero remaining test keys
- final line: `[PASS] one-node Mooncake Store native CXL PUT/GET matrix`

This PASS proves the native mapping, Master allocation/publication, CXL write/read,
integrity, and reclaim path on one node. It does **not** prove shared visibility from a
second host, vLLM/MooncakeStoreConnector integration, rack-to-rack RDMA, or CXL
bandwidth.

---

## Part 3 — Next milestone: TODO 1.5 / TODO 5-A — Two-node shared-CXL, single-Master

**Status:** `DESIGN.md` §18A — *READY FOR T2 LAB EXECUTION (T0 harness PASS)*.

Architecturally this belongs to **TODO 5** (rack-local multi-node deployment). Because
device-DAX now works, run it first as a **TODO 1.5 Store-level integration gate** before
any vLLM MultiConnector work. It is deliberately **not TODO 6**, because there is:

- one shared CXL pool
- one rack
- no rack-to-rack RDMA
- one Mooncake Master

### 3.1 Target topology

```
                    Mooncake Master
              metadata + logical allocation
                         |
              +----------+----------+
              |                     |
       Node 0 Store client     Node 1 Store client
 MooncakeDistributedStore  MooncakeDistributedStore
       CxlTransport             CxlTransport
              |                     |
              +------ offsets ------+
                         |
               Shared CXL pool
```

The TODO 1.5 acceptance harness calls `MooncakeDistributedStore` directly. The future
`MooncakeStoreConnector` sits above this API, but vLLM connector/worker behavior is
deliberately outside this Store-level gate.

### 3.2 Ownership split

**The Master owns:**
- object → replica metadata
- logical offset allocation (the existing `CxlAllocationStrategy`)
- `PROCESSING → COMPLETE` publication state
- remove / eviction lifecycle
- client health

**Both nodes own:**
- their local CXL mapping (local `/dev/dax` name may differ per node)
- offset validation
- local CXL reads and writes
- checksum validation

**Invariant:** both nodes must agree on the same logical `cxl_pool_id`, capacity,
mapping offset, and backend version. They may `mmap` the pool at **different virtual
addresses** — only **offsets** may cross a process/node boundary. With the native
provider, each node also advertises a **disjoint 16 MiB-aligned allocation-owned
subrange** while still mapping the complete pool. (`DESIGN.md` §18A.2 and §19.)

### 3.3 Expected function path

**Node 0 writes:**
```
Node 0 MooncakeDistributedStore.put
  -> PutStart(object_id)
  -> Master CxlAllocationStrategy
  -> allocate(pool_id, offset, length)
  -> replica status = PROCESSING
  -> Node 0 CxlTransport::write(offset)
  -> checksum
  -> PutEnd()
  -> replica status = COMPLETE
```

**Node 1 reads:**
```
Node 1 MooncakeDistributedStore.get
  -> Get(object_id)
  -> Master metadata lookup
  -> CXL replica(pool_id, offset, length)
  -> verify Node 1 has matching pool mounted
  -> Node 1 CxlTransport::resolve(offset)
  -> CXL read
  -> checksum
  -> return KV
```

Because both nodes share the same physical pool, **Node 1 resolves the offset through
its own local mapping.** Payload transfer does **not** go through RDMA or TCP. (Note:
`P2PHANDSHAKE` is still a *metadata/control* dependency; this milestone does not claim
network-free control or RDMA payload transfer. Capture NIC counters separately if you
need to *prove* the payload avoided the NIC — the correctness harness does not infer it.)

### 3.4 Run procedure — three terminals

Native launcher: `scripts/run_todo2_native_cxl.sh <master|node0|node1|unit>`. It is a
strict ownership wrapper around the TODO 1.5 protocol harness, forces
`MC_CXL_PROVIDER=mooncake`, keeps the Master **foreground**, and never kills
system-wide processes. `MOONCAKE_STORE_CHECKSUM=1` is forced on.

**Shared environment on Master + both nodes** (must be identical where noted):

```bash
export MC_CXL_TEST_DESTRUCTIVE=1              # explicit destructive opt-in
export MC_CXL_BACKEND_KIND=devdax             # TODO1.5 accepts only devdax
export MC_CXL_DEV_PATH=/dev/daxX.Y            # LOCAL device path — may differ per node
export MC_CXL_DEV_SIZE=<mapped bytes>         # MUST match on both nodes + Master
export MC_CXL_MAP_OFFSET=0                    # MUST match on both nodes
export MC_CXL_POOL_ID=<stable pool id>        # MUST match on both nodes
export MOONCAKE_MASTER_ADDRESS=<host:port>
```

Each client also exports one disjoint portion of the full pool. Example for a 1 GiB
mapping (both boundaries are 16 MiB aligned):

```
Node 0: MC_CXL_OWNED_OFFSET=0          MC_CXL_OWNED_SIZE=536870912
Node 1: MC_CXL_OWNED_OFFSET=536870912  MC_CXL_OWNED_SIZE=536870912
```

**Terminal A — Master (foreground):**
```bash
cd Mooncake-dev
bash scripts/run_todo2_native_cxl.sh master
# execs: mooncake_master --rpc_port=<port> --metrics_port=19003
#        --enable_cxl=true --allocation_strategy=cxl
#        --cxl_path=$MC_CXL_DEV_PATH --cxl_size=$MC_CXL_DEV_SIZE
#        --default_kv_lease_ttl=1h
```

**Terminal B — Node 1 reader (start before Node 0):**
```bash
cd Mooncake-dev
export TODO2_RUN_ID=<unique-id>               # IDENTICAL on node0 and node1
export TODO2_NODE_ID=node1
export MOONCAKE_LOCAL_HOSTNAME=<host:port>    # unique, peer-reachable, != Master
export MC_CXL_OWNED_OFFSET=<node1 partition offset>
export MC_CXL_OWNED_SIZE=<node1 partition size>
bash scripts/run_todo2_native_cxl.sh node1
```

**Terminal C — Node 0 writer (owns cleanup):**
```bash
cd Mooncake-dev
export TODO2_RUN_ID=<same-unique-id>
export TODO2_NODE_ID=node0
export MOONCAKE_LOCAL_HOSTNAME=<host:port>    # different from node1
export MC_CXL_OWNED_OFFSET=<node0 partition offset>
export MC_CXL_OWNED_SIZE=<node0 partition size>
bash scripts/run_todo2_native_cxl.sh node0
```

Optional native-wrapper knobs: `TODO2_TIMEOUT_SEC=180`, `TODO2_POLL_MS=100`,
`TODO2_CLEANUP=1` (node0 removes only its run namespace),
`TODO2_OUTPUT_DIR=/tmp/callosum-todo2`, and `TODO2_TEST_MODE=matrix|wss`.

Both nodes emit JSONL phase/status records and an atomic per-node summary under
`$TODO2_OUTPUT_DIR/<run-id>-<role>.json` — no payload bytes, raw pointers, CXL private
records, or transport keys. **Phase coordination itself uses Store objects as markers**
(not a side-channel), so a broken Master metadata path cannot look healthy.

### 3.5 The T0 protocol harness (portable, no hardware)

Before touching hardware, the two-process orchestration + fail-closed behavior runs
anywhere:

```bash
cd Mooncake-dev
bash scripts/run_todo2_native_cxl.sh unit
# or via CTest (labels cxl;todo15;t0;unit):
ctest --test-dir build-todo1-cpu -L todo15 --output-on-failure
```

Four registered cases map to the four-test law (`DESIGN.md` §18A.4):

| Tier | CTest case |
|---|---|
| Preflight | `todo15_shared_cxl_preflight_validation_test` |
| Functional | `todo15_shared_cxl_functional_bidirectional_single_and_batch_test` |
| Failure/debug | `todo15_shared_cxl_failure_checksum_mismatch_blocks_publication_test` |
| Status | `todo15_shared_cxl_status_summary_is_safe_and_complete_test` |

T0 protocol tests alone do **not** close the milestone.

### 3.6 Test matrix and exit gate

**Objects:** deterministic **4 KiB, 64 KiB, 1 MiB, 16 MiB - 4 KiB** through both the
single and batch APIs, in **both directions** (Node 0→1 and Node 1→0). Every fetched
object requires exact length, SHA-256, and byte equality. The near-16 MiB value is the
current source limit explained in §2.5.

**Exit gate (`DESIGN.md` §18A.5):** both physical nodes exit **zero** with, per node:

- **8 puts, 8 gets** (4 sizes × 2 APIs)
- **35,782,656 bytes written, 35,782,656 bytes read**
  (`(4096 + 65536 + 1048576 + 16773120) × 2`)
- **8 exact checksum/byte verifications**

Master logs show **both CXL mounts** and **no incomplete publication**. Fail closed on
pool/config mismatch, Store failure, partial batch, corruption, timeout, or cleanup
failure. If claiming the payload avoided the NIC, record NIC counters separately.

---

## Quick reference — the three gates

```bash
# Single-node PUT/GET (T0, file-backed):
cd Mooncake-dev && bash scripts/bootstrap_todo1_lab.sh \
  && bash scripts/run_todo1_single_cxl_test.sh

# Single-node PUT/GET (T2, native /dev/dax; export §2.2 environment first):
cd Mooncake-dev && bash scripts/run_todo15_one_node_cxl.sh run

# Two-node shared-CXL protocol harness (T0, portable):
cd Mooncake-dev && bash scripts/run_todo2_native_cxl.sh unit
# Two-node shared-CXL acceptance (T2): run master/node1/node0 per §3.4 on /dev/dax.
```
