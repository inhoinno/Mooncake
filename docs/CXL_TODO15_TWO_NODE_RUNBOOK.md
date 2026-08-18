# TODO1.5: two-node Mooncake Store over one shared CXL pool

## Purpose

This is the first two-node acceptance gate after TODO1 device-DAX mapping. It
uses the existing `MooncakeDistributedStore` path directly, before vLLM or a KV
connector is introduced:

```text
Mooncake Master (one metadata owner and logical CXL allocator)
                  |
       +----------+----------+
       |                     |
Node 0 Store client     Node 1 Store client
       |                     |
local CxlTransport      local CxlTransport
       |                     |
       +---- physically shared CXL pool ----+
```

The test performs these operations with deterministic 4 KiB, 64 KiB, 1 MiB,
and 16 MiB objects:

1. Node 0 `put`; Node 1 `get`.
2. Node 0 `put_batch`; Node 1 `get_batch`.
3. Node 1 repeats both APIs toward Node 0.

Every received object must have the exact expected length, SHA-256 checksum,
and byte sequence. Coordination markers are Mooncake Store objects, not a
side-channel server, so the Master metadata path is part of the gate.

## What this test does and does not prove

A two-node PASS proves that the current Mooncake Master metadata, mounted CXL
replica descriptors, portable offsets, both local mappings, and Store
single/batch APIs interoperate for this shared pool. It does not exercise vLLM,
TraCT's confidential index/allocator, RDMA payload transfer, or rack-to-rack
replication.

`P2PHANDSHAKE` still exchanges endpoint and segment metadata over the network.
The object payload is expected to be copied by each node's local `CxlTransport`
from its own mapping. To prove that no payload traversed a NIC, record NIC byte
counters separately before and after the test; this harness does not infer that
fact from a checksum PASS.

## Safety and prerequisites

- Use a dedicated, coherent CXL shared-memory extent visible to both physical
  nodes. Do not target an in-use TraCT pool.
- Both nodes must use the same logical `MC_CXL_POOL_ID`, capacity, and mapping
  offset. Their local `/dev/daxX.Y` names may differ.
- The Mooncake Master and both P2P client endpoints must be mutually reachable.
- The current staged `mooncake.store` and `mooncake_master` artifacts must be
  built by `scripts/bootstrap_todo1_lab.sh` on both nodes.
- The DAX device must be readable and writable by the test user.
- Reserve at least 1 GiB for this gate. The deterministic data alone occupies
  71,581,696 bytes across both directions, before allocator and metadata
  overhead.

The launcher rejects regular files and requires
`MC_CXL_TEST_DESTRUCTIVE=1`. It removes only its run-specific Store keys after
both directions pass when `TODO15_CLEANUP=1` (the default).

## 1. Portable preflight tests

Run this on either checkout before entering the hardware phase:

```bash
cd ~/inho/my_projects/callosum/Mooncake-dev
bash scripts/run_todo15_shared_cxl.sh unit
```

Expected result: four independently reported PASS cases for configuration,
bidirectional single/batch behavior, corruption blocking publication, and safe
status output. These are T0 harness tests, not hardware evidence.

## 2. Start the single Master on Node 0

Choose one run ID and copy it exactly to both client terminals. The example
assumes Node 0 is `10.0.0.10` and Node 1 is `10.0.0.11`.

```bash
cd ~/inho/my_projects/callosum/Mooncake-dev

export MC_CXL_TEST_DESTRUCTIVE=1
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_PROVIDER=faketract
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_DEV_SIZE=1073741824
export MC_CXL_MAP_OFFSET=0
export MC_CXL_POOL_ID=rack0-shared-pool0
export MOONCAKE_MASTER_ADDRESS=10.0.0.10:50051
export TODO15_METRICS_PORT=19003

bash scripts/run_todo15_shared_cxl.sh master
```

Keep this terminal running. The script owns no background daemon and stops
nothing else; use Ctrl-C after both client summaries report PASS.

## 3. Start Node 1 first

Node 1 may have a different local DAX device name, but the pool ID and capacity
must be identical. It waits for Node 0's Store marker.

```bash
cd ~/inho/my_projects/callosum/Mooncake-dev

export MC_CXL_TEST_DESTRUCTIVE=1
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_PROVIDER=faketract
export MC_CXL_DEV_PATH=/dev/dax1.0
export MC_CXL_DEV_SIZE=1073741824
export MC_CXL_MAP_OFFSET=0
export MC_CXL_POOL_ID=rack0-shared-pool0
export MOONCAKE_MASTER_ADDRESS=10.0.0.10:50051

export TODO15_RUN_ID=todo15-20260817-001
export TODO15_NODE_ID=node1
export MOONCAKE_LOCAL_HOSTNAME=10.0.0.11:50072
export TODO15_OUTPUT_DIR=/tmp/callosum-todo15

bash scripts/run_todo15_shared_cxl.sh node1
```

## 4. Run Node 0

Use a separate Node 0 terminal from the Master:

```bash
cd ~/inho/my_projects/callosum/Mooncake-dev

export MC_CXL_TEST_DESTRUCTIVE=1
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_PROVIDER=faketract
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_DEV_SIZE=1073741824
export MC_CXL_MAP_OFFSET=0
export MC_CXL_POOL_ID=rack0-shared-pool0
export MOONCAKE_MASTER_ADDRESS=10.0.0.10:50051

export TODO15_RUN_ID=todo15-20260817-001
export TODO15_NODE_ID=node0
export MOONCAKE_LOCAL_HOSTNAME=10.0.0.10:50071
export TODO15_OUTPUT_DIR=/tmp/callosum-todo15
export TODO15_CLEANUP=1

bash scripts/run_todo15_shared_cxl.sh node0
```

## 5. Acceptance criteria

Both nodes must exit zero and produce one terminal JSON event with
`"status": "PASS"`. Inspect the summaries:

```bash
python3 -m json.tool /tmp/callosum-todo15/todo15-20260817-001-node0.json
python3 -m json.tool /tmp/callosum-todo15/todo15-20260817-001-node1.json
```

Each summary must report:

- `objects_put: 8` and `objects_get: 8`;
- `bytes_put: 35790848` and `bytes_get: 35790848`;
- `checksums_verified: 8`;
- `checksum_algorithm: "sha256"`;
- `exact_byte_equality: true`;
- all four single/batch write/read phases complete in its direction.

Any setup error, miss, wrong batch cardinality, size mismatch, checksum
mismatch, exact-byte mismatch, stale run key, marker timeout, or cleanup error
returns nonzero and writes a `FAIL` summary. A failed node must not be reported
as a hardware PASS merely because the other node completed some operations.

## Debugging order

1. Confirm both nodes print the same pool ID/capacity and their correct local
   DAX path.
2. Confirm the Master is reachable and logs two mounted CXL segments with
   distinct client endpoints.
3. Confirm firewalls permit the P2P handshake endpoints advertised by both
   clients. A shared CXL payload path does not remove this metadata dependency.
4. Compare the first `test_failure` JSON event and the corresponding Master
   log. Do not use a new run with the same ID until stale objects are removed.
5. For suspected CXL coherence/mapping faults, rerun with a fresh run ID and
   compare the expected/actual SHA-256 values without printing payload bytes.

If one node dies, restart both client roles with a new run ID. Writer-disconnect
survival and Master restart persistence are later failure-injection gates; they
are not silently treated as passing behavior in this first milestone.
