# TODO #2: two native Mooncake CXL clients with one Master

## Acceptance topology

TODO #2 removes FakeTraCT from the two-node Store test. One Mooncake Master is
the only object-metadata and CXL-allocation authority. Both clients map the
complete coherent CXL pool so either can resolve every portable replica
offset, while each contributes one disjoint allocation-owned subrange:

```text
                         Mooncake Master
               object metadata + replica lifetime
                    /                      \
      node0 segment allocator       node1 segment allocator
          [0, pool/2)                 [pool/2, pool)
                    \                      /
             one physically shared CXL pool
             mapped completely on both nodes
```

`MC_CXL_PROVIDER=mooncake` is mapping-only. Its `metadata_lookup()` and
`alloc()` operations fail explicitly because those operations belong to the
Master in this milestone. The Master creates the actual CacheLib extent
allocator for each advertised partition. Removing/evicting the last replica
destroys its `AllocatedBuffer` and returns that extent to the same Master-side
allocator.

The existing TODO1.5 checksum harness supplies the payload protocol: Node 0
single/batch writes are read by Node 1, then the direction reverses, for 4 KiB,
64 KiB, 1 MiB, and 16 MiB deterministic objects. This wrapper changes the
ownership topology, not that proven protocol.

## Build and T0 gates

Build with CXL and Store enabled, then run five independent TODO #2 tests: the
four required preflight/functional/failure/status categories plus a real
single-process Store Put/Get/Batch/Remove integration gate:

```bash
cd ~/inho/my_projects/callosum/Mooncake-dev
MOONCAKE_USE_CUDA=OFF bash scripts/build_todo2_overlay.sh
```

The gates cover native-provider preflight, disjoint allocation/reclaim,
overlap/bounds/pool-ID failure including concurrent mounts, and portable
ownership status.

## Lab configuration

The example uses one 1 GiB shared pool split equally. Local `/dev/dax` names
may differ, but pool ID, full capacity, and mapping offset must describe the
same physical bytes.

Common settings in all three terminals:

```bash
export MC_CXL_TEST_DESTRUCTIVE=1
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_DEV_SIZE=1073741824
export MC_CXL_MAP_OFFSET=0
export MC_CXL_POOL_ID=rack0-shared-pool0
export MOONCAKE_MASTER_ADDRESS=10.0.0.10:50051
```

### Terminal 1: Master on Node 0

```bash
export MC_CXL_DEV_PATH=/dev/dax0.0
bash scripts/run_todo2_native_cxl.sh master
```

### Terminal 2: client Node 1

```bash
export MC_CXL_DEV_PATH=/dev/dax1.0
export MC_CXL_OWNED_OFFSET=536870912
export MC_CXL_OWNED_SIZE=536870912
export TODO2_RUN_ID=todo2-$(date -u +%Y%m%dT%H%M%SZ)
export TODO2_NODE_ID=node1
export MOONCAKE_LOCAL_HOSTNAME=10.0.0.11:50072
bash scripts/run_todo2_native_cxl.sh node1
```

Start Node 1 first; it waits for the writer marker. Copy its exact
`TODO2_RUN_ID` into Terminal 3.

### Terminal 3: client Node 0

```bash
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_OWNED_OFFSET=0
export MC_CXL_OWNED_SIZE=536870912
export TODO2_RUN_ID=<same value as Node 1>
export TODO2_NODE_ID=node0
export MOONCAKE_LOCAL_HOSTNAME=10.0.0.10:50071
export TODO2_CLEANUP=1
bash scripts/run_todo2_native_cxl.sh node0
```

## PASS evidence

Both clients must exit zero and report eight puts, eight gets, 35,790,848
bytes in each direction, eight SHA-256 verifications, and exact byte equality.
The Master must log two `event=cxl_partition_mount status=ok` records with the
same pool ID and non-overlapping owned extents. Its segment-detail endpoint
reports mapped capacity, owned offset/capacity, allocator used/capacity, and
transport endpoint for each client.

Cleanup is part of the test: after both peers acknowledge completion, Store
removal erases Master metadata and releases the corresponding partition
allocations. The T0 reclaim test also allocates again after release.

## 500 GiB single-Put/peer-Get working-set gate

Run the small bidirectional matrix above first. The pressure-free WSS gate is a
separate protocol: Node 0 uses only individual `put` calls, retains the entire
working set, and publishes `ready`; Node 1 then uses only individual peer
`get` calls and verifies SHA-256 plus exact bytes. Payload generation is
streaming and bounded to one object at a time. The reader may simultaneously
hold the 16 MiB received and expected buffers, plus binding-local copies; it
does not build an in-DRAM copy of the complete WSS.

The default exact 500 GiB plan is 536,870,912,000 bytes and 120,020 objects:

- 30,000 objects of 4 KiB;
- 30,013 objects of 64 KiB;
- 30,007 objects of 1 MiB;
- 30,000 objects of 16 MiB.

Use a minimum 1 TiB full pool for the default example. A 512 GiB writer
partition passes the default 500 GiB + 8 GiB headroom preflight. The reader
partition occupies the other half. Both boundaries are 16 MiB aligned, which
matches the actual pinned CacheLib `Slab::kSize` in this source tree.

Apply the common exports in every terminal on both nodes, then start the Master
with the Node 0-local DAX path:

```bash
export MC_CXL_TEST_DESTRUCTIVE=1
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_DEV_SIZE=1099511627776
export MC_CXL_MAP_OFFSET=0
export MC_CXL_POOL_ID=rack0-shared-pool0
export MOONCAKE_MASTER_ADDRESS=10.0.0.10:50051

export MC_CXL_DEV_PATH=/dev/dax0.0
bash scripts/run_todo2_cxl_wss.sh master
```

Start Node 1 first:

```bash
export MC_CXL_DEV_PATH=/dev/dax1.0
export MC_CXL_OWNED_OFFSET=549755813888
export MC_CXL_OWNED_SIZE=549755813888
export TODO2_RUN_ID=todo2-wss-20260817-001
export TODO2_NODE_ID=node1
export MOONCAKE_LOCAL_HOSTNAME=10.0.0.11:50072
bash scripts/run_todo2_cxl_wss.sh node1
```

Then start Node 0 in a third terminal:

```bash
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_OWNED_OFFSET=0
export MC_CXL_OWNED_SIZE=549755813888
export TODO2_RUN_ID=todo2-wss-20260817-001
export TODO2_NODE_ID=node0
export MOONCAKE_LOCAL_HOSTNAME=10.0.0.10:50071
export TODO2_CLEANUP=1
bash scripts/run_todo2_cxl_wss.sh node0
```

Run the four portable WSS protocol checks with:

```bash
bash scripts/run_todo2_cxl_wss.sh unit
```

A PASS requires Node 0 `bytes_put` and Node 1 `bytes_get` to equal
536,870,912,000, Node 1 to report 120,020 checksum verifications, the four
size counts above, matching plan digests, and terminal PASS on both nodes.
With cleanup enabled, Node 0 also removes all 120,020 objects after peer
verification and reports the same `bytes_removed`. These removals exercise
Master-owned metadata destruction and partition deallocation; they are not an
eviction-pressure test.

Use `TODO2_WSS_BYTES`, `TODO2_WSS_HEADROOM_BYTES`,
`TODO2_WSS_PROGRESS_BYTES`, `TODO2_WSS_TIMEOUT_SEC`, and
`TODO2_WSS_LEASE_TTL` to tune the run. The target must be 4 KiB aligned.
Decimal 500 GB is not aligned; use 500,000,002,048 bytes for the smallest
aligned target at or above it.

## Remaining design limits after this milestone

- Pool identity is a trust/configuration assertion. The Master cannot prove
  that differently named DAX devices expose the same coherent physical bytes.
- The first native mount establishes the pool ID for that Master lifetime.
- Allocation is partition-local and does not borrow idle capacity from another
  client partition.
- Existing eviction candidate selection is global. Under asymmetric pressure,
  evicting a replica from another partition does not free the failed writer's
  partition; partition-aware victim selection is required before pressure and
  churn are a production acceptance gate.
- Master restart persistence/allocator reconstruction and client-loss recovery
  are not proven by this two-node correctness run.
- `P2PHANDSHAKE` remains network control-plane traffic. This is shared-CXL
  payload testing, not RDMA rack-to-rack payload testing.
