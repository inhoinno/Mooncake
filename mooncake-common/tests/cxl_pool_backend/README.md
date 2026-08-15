# Mooncake CXL pool endpoint — TODO 1

This directory is the portable phase-one debug surface for Mooncake's CXL
memory tier. `mooncake::faketract` implements the public API in
`mooncake-common/include/cxl_pool_backend.h` without requiring Mooncake Store,
RDMA, vLLM, or the confidential CXL connector.

The endpoint deliberately exposes only two connector operations:

```text
metadata_lookup(opaque_object_id) -> MISS | CxlLocation
alloc(opaque_object_id, bytes)     -> CxlAllocation | failure
```

`alloc()` returns an invisible reservation. Payload bytes become discoverable
only after `commit()`; `abort()` and destruction release unpublished state. The
same backend provides validated offset translation to `CxlTransport`, which no
longer opens or maps `/dev/dax` itself.

## Code path

```text
Mooncake Store client
  -> TransferEngine / MultiTransport
  -> CxlTransport
  -> CxlPoolBackend                  (stable interface)
       faketract::MmapCxlPoolBackend (open development provider)
         file model                  (portable CI/manual debug)
         devdax                      (phase-one lab)
       private TraCT adapter         (future out-of-tree provider)
```

`faketract::MmapCxlPoolBackend` owns the mapping and a small in-process
reservation/index model. `faketract::MmapCxlAllocation` is one reservation and
enforces reserved -> committed/aborted state. These classes imitate only the
observable TraCT contract; they do not implement or replace TraCT's allocator,
prefix index, locking, eviction, or recovery.

The real adapter should implement the same `CxlPoolBackend` interface:

```text
metadata_lookup(id) -> legacy TraCT prefix-index lookup
alloc(id, bytes)     -> legacy TraCT allocation/reservation
commit()/abort()     -> legacy TraCT publication/cleanup
resolve(offset)      -> validated mapping projection
```

Mooncake Store's existing `PutStart(PROCESSING) -> transfer -> PutEnd(COMPLETE)`
and `PutRevoke` behavior remains the production object lifecycle. The open
`metadata_lookup`/`alloc` implementation is a black-box connector model and
must not be run as a second allocator over a Store-managed pool extent.

## Portable tests

Run all four independently reportable T0 gates:

```bash
bash mooncake-common/tests/cxl_pool_backend/run_todo1_tests.sh
```

Run one gate while debugging:

```bash
bash mooncake-common/tests/cxl_pool_backend/run_todo1_tests.sh \
  cxl_pool_backend_failure_test
```

Each executable compiles only the endpoint and prints its tier plus explicit
`PASS`, `FAIL`, or `SKIP` status.

## `/dev/dax` lab gate

Use a dedicated, disposable, alignment-safe device-DAX subrange. This probe
writes 4 KiB and therefore requires an explicit destructive-test opt-in.

```bash
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_POOL_ID=rack0-pool0
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_DEV_SIZE=$((2 * 1024 * 1024))
export MC_CXL_MAP_OFFSET=0
export MC_CXL_MAP_ALIGNMENT=$((2 * 1024 * 1024))
export MC_CXL_ALLOC_ALIGNMENT=64
export MC_CXL_TEST_DESTRUCTIVE=1

bash mooncake-common/tests/cxl_pool_backend/run_todo1_tests.sh \
  cxl_pool_backend_devdax_test
```

Change `MC_CXL_MAP_OFFSET` and `MC_CXL_DEV_SIZE` to the dedicated region that
your confidential allocator or lab owner reserves. Never point this destructive
probe at a live pool containing metadata or KV data.

Do **not** pass `/dev/dax*` to Mooncake's existing `cxl_transport_test` or
`cxl_client_integration_test`: those are file-model tests that create and
truncate their configured path. Use only the explicitly gated devdax probe
above for the first device-DAX check.

## Mooncake startup alignment

The Store master and every client transfer endpoint must describe the same
reserved extent. For example, for a 16 GiB pool:

```bash
./build/mooncake-store/src/mooncake_master \
  --enable_cxl=true \
  --cxl_path=/dev/dax0.0 \
  --cxl_size=17179869184 \
  --allocation_strategy=cxl

export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_POOL_ID=rack0-pool0
export MC_CXL_DEV_PATH=/dev/dax0.0
export MC_CXL_DEV_SIZE=17179869184
```

The endpoint fails startup when the validated mapping capacity differs from
the Store allocator capacity. The old `MC_CXL_DEV_PATH` and
`MC_CXL_DEV_SIZE` variables remain supported. Set `MC_CXL_POOL_ID` explicitly
for stable rack/pool status; without it, the path is used only as a
backward-compatible identity fallback.
