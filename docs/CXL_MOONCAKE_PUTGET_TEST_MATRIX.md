# Mooncake CXL Put/Get test matrix

This index separates software emulation, the small two-node correctness gate,
the native ownership gate, and the large live-working-set gate. A PASS at one
row does not imply a PASS at a later row.

| Gate | Real path | Required sizes/APIs | Launcher |
|---|---|---|---|
| Legacy one-node mini-test | private legacy Master + Store + file-backed FakeTraCT CXL mapping | 4 KiB, 64 KiB, 1 MiB, 16 MiB; single and batch | `scripts/run_todo1_cxl_store_matrix.sh` |
| Two-node shared-CXL mini-test | one Master + two FakeTraCT-backed clients over one shared pool | same four sizes; Node 0 single/batch to Node 1, then reverse | `scripts/run_todo15_shared_cxl.sh` |
| Two-node native mini-test | one Master owns metadata/alloc/free; two mapping-only native CXL clients own disjoint partitions | same four sizes; Node 0 single/batch to Node 1, then reverse | `scripts/run_todo2_native_cxl.sh` |
| Two-node native 500 GiB WSS | native topology; Node 0 retains all objects before Node 1 reads | same four sizes; single Put and peer single Get only | `scripts/run_todo2_cxl_wss.sh` |

Portable unit gates are available as:

```bash
bash scripts/run_todo15_shared_cxl.sh unit
bash scripts/run_todo2_native_cxl.sh unit
bash scripts/run_todo2_cxl_wss.sh unit
```

The first row can execute on one Linux host with a regular file. The remaining
hardware rows require two physical nodes mapping the same dedicated coherent
CXL extent. For both two-node mini-tests, start `master`, then `node1`, then
`node0`. The WSS gate uses the same order and native ownership settings.

All gates use deterministic payloads and correctness verification. The
two-node Python protocols additionally calculate SHA-256 and compare exact
bytes. Structured summaries report the exact API phases, sizes, byte counts,
and terminal status without payloads, process pointers, CXL virtual addresses,
or transport keys.
