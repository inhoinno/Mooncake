#!/usr/bin/env bash
# Launch the TODO #2 one-way single-Put/peer-Get CXL working-set gate.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
role="${1:-}"

usage() {
  cat <<'EOF'
Usage: bash scripts/run_todo2_cxl_wss.sh <master|node0|node1|unit>

This reuses the native TODO #2 topology and changes only the payload protocol:
  node0: single Put until the complete WSS is live
  node1: peer single Get with SHA-256 and exact-byte verification

Use the same MC_CXL_*, TODO2_RUN_ID, TODO2_NODE_ID,
MOONCAKE_MASTER_ADDRESS, and MOONCAKE_LOCAL_HOSTNAME settings documented by
run_todo2_native_cxl.sh. Defaults:
  TODO2_WSS_BYTES=536870912000          # 500 GiB, not decimal 500 GB
  TODO2_WSS_HEADROOM_BYTES=8589934592   # writer needs WSS + 8 GiB
  TODO2_WSS_PROGRESS_BYTES=1073741824   # one JSON progress event per GiB
  TODO2_WSS_TIMEOUT_SEC=86400
  TODO2_WSS_LEASE_TTL=24h
  TODO2_CLEANUP=1

Decimal 500 GB is not 4 KiB aligned. The smallest aligned target at or above
it is TODO2_WSS_BYTES=500000002048. Every target must be large enough to
include all four sizes.
EOF
}

case "$role" in
  unit)
    cd "$repo_dir/scripts"
    exec "${PYTHON_BIN:-python3}" -m unittest -v test_todo2_cxl_wss.py
    ;;
  master|node0|node1) ;;
  -h|--help|help) usage; exit 0 ;;
  "") usage; exit 2 ;;
  *) usage; echo "[FATAL] unknown role: $role" >&2; exit 2 ;;
esac

export TODO2_TEST_MODE=wss
exec bash "$repo_dir/scripts/run_todo2_native_cxl.sh" "$role"
