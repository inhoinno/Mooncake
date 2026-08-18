#!/usr/bin/env bash
# TODO #2: one Mooncake Master plus two native CXL Store clients.
#
# This is a strict ownership wrapper around the TODO1.5 matrix harness or the
# streaming WSS harness. Both clients map the complete shared pool. Each client
# tells the Master about one disjoint allocation-owned subrange.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
role="${1:-}"
test_mode="${TODO2_TEST_MODE:-matrix}"

fatal() {
  echo "[FATAL] $*" >&2
  exit 2
}

usage() {
  cat <<'EOF'
Usage: bash scripts/run_todo2_native_cxl.sh <master|node0|node1|unit>

Common hardware settings:
  MC_CXL_TEST_DESTRUCTIVE=1
  MC_CXL_BACKEND_KIND=devdax
  MC_CXL_DEV_PATH=/dev/daxX.Y
  MC_CXL_DEV_SIZE=<full shared-pool bytes>
  MC_CXL_POOL_ID=<same stable ID on both nodes>
  MOONCAKE_MASTER_ADDRESS=<master host:port>

Required on each client (not Master):
  MC_CXL_OWNED_OFFSET=<this client's offset within the full mapping>
  MC_CXL_OWNED_SIZE=<this client's allocation capacity>
  TODO2_RUN_ID=<same unique run ID on both nodes>
  TODO2_NODE_ID=<node0 or node1 diagnostic name>
  MOONCAKE_LOCAL_HOSTNAME=<unique peer-reachable host:port>

Example for a 1 GiB pool:
  node0: MC_CXL_OWNED_OFFSET=0         MC_CXL_OWNED_SIZE=536870912
  node1: MC_CXL_OWNED_OFFSET=536870912 MC_CXL_OWNED_SIZE=536870912
EOF
}

case "$role" in
  master|node0|node1|unit) ;;
  -h|--help|help) usage; exit 0 ;;
  "") usage; exit 2 ;;
  *) usage; fatal "unknown role: $role" ;;
esac

case "$test_mode" in
  matrix|wss) ;;
  *) fatal "TODO2_TEST_MODE must be matrix or wss" ;;
esac

if [ "$role" = "unit" ]; then
  build_dir_name="${MOONCAKE_BUILD_DIR:-build-todo1-cpu}"
  [ -f "$repo_dir/$build_dir_name/CTestTestfile.cmake" ] ||
    fatal "missing configured build $repo_dir/$build_dir_name"
  exec ctest --test-dir "$repo_dir/$build_dir_name" --output-on-failure \
    -L todo2
fi

export MC_CXL_PROVIDER=mooncake
export TODO15_MODE="$test_mode"
if [ "$test_mode" = "wss" ]; then
  export TODO15_LEASE_TTL="${TODO2_WSS_LEASE_TTL:-24h}"
fi

if [ "$role" != "master" ]; then
  : "${MC_CXL_DEV_SIZE:?set MC_CXL_DEV_SIZE to the full mapped capacity}"
  : "${MC_CXL_OWNED_OFFSET:?set this client's allocation-owned offset}"
  : "${MC_CXL_OWNED_SIZE:?set this client's allocation-owned capacity}"
  for value_name in MC_CXL_DEV_SIZE MC_CXL_OWNED_OFFSET MC_CXL_OWNED_SIZE; do
    value="${!value_name}"
    case "$value" in
      ""|*[!0-9]*) fatal "$value_name must be a non-negative decimal integer" ;;
    esac
  done
  [ "$MC_CXL_DEV_SIZE" -gt 0 ] || fatal "MC_CXL_DEV_SIZE must be positive"
  [ "$MC_CXL_OWNED_SIZE" -gt 0 ] || fatal "MC_CXL_OWNED_SIZE must be positive"

  # CacheLib's pinned Slab::kSize is 2^24 bytes in this source tree. Keep the
  # shell preflight equal to SegmentManager's C++ ownership validation.
  slab_size=16777216
  [ $((MC_CXL_OWNED_OFFSET % slab_size)) -eq 0 ] ||
    fatal "MC_CXL_OWNED_OFFSET must be 16 MiB aligned"
  [ $((MC_CXL_OWNED_SIZE % slab_size)) -eq 0 ] ||
    fatal "MC_CXL_OWNED_SIZE must be 16 MiB aligned"
  [ "$MC_CXL_OWNED_OFFSET" -le "$MC_CXL_DEV_SIZE" ] ||
    fatal "owned offset exceeds the full shared pool"
  [ "$MC_CXL_OWNED_SIZE" -le \
      $((MC_CXL_DEV_SIZE - MC_CXL_OWNED_OFFSET)) ] ||
    fatal "owned extent exceeds the full shared pool"

  : "${TODO2_RUN_ID:?set the same TODO2_RUN_ID on both clients}"
  : "${TODO2_NODE_ID:?set TODO2_NODE_ID for this client}"
  export TODO15_RUN_ID="$TODO2_RUN_ID"
  export TODO15_NODE_ID="$TODO2_NODE_ID"
  export TODO15_OUTPUT_DIR="${TODO2_OUTPUT_DIR:-/tmp/callosum-todo2}"
  if [ "$test_mode" = "wss" ]; then
    export TODO15_TIMEOUT_SEC="${TODO2_WSS_TIMEOUT_SEC:-86400}"
    export TODO15_POLL_MS="${TODO2_WSS_POLL_MS:-250}"
    export TODO15_WSS_BYTES="${TODO2_WSS_BYTES:-536870912000}"
    export TODO15_WSS_HEADROOM_BYTES="${TODO2_WSS_HEADROOM_BYTES:-8589934592}"
    export TODO15_WSS_PROGRESS_BYTES="${TODO2_WSS_PROGRESS_BYTES:-1073741824}"
    export TODO15_COMPONENT=todo2_cxl_wss
    export TODO15_TIER=T2_NATIVE_SHARED_CXL_WSS
  else
    export TODO15_TIMEOUT_SEC="${TODO2_TIMEOUT_SEC:-180}"
    export TODO15_POLL_MS="${TODO2_POLL_MS:-100}"
    export TODO15_COMPONENT=todo2_native_cxl
    export TODO15_TIER=T2_NATIVE_SHARED_CXL
  fi
  export TODO15_CLEANUP="${TODO2_CLEANUP:-1}"

  echo "[preflight] milestone=TODO2 provider=mooncake role=$role mode=$test_mode"
  echo "[preflight] mapped_capacity=$MC_CXL_DEV_SIZE pool_id=${MC_CXL_POOL_ID:-unset}"
  echo "[preflight] owned_offset=$MC_CXL_OWNED_OFFSET owned_capacity=$MC_CXL_OWNED_SIZE"
fi

exec bash "$repo_dir/scripts/run_todo15_shared_cxl.sh" "$role"
