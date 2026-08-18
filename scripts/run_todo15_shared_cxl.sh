#!/usr/bin/env bash
# Launch one role of the TODO1.5 two-node Mooncake shared-CXL test.
#
# This script intentionally keeps the Master in the foreground and never kills
# system-wide processes.  Run master, node1, and node0 in separate terminals.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
role="${1:-}"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-todo1-cpu}"

fatal() {
  echo "[FATAL] $*" >&2
  exit 2
}

usage() {
  cat <<'EOF'
Usage: bash scripts/run_todo15_shared_cxl.sh <master|node0|node1|unit>

Shared settings on both nodes:
  MC_CXL_TEST_DESTRUCTIVE=1
  MC_CXL_BACKEND_KIND=devdax
  MC_CXL_DEV_PATH=/dev/daxX.Y       # local device path may differ
  MC_CXL_DEV_SIZE=<mapped bytes>     # must match on both nodes and Master
  MC_CXL_MAP_OFFSET=0                # must match on both nodes
  MC_CXL_POOL_ID=<stable pool id>    # must match on both nodes
  MOONCAKE_MASTER_ADDRESS=<host:port>

Client-only settings:
  TODO15_RUN_ID=<unique id>          # identical on node0 and node1
  TODO15_NODE_ID=<diagnostic node id>
  MOONCAKE_LOCAL_HOSTNAME=<host:port># unique and peer-reachable

Optional:
  TODO15_TIMEOUT_SEC=180 TODO15_POLL_MS=100 TODO15_CLEANUP=1
  TODO15_OUTPUT_DIR=/tmp/callosum-todo15
  TODO15_LOCAL_BUFFER_SIZE=268435456
  TODO15_MODE=matrix|wss TODO15_LEASE_TTL=1h
EOF
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  case "$value" in
    ""|*[!0-9]*|0) fatal "$name must be a positive decimal integer" ;;
  esac
}

require_nonnegative_integer() {
  local name="$1"
  local value="$2"
  case "$value" in
    ""|*[!0-9]*) fatal "$name must be a non-negative decimal integer" ;;
  esac
}

parse_host_port() {
  local name="$1"
  local endpoint="$2"
  [[ "$endpoint" == *:* ]] || fatal "$name must be host:port"
  parsed_host="${endpoint%:*}"
  parsed_port="${endpoint##*:}"
  [ -n "$parsed_host" ] || fatal "$name host must not be empty"
  require_positive_integer "$name port" "$parsed_port"
  [ "$parsed_port" -le 65535 ] || fatal "$name port must be <= 65535"
}

port_is_open() {
  local host="$1"
  local port="$2"
  (exec 9<>"/dev/tcp/$host/$port") 2>/dev/null
}

select_python() {
  if [ -n "${PYTHON_BIN:-}" ]; then
    python_bin="$PYTHON_BIN"
  elif [ -x "$repo_dir/$build_dir_name/.venv/bin/python" ]; then
    python_bin="$repo_dir/$build_dir_name/.venv/bin/python"
  else
    python_bin="$(command -v python3 || true)"
  fi
  if [ -n "$python_bin" ] && [[ "$python_bin" != */* ]]; then
    python_bin="$(command -v "$python_bin" || true)"
  fi
  [ -n "$python_bin" ] && [ -x "$python_bin" ] ||
    fatal "Python 3 is unavailable; run bash scripts/bootstrap_todo1_lab.sh"
}

case "$role" in
  unit)
    select_python
    echo "[preflight] tier=T0 test=TODO1.5_protocol python=$python_bin"
    cd "$repo_dir/scripts"
    exec "$python_bin" -m unittest -v \
      test_todo15_shared_cxl.py
    ;;
  master|node0|node1) ;;
  -h|--help|help|"") usage; [ -n "$role" ] && exit 0 || exit 2 ;;
  *) usage; fatal "unknown role: $role" ;;
esac

[ "$(uname -s)" = "Linux" ] || fatal "shared-CXL execution requires Linux"
case "$build_dir_name" in
  ""|/*|..|../*|*/../*|*/..) fatal "MOONCAKE_BUILD_DIR must stay inside $repo_dir" ;;
esac

: "${MC_CXL_TEST_DESTRUCTIVE:?set MC_CXL_TEST_DESTRUCTIVE=1}"
: "${MC_CXL_BACKEND_KIND:?set MC_CXL_BACKEND_KIND=devdax}"
: "${MC_CXL_DEV_PATH:?set MC_CXL_DEV_PATH to the local /dev/dax device}"
: "${MC_CXL_DEV_SIZE:?set MC_CXL_DEV_SIZE to the mapped capacity}"
: "${MC_CXL_POOL_ID:?set one stable MC_CXL_POOL_ID on both nodes}"
: "${MOONCAKE_MASTER_ADDRESS:?set MOONCAKE_MASTER_ADDRESS=host:port}"
export MC_CXL_MAP_OFFSET="${MC_CXL_MAP_OFFSET:-0}"

[ "$MC_CXL_TEST_DESTRUCTIVE" = "1" ] ||
  fatal "MC_CXL_TEST_DESTRUCTIVE must equal 1"
[ "$MC_CXL_BACKEND_KIND" = "devdax" ] ||
  fatal "TODO1.5 accepts only MC_CXL_BACKEND_KIND=devdax"
case "$MC_CXL_DEV_PATH" in
  /dev/dax*) ;;
  *) fatal "MC_CXL_DEV_PATH must be an explicit /dev/dax device" ;;
esac
[ -c "$MC_CXL_DEV_PATH" ] || fatal "$MC_CXL_DEV_PATH is not a character device"
[ -r "$MC_CXL_DEV_PATH" ] && [ -w "$MC_CXL_DEV_PATH" ] ||
  fatal "$MC_CXL_DEV_PATH must be readable and writable by $(id -un)"
require_positive_integer MC_CXL_DEV_SIZE "$MC_CXL_DEV_SIZE"
require_nonnegative_integer MC_CXL_MAP_OFFSET "$MC_CXL_MAP_OFFSET"
parse_host_port MOONCAKE_MASTER_ADDRESS "$MOONCAKE_MASTER_ADDRESS"
master_host="$parsed_host"
master_port="$parsed_port"

package_root="$repo_dir/mooncake-wheel"
master_binary="$package_root/mooncake/mooncake_master"
test_mode="${TODO15_MODE:-matrix}"
case "$test_mode" in
  matrix) harness="$repo_dir/scripts/todo15_shared_cxl_stage1.py" ;;
  wss) harness="$repo_dir/scripts/todo2_cxl_wss.py" ;;
  *) fatal "TODO15_MODE must be matrix or wss" ;;
esac
[ -f "$harness" ] || fatal "missing harness: $harness"

export MC_CXL_PROVIDER="${MC_CXL_PROVIDER:-faketract}"
export MOONCAKE_STORE_CHECKSUM=1

echo "[preflight] milestone=TODO1.5 role=$role tier=T2_SHARED_CXL mode=$test_mode"
echo "[preflight] provider=$MC_CXL_PROVIDER backend=$MC_CXL_BACKEND_KIND"
echo "[preflight] pool_id=$MC_CXL_POOL_ID capacity=$MC_CXL_DEV_SIZE map_offset=$MC_CXL_MAP_OFFSET device=$MC_CXL_DEV_PATH"
echo "[preflight] master=$MOONCAKE_MASTER_ADDRESS"

if [ "$role" = "master" ]; then
  [ -x "$master_binary" ] ||
    fatal "missing staged mooncake_master: $master_binary; run bootstrap_todo1_lab.sh"
  metrics_port="${TODO15_METRICS_PORT:-19003}"
  require_positive_integer TODO15_METRICS_PORT "$metrics_port"
  [ "$metrics_port" -le 65535 ] || fatal "TODO15_METRICS_PORT must be <= 65535"
  [ "$metrics_port" -ne "$master_port" ] ||
    fatal "TODO15_METRICS_PORT and Master RPC port must differ"
  lease_ttl="${TODO15_LEASE_TTL:-1h}"
  [[ "$lease_ttl" =~ ^[1-9][0-9]*(ms|s|m|h)$ ]] ||
    fatal "TODO15_LEASE_TTL must be a positive duration such as 1h or 24h"
  port_is_open "$master_host" "$master_port" &&
    fatal "$MOONCAKE_MASTER_ADDRESS is already in use"
  port_is_open "$master_host" "$metrics_port" &&
    fatal "$master_host:$metrics_port is already in use"
  echo "[launch] Mooncake Master remains in the foreground; stop it with Ctrl-C"
  exec "$master_binary" \
    --rpc_port="$master_port" \
    --metrics_port="$metrics_port" \
    --enable_metric_reporting=false \
    --enable_cxl=true \
    --allocation_strategy=cxl \
    --cxl_path="$MC_CXL_DEV_PATH" \
    --cxl_size="$MC_CXL_DEV_SIZE" \
    --default_kv_lease_ttl="$lease_ttl"
fi

: "${TODO15_RUN_ID:?set the same unique TODO15_RUN_ID on node0 and node1}"
: "${TODO15_NODE_ID:?set TODO15_NODE_ID to a stable diagnostic node name}"
: "${MOONCAKE_LOCAL_HOSTNAME:?set a unique, peer-reachable host:port per node}"
[[ "$TODO15_RUN_ID" =~ ^[A-Za-z0-9._-]{1,96}$ ]] ||
  fatal "TODO15_RUN_ID contains unsupported characters or exceeds 96 characters"
parse_host_port MOONCAKE_LOCAL_HOSTNAME "$MOONCAKE_LOCAL_HOSTNAME"
local_host="$parsed_host"
local_port="$parsed_port"
[ "$MOONCAKE_LOCAL_HOSTNAME" != "$MOONCAKE_MASTER_ADDRESS" ] ||
  fatal "MOONCAKE_LOCAL_HOSTNAME must differ from the Master endpoint"
port_is_open "$master_host" "$master_port" ||
  fatal "Mooncake Master is unreachable at $MOONCAKE_MASTER_ADDRESS"

select_python
[ -f "$package_root/mooncake/store.so" ] ||
  fatal "missing staged mooncake.store; run bash scripts/bootstrap_todo1_lab.sh"
python_path="$package_root"
if [ -n "${PYTHONPATH:-}" ]; then
  python_path="$python_path:$PYTHONPATH"
fi
PYTHONPATH="$python_path" "$python_bin" -c \
  'import mooncake.store as store; print("[PASS] import=mooncake.store module=" + store.__file__)'

timeout_sec="${TODO15_TIMEOUT_SEC:-180}"
poll_ms="${TODO15_POLL_MS:-100}"
local_buffer_size="${TODO15_LOCAL_BUFFER_SIZE:-268435456}"
cleanup="${TODO15_CLEANUP:-1}"
require_positive_integer TODO15_POLL_MS "$poll_ms"
require_positive_integer TODO15_LOCAL_BUFFER_SIZE "$local_buffer_size"
case "$timeout_sec" in
  ""|*[!0-9.]*) fatal "TODO15_TIMEOUT_SEC must be a positive number" ;;
esac
case "$cleanup" in 0|1) ;; *) fatal "TODO15_CLEANUP must be 0 or 1" ;; esac

output_dir="${TODO15_OUTPUT_DIR:-/tmp/callosum-todo15}"
mkdir -p "$output_dir"
summary_json="$output_dir/${TODO15_RUN_ID}-${role}.json"
args=(
  --role "$role"
  --run-id "$TODO15_RUN_ID"
  --node-id "$TODO15_NODE_ID"
  --pool-id "$MC_CXL_POOL_ID"
  --device-name "$MC_CXL_DEV_PATH"
  --capacity "$MC_CXL_DEV_SIZE"
  --local-hostname "$MOONCAKE_LOCAL_HOSTNAME"
  --master-server "$MOONCAKE_MASTER_ADDRESS"
  --component "${TODO15_COMPONENT:-todo15_shared_cxl}"
  --tier "${TODO15_TIER:-T2_SHARED_CXL}"
  --metadata-server P2PHANDSHAKE
  --global-segment-size 0
  --local-buffer-size "$local_buffer_size"
  --mapping-offset "$MC_CXL_MAP_OFFSET"
  --timeout-sec "$timeout_sec"
  --poll-ms "$poll_ms"
  --summary-json "$summary_json"
)
if [ "$test_mode" = "wss" ]; then
  wss_bytes="${TODO15_WSS_BYTES:-536870912000}"
  headroom_bytes="${TODO15_WSS_HEADROOM_BYTES:-8589934592}"
  progress_bytes="${TODO15_WSS_PROGRESS_BYTES:-1073741824}"
  require_positive_integer TODO15_WSS_BYTES "$wss_bytes"
  require_nonnegative_integer TODO15_WSS_HEADROOM_BYTES "$headroom_bytes"
  require_positive_integer TODO15_WSS_PROGRESS_BYTES "$progress_bytes"
  args+=(
    --wss-bytes "$wss_bytes"
    --headroom-bytes "$headroom_bytes"
    --progress-bytes "$progress_bytes"
  )
fi
if [ "$role" = "node0" ] && [ "$cleanup" = "1" ]; then
  args+=(--cleanup)
fi

echo "[launch] node_id=$TODO15_NODE_ID local_endpoint=$MOONCAKE_LOCAL_HOSTNAME"
echo "[launch] run_id=$TODO15_RUN_ID summary=$summary_json cleanup=$cleanup"
PYTHONPATH="$python_path" exec "$python_bin" "$harness" "${args[@]}"
