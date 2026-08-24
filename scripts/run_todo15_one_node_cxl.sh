#!/usr/bin/env bash
# Run one Mooncake Master plus one native Mooncake CXL Store client on a
# dedicated device-DAX mapping. The client owns the complete allocation range.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-run}"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-todo1-cpu}"

fatal() {
  echo "[FATAL] $*" >&2
  exit 2
}

usage() {
  cat <<'EOF'
Usage: bash scripts/run_todo15_one_node_cxl.sh [run|preflight|unit]

Required for run/preflight:
  MC_CXL_TEST_DESTRUCTIVE=1
  MC_CXL_DEV_PATH=/dev/daxX.Y
  MC_CXL_DEV_SIZE=<mapped bytes>      # >=64 MiB, 16 MiB aligned
  MC_CXL_POOL_ID=<stable logical ID>

Optional:
  MOONCAKE_MASTER_ADDRESS=127.0.0.1:50051
  TODO15_ONE_LOCAL_ENDPOINT=127.0.0.1:50071
  TODO15_ONE_METRICS_PORT=19003
  TODO15_ONE_LOCAL_BUFFER_SIZE=268435456
  TODO15_ONE_RUN_ID=todo15-one-<unique>
  TODO15_ONE_OUTPUT_DIR=/tmp/callosum-todo15-one/<run-id>
  TODO15_ONE_STARTUP_TIMEOUT_SEC=20
EOF
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  case "$value" in
    ""|*[!0-9]*|0) fatal "$name must be a positive decimal integer" ;;
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

case "$mode" in
  unit)
    select_python
    cd "$repo_dir/scripts"
    exec "$python_bin" -m unittest -v test_todo15_one_node_cxl.py
    ;;
  run|preflight) ;;
  -h|--help|help) usage; exit 0 ;;
  *) usage; fatal "unknown mode: $mode" ;;
esac

[ "$(uname -s)" = "Linux" ] || fatal "native device-DAX execution requires Linux"
case "$build_dir_name" in
  ""|/*|..|../*|*/../*|*/..) fatal "MOONCAKE_BUILD_DIR must stay inside $repo_dir" ;;
esac

: "${MC_CXL_TEST_DESTRUCTIVE:?set MC_CXL_TEST_DESTRUCTIVE=1}"
: "${MC_CXL_DEV_PATH:?set MC_CXL_DEV_PATH=/dev/daxX.Y}"
: "${MC_CXL_DEV_SIZE:?set MC_CXL_DEV_SIZE to the mapped capacity}"
: "${MC_CXL_POOL_ID:?set MC_CXL_POOL_ID to a stable logical pool ID}"
[ "$MC_CXL_TEST_DESTRUCTIVE" = "1" ] ||
  fatal "MC_CXL_TEST_DESTRUCTIVE must equal 1"
case "$MC_CXL_DEV_PATH" in
  /dev/dax*) ;;
  *) fatal "MC_CXL_DEV_PATH must be an explicit /dev/dax device" ;;
esac
[ -c "$MC_CXL_DEV_PATH" ] || fatal "$MC_CXL_DEV_PATH is not a character device"
[ -r "$MC_CXL_DEV_PATH" ] && [ -w "$MC_CXL_DEV_PATH" ] ||
  fatal "$MC_CXL_DEV_PATH must be readable and writable by $(id -un)"
require_positive_integer MC_CXL_DEV_SIZE "$MC_CXL_DEV_SIZE"
slab_size=16777216
[ $((MC_CXL_DEV_SIZE % slab_size)) -eq 0 ] ||
  fatal "MC_CXL_DEV_SIZE must be 16 MiB aligned for the Master CacheLib allocator"
[ "$MC_CXL_DEV_SIZE" -ge 67108864 ] ||
  fatal "MC_CXL_DEV_SIZE must be at least 64 MiB for the four allocation classes"

master_address="${MOONCAKE_MASTER_ADDRESS:-127.0.0.1:50051}"
local_endpoint="${TODO15_ONE_LOCAL_ENDPOINT:-127.0.0.1:50071}"
metrics_port="${TODO15_ONE_METRICS_PORT:-19003}"
local_buffer_size="${TODO15_ONE_LOCAL_BUFFER_SIZE:-268435456}"
startup_timeout="${TODO15_ONE_STARTUP_TIMEOUT_SEC:-20}"
run_id="${TODO15_ONE_RUN_ID:-todo15-one-$$}"
[[ "$run_id" =~ ^[A-Za-z0-9._-]{1,96}$ ]] ||
  fatal "TODO15_ONE_RUN_ID contains unsupported characters or exceeds 96 characters"
parse_host_port MOONCAKE_MASTER_ADDRESS "$master_address"
master_host="$parsed_host"
master_port="$parsed_port"
parse_host_port TODO15_ONE_LOCAL_ENDPOINT "$local_endpoint"
[ "$local_endpoint" != "$master_address" ] ||
  fatal "TODO15_ONE_LOCAL_ENDPOINT must differ from the Master endpoint"
require_positive_integer TODO15_ONE_METRICS_PORT "$metrics_port"
[ "$metrics_port" -le 65535 ] || fatal "TODO15_ONE_METRICS_PORT must be <= 65535"
[ "$metrics_port" -ne "$master_port" ] ||
  fatal "TODO15_ONE_METRICS_PORT and Master RPC port must differ"
require_positive_integer TODO15_ONE_LOCAL_BUFFER_SIZE "$local_buffer_size"
require_positive_integer TODO15_ONE_STARTUP_TIMEOUT_SEC "$startup_timeout"

select_python
package_root="$repo_dir/mooncake-wheel"
master_binary="$package_root/mooncake/mooncake_master"
harness="$repo_dir/scripts/todo15_one_node_cxl.py"
[ -x "$master_binary" ] ||
  fatal "missing staged mooncake_master: $master_binary; run bootstrap_todo1_lab.sh"
[ -f "$package_root/mooncake/store.so" ] ||
  fatal "missing staged mooncake.store; run bootstrap_todo1_lab.sh"
[ -f "$harness" ] || fatal "missing one-node harness: $harness"

export MC_CXL_PROVIDER=mooncake
export MC_CXL_BACKEND_KIND=devdax
export MC_CXL_MAP_OFFSET=0
export MC_CXL_OWNED_OFFSET=0
export MC_CXL_OWNED_SIZE="$MC_CXL_DEV_SIZE"
export MOONCAKE_STORE_CHECKSUM=1

python_path="$package_root"
if [ -n "${PYTHONPATH:-}" ]; then
  python_path="$python_path:$PYTHONPATH"
fi

echo "[preflight] milestone=TODO1.5-one-node tier=T2_NATIVE_CXL"
echo "[preflight] provider=$MC_CXL_PROVIDER backend=$MC_CXL_BACKEND_KIND"
echo "[preflight] pool_id=$MC_CXL_POOL_ID capacity=$MC_CXL_DEV_SIZE device=$MC_CXL_DEV_PATH"
echo "[preflight] mapped_offset=0 owned_offset=0 owned_capacity=$MC_CXL_OWNED_SIZE"
echo "[preflight] master=$master_address client=$local_endpoint"
PYTHONPATH="$python_path" "$python_bin" -c \
  'import mooncake.store as store; print("[PASS] import=mooncake.store module=" + store.__file__)'

if [ "$mode" = "preflight" ]; then
  echo "[PASS] native one-node preflight completed; device was not mapped or written"
  exit 0
fi

port_is_open "$master_host" "$master_port" &&
  fatal "$master_address is already in use"
port_is_open "$master_host" "$metrics_port" &&
  fatal "$master_host:$metrics_port is already in use"

output_dir="${TODO15_ONE_OUTPUT_DIR:-/tmp/callosum-todo15-one/$run_id}"
mkdir -p "$output_dir"
summary_json="$output_dir/summary.json"
master_log="$output_dir/mooncake-master.log"
client_log="$output_dir/store-client.log"
master_pid=""

cleanup() {
  local status="$?"
  trap - EXIT INT TERM
  if [ -n "$master_pid" ] && kill -0 "$master_pid" 2>/dev/null; then
    kill -TERM "$master_pid" 2>/dev/null || true
    wait "$master_pid" 2>/dev/null || true
  fi
  if [ "$status" -ne 0 ]; then
    echo "[FAIL] one-node native CXL gate exit=$status" >&2
    echo "[debug] master_log=$master_log" >&2
    echo "[debug] client_log=$client_log" >&2
    tail -80 "$master_log" >&2 2>/dev/null || true
    tail -80 "$client_log" >&2 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

"$master_binary" \
  --rpc_port="$master_port" \
  --metrics_port="$metrics_port" \
  --enable_metric_reporting=false \
  --enable_cxl=true \
  --allocation_strategy=cxl \
  --cxl_path="$MC_CXL_DEV_PATH" \
  --cxl_size="$MC_CXL_DEV_SIZE" \
  --default_kv_lease_ttl=1h \
  >"$master_log" 2>&1 &
master_pid=$!

ready=0
for ((attempt = 0; attempt < startup_timeout * 5; ++attempt)); do
  if ! kill -0 "$master_pid" 2>/dev/null; then
    wait "$master_pid" || true
    fatal "mooncake_master exited before becoming ready; inspect $master_log"
  fi
  if port_is_open "$master_host" "$master_port"; then
    ready=1
    break
  fi
  sleep 0.2
done
[ "$ready" -eq 1 ] ||
  fatal "mooncake_master was not ready within ${startup_timeout}s"
echo "[PASS] master_ready pid=$master_pid endpoint=$master_address"

args=(
  --run-id "$run_id"
  --pool-id "$MC_CXL_POOL_ID"
  --device-name "$MC_CXL_DEV_PATH"
  --capacity "$MC_CXL_DEV_SIZE"
  --local-hostname "$local_endpoint"
  --master-server "$master_address"
  --metadata-server P2PHANDSHAKE
  --global-segment-size 0
  --local-buffer-size "$local_buffer_size"
  --mapping-offset 0
  --owned-offset 0
  --owned-capacity "$MC_CXL_DEV_SIZE"
  --summary-json "$summary_json"
)

set -o pipefail
PYTHONPATH="$python_path" \
  "$python_bin" "$harness" "${args[@]}" 2>&1 | tee "$client_log"

PYTHONPATH="$python_path" "$python_bin" -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    data = json.load(handle)
assert data["status"] == "PASS", data
assert data["objects_put"] == data["objects_get"] == 8, data
assert data["checksums_verified"] == 8, data
print("[PASS] summary_ok=true objects=8 exact_bytes=true")
' "$summary_json"

echo "[PASS] one-node Mooncake Store native CXL PUT/GET matrix"
echo "[result] summary_json=$summary_json"
echo "[result] client_log=$client_log"
echo "[result] master_log=$master_log"
