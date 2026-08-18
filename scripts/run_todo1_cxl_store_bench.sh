#!/usr/bin/env bash
# Run a real single-node Mooncake Store -> file-backed CXL correctness and
# throughput probe. This is the T0 software gate: it does not claim device-DAX
# or CXL hardware bandwidth.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-todo1-cpu}"
pool_size="${CXL_BENCH_POOL_SIZE_BYTES:-8589934592}"
master_host="${CXL_BENCH_MASTER_HOST:-127.0.0.1}"
master_port="${CXL_BENCH_MASTER_PORT:-50051}"
metrics_port="${CXL_BENCH_METRICS_PORT:-19003}"
local_endpoint="${CXL_BENCH_LOCAL_ENDPOINT:-127.0.0.1:50071}"
num_objects="${CXL_BENCH_NUM_OBJECTS:-256}"
value_size="${CXL_BENCH_VALUE_SIZE:-1048576}"
batch_size="${CXL_BENCH_BATCH_SIZE:-8}"
local_buffer_size="${CXL_BENCH_LOCAL_BUFFER_SIZE:-268435456}"
startup_timeout="${CXL_BENCH_STARTUP_TIMEOUT_SEC:-20}"

fatal() {
  echo "[FATAL] $*" >&2
  exit 2
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  case "$value" in
    ""|*[!0-9]*|0) fatal "$name must be a positive decimal integer" ;;
  esac
}

require_port() {
  local name="$1"
  local value="$2"
  require_positive_integer "$name" "$value"
  [ "$value" -le 65535 ] || fatal "$name must be <= 65535"
}

port_is_open() {
  local host="$1"
  local port="$2"
  (exec 9<>"/dev/tcp/$host/$port") 2>/dev/null
}

case "$build_dir_name" in
  ""|/*|..|../*|*/../*|*/..) fatal "MOONCAKE_BUILD_DIR must stay inside $repo_dir" ;;
esac
[ "$(uname -s)" = "Linux" ] ||
  fatal "the staged Mooncake bindings and CXL Store benchmark require Linux"
require_positive_integer CXL_BENCH_POOL_SIZE_BYTES "$pool_size"
require_positive_integer CXL_BENCH_NUM_OBJECTS "$num_objects"
require_positive_integer CXL_BENCH_VALUE_SIZE "$value_size"
[ "$((value_size % 512))" -eq 0 ] ||
  fatal "CXL_BENCH_VALUE_SIZE must be 512-byte aligned"
require_positive_integer CXL_BENCH_BATCH_SIZE "$batch_size"
require_positive_integer CXL_BENCH_LOCAL_BUFFER_SIZE "$local_buffer_size"
require_positive_integer CXL_BENCH_STARTUP_TIMEOUT_SEC "$startup_timeout"
require_port CXL_BENCH_MASTER_PORT "$master_port"
require_port CXL_BENCH_METRICS_PORT "$metrics_port"
[ "$master_port" -ne "$metrics_port" ] ||
  fatal "master and metrics ports must differ"

package_root="$repo_dir/mooncake-wheel"
master_binary="$package_root/mooncake/mooncake_master"
benchmark="$repo_dir/mooncake-store/benchmarks/store_kv_bench.py"

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
[ -x "$master_binary" ] ||
  fatal "staged mooncake_master is missing: $master_binary; run bash scripts/bootstrap_todo1_lab.sh"
[ -f "$benchmark" ] || fatal "benchmark source is missing: $benchmark"
[ -f "$package_root/mooncake/store.so" ] ||
  fatal "staged mooncake.store extension is missing; run bash scripts/bootstrap_todo1_lab.sh"

python_path="$package_root"
if [ -n "${PYTHONPATH:-}" ]; then
  python_path="$python_path:$PYTHONPATH"
fi

echo "[preflight] python=$python_bin"
echo "[preflight] PYTHONPATH=$package_root"
PYTHONPATH="$python_path" "$python_bin" -c \
  'import mooncake.store as store; print("[PASS] import=mooncake.store module=" + store.__file__)'

port_is_open "$master_host" "$master_port" &&
  fatal "$master_host:$master_port is already in use; set CXL_BENCH_MASTER_PORT"
port_is_open "$master_host" "$metrics_port" &&
  fatal "$master_host:$metrics_port is already in use; set CXL_BENCH_METRICS_PORT"

owns_pool_file=0
if [ -n "${CXL_BENCH_POOL_FILE:-}" ]; then
  pool_file="$CXL_BENCH_POOL_FILE"
  case "$pool_file" in
    /dev/*) fatal "T0 file-backed benchmark must not target a device node" ;;
  esac
  if [ -e "$pool_file" ] && [ ! -f "$pool_file" ]; then
    fatal "CXL_BENCH_POOL_FILE must be a regular file"
  fi
  echo "[WARNING] truncating requested pool file: $pool_file"
else
  pool_file="$(mktemp /tmp/callosum-cxl-store-pool.XXXXXX)"
  owns_pool_file=1
fi
truncate -s "$pool_size" "$pool_file"

output_dir="${CXL_BENCH_OUTPUT_DIR:-}"
if [ -z "$output_dir" ]; then
  output_dir="$(mktemp -d /tmp/callosum-cxl-store-bench.XXXXXX)"
else
  mkdir -p "$output_dir"
fi
summary_json="$output_dir/summary.json"
master_log="$output_dir/mooncake-master.log"
benchmark_log="$output_dir/store-kv-bench.log"
master_pid=""

cleanup() {
  local status="$?"
  trap - EXIT INT TERM
  if [ -n "$master_pid" ] && kill -0 "$master_pid" 2>/dev/null; then
    kill -TERM "$master_pid" 2>/dev/null || true
    wait "$master_pid" 2>/dev/null || true
  fi
  if [ "$owns_pool_file" -eq 1 ] && [ -e "$pool_file" ]; then
    rm -f -- "$pool_file"
  fi
  if [ "$status" -ne 0 ]; then
    echo "[FAIL] benchmark exit=$status" >&2
    echo "[debug] master_log=$master_log" >&2
    echo "[debug] benchmark_log=$benchmark_log" >&2
    tail -80 "$master_log" >&2 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

echo "[preflight] tier=T0 provider=faketract backend=file"
echo "[preflight] pool_file=$pool_file pool_bytes=$pool_size"
echo "[preflight] master=$master_host:$master_port metrics_port=$metrics_port"
echo "[preflight] output_dir=$output_dir"

"$master_binary" \
  --rpc_port="$master_port" \
  --metrics_port="$metrics_port" \
  --enable_metric_reporting=false \
  --enable_cxl=true \
  --allocation_strategy=cxl \
  --cxl_path="$pool_file" \
  --cxl_size="$pool_size" \
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
  fatal "mooncake_master was not ready within ${startup_timeout}s; inspect $master_log"
echo "[PASS] master_ready pid=$master_pid endpoint=$master_host:$master_port"

benchmark_args=(
  --scenario verify_write
  --local-hostname "$local_endpoint"
  --metadata-server P2PHANDSHAKE
  --master-server "$master_host:$master_port"
  --protocol cxl
  --device-name "$pool_file"
  --global-segment-size 0
  --local-buffer-size "$local_buffer_size"
  --io-api plain
  --numjobs 1
  --iodepth 1
  --batch-size "$batch_size"
  --nr-objects "$num_objects"
  --value-size "$value_size"
  --key-prefix todo1cxl
  --memory-replica-num 1
  --verify
  --pattern 0xab
  --journal failures
  --output-dir "$output_dir"
  --summary-json "$summary_json"
)

echo "[benchmark] objects=$num_objects value_bytes=$value_size batch_size=$batch_size checksum=on"
set -o pipefail
MC_CXL_PROVIDER=faketract \
MC_CXL_BACKEND_KIND=file \
MC_CXL_POOL_ID="${MC_CXL_POOL_ID:-callosum-store-bench-pool}" \
MC_CXL_DEV_PATH="$pool_file" \
MC_CXL_DEV_SIZE="$pool_size" \
MOONCAKE_STORE_CHECKSUM=1 \
PYTHONPATH="$python_path" \
  "$python_bin" "$benchmark" "${benchmark_args[@]}" 2>&1 | tee "$benchmark_log"

PYTHONPATH="$python_path" "$python_bin" -c \
  'import json, sys; data=json.load(open(sys.argv[1], encoding="utf-8")); assert data["ok"], data; print("[PASS] summary_ok=true phases=" + ",".join(data["phases"]))' \
  "$summary_json"

echo "[PASS] Mooncake Store -> file-backed CXL Put/Get checksum validation"
echo "[result] summary_json=$summary_json"
echo "[result] benchmark_log=$benchmark_log"
echo "[result] master_log=$master_log"
