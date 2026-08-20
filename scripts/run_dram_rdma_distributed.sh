#!/usr/bin/env bash
# TODO#Extra: N distributed Mooncake RDMA source clients -> one GPU consumer.
# All roles use one Mooncake Master and one HTTP Transfer Engine metadata store.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
role="${1:-}"

fatal() { echo "[FATAL] $*" >&2; exit 2; }
usage() {
  cat <<'EOF'
Usage: bash scripts/run_dram_rdma_distributed.sh <master|source|prep|consumer>

Shared:
  TODOEXTRA_MASTER_ADDRESS=192.168.3.43:50051
  TODOEXTRA_METADATA_URL=http://192.168.3.43:8080/metadata
  TODOEXTRA_KEY=todoextra-8g TODOEXTRA_BLOCK_GIB=8
  RDMA_DEVICE_NAME=<Mooncake RDMA device; blank enables auto discovery>

source (run on every DRAM source node):
  TODOEXTRA_LOCAL_IP=192.168.3.44 TODOEXTRA_SOURCE_COUNT=4
  TODOEXTRA_SOURCE_BASE_PORT=50200 TODOEXTRA_SEGMENT_GIB=8

prep/consumer (run on the GPU consumer node):
  TODOEXTRA_LOCAL_IP=192.168.3.43 TODOEXTRA_EXPECT_SOURCES=4
  TODOEXTRA_ITERATIONS=3 TODOEXTRA_GPU_ID=0 TODOEXTRA_WITH_GDR=1
EOF
}

case "$role" in master|source|prep|consumer) ;; *) usage; exit 2 ;; esac
[ "$(uname -s)" = Linux ] || fatal "requires Linux"

build_dir_name="${MOONCAKE_BUILD_DIR:-build-gpu-multipath}"
if [ -n "${PYTHON_BIN:-}" ]; then python_bin="$PYTHON_BIN"
elif [ -x "$repo_dir/$build_dir_name/.venv/bin/python" ]; then python_bin="$repo_dir/$build_dir_name/.venv/bin/python"
else python_bin="$(command -v python3)"; fi
pkg_root="$repo_dir/mooncake-wheel"
export PYTHONPATH="$pkg_root${PYTHONPATH:+:$PYTHONPATH}"
master_bin="$pkg_root/mooncake/mooncake_master"
meta_py="$pkg_root/mooncake/http_metadata_server.py"
[ -f "$pkg_root/mooncake/store.so" ] || fatal "missing CUDA-built mooncake/store.so"

master_address="${TODOEXTRA_MASTER_ADDRESS:-192.168.3.43:50051}"
master_host="${master_address%:*}"; master_port="${master_address##*:}"
metadata_url="${TODOEXTRA_METADATA_URL:-http://$master_host:8080/metadata}"
metadata_port="${TODOEXTRA_METADATA_PORT:-8080}"
local_ip="${TODOEXTRA_LOCAL_IP:-}"
device_name="${RDMA_DEVICE_NAME:-}"
key="${TODOEXTRA_KEY:-todoextra-8g}"
block_gib="${TODOEXTRA_BLOCK_GIB:-8}"
block_bytes=$((block_gib * 1024 * 1024 * 1024))
segment_gib="${TODOEXTRA_SEGMENT_GIB:-8}"
segment_bytes=$((segment_gib * 1024 * 1024 * 1024))
out_dir="${TODOEXTRA_OUT_DIR:-/tmp/todoextra-rdma}"
mkdir -p "$out_dir"

common=(--store-module mooncake.store --master-server "$master_address"
        --metadata-server "$metadata_url" --device-name "$device_name"
        --key "$key" --block-bytes "$block_bytes")

if [ "$role" = master ]; then
  [ -x "$master_bin" ] && [ -f "$meta_py" ] || fatal "missing master/metadata artifacts"
  meta_pid=""
  cleanup() { [ -n "$meta_pid" ] && kill "$meta_pid" 2>/dev/null || true; }
  trap cleanup EXIT
  "$python_bin" "$meta_py" --host 0.0.0.0 --port "$metadata_port" \
    >"$out_dir/metadata.log" 2>&1 & meta_pid=$!
  sleep 1
  echo "[todoextra] Master=$master_address metadata=$metadata_url"
  "$master_bin" --rpc_address=0.0.0.0 --rpc_port="$master_port" \
    --metrics_port="${TODOEXTRA_METRICS_PORT:-19004}" \
    --enable_metric_reporting=false --default_kv_lease_ttl="${TODOEXTRA_LEASE_TTL:-24h}"
  exit $?
fi

[ -n "$local_ip" ] || fatal "set TODOEXTRA_LOCAL_IP to this node's peer-reachable IP"

if [ "$role" = source ]; then
  source_count="${TODOEXTRA_SOURCE_COUNT:-1}"
  base_port="${TODOEXTRA_SOURCE_BASE_PORT:-50200}"
  pids=()
  cleanup() { for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done; }
  trap cleanup EXIT INT TERM
  for i in $(seq 0 $((source_count-1))); do
    "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role server \
      --local-hostname "$local_ip:$((base_port+i))" --segment-bytes "$segment_bytes" \
      "${common[@]}" >"$out_dir/source-${local_ip//./_}-$i.log" 2>&1 & pids+=("$!")
  done
  for i in $(seq 0 $((source_count-1))); do
    log="$out_dir/source-${local_ip//./_}-$i.log"; tries=0
    until grep -q '"status": "READY"' "$log" 2>/dev/null; do
      sleep .3; tries=$((tries+1)); [ "$tries" -lt 200 ] || fatal "source $i not ready: $log"
    done
  done
  echo "[todoextra] $source_count RDMA source clients READY on $local_ip; Ctrl-C to stop"
  wait
fi

expect_sources="${TODOEXTRA_EXPECT_SOURCES:-1}"
if [ "$role" = prep ]; then
  exec "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role prep \
    --local-hostname "$local_ip:${TODOEXTRA_PREP_PORT:-50190}" \
    --min-source-segments "$expect_sources" "${common[@]}"
fi

iterations="${TODOEXTRA_ITERATIONS:-1}"
consumer_buf="${TODOEXTRA_CONSUMER_BUFFER_BYTES:-$((block_bytes + 1024*1024*1024))}"
gpu_id="${TODOEXTRA_GPU_ID:-0}"
echo "[todoextra] fetch key=$key size=${block_gib}GiB from >=$expect_sources RDMA sources"

"$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role consumer --mode dram \
  --local-hostname "$local_ip:${TODOEXTRA_DRAM_PORT:-50180}" --iterations "$iterations" \
  --consumer-buffer-bytes "$consumer_buf" --summary-json "$out_dir/dram.json" \
  "${common[@]}"

MC_STORE_RDMA_GPU_DIRECT=0 MC_STORE_TRACE_GPU_TRANSFERS=1 \
"$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role consumer --mode gpu \
  --local-hostname "$local_ip:${TODOEXTRA_GPU_PORT:-50181}" --iterations "$iterations" \
  --consumer-buffer-bytes "$consumer_buf" --gpu-id "$gpu_id" \
  --summary-json "$out_dir/gpu-staged.json" "${common[@]}" 2>&1 | tee "$out_dir/gpu-staged.log"

if [ "${TODOEXTRA_WITH_GDR:-0}" = 1 ]; then
  if ! env MC_STORE_RDMA_GPU_DIRECT=1 MC_STORE_TRACE_GPU_TRANSFERS=1 \
    "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role consumer --mode gpu \
      --local-hostname "$local_ip:${TODOEXTRA_GDR_PORT:-50182}" --iterations "$iterations" \
      --consumer-buffer-bytes "$consumer_buf" --gpu-id "$gpu_id" \
      --summary-json "$out_dir/gpu-gdr.json" "${common[@]}" 2>&1 | tee "$out_dir/gpu-gdr.log"; then
    echo "[todoextra] GDR candidate failed (no fallback); see $out_dir/gpu-gdr.log" >&2
  fi
fi

echo "[todoextra] summaries/logs: $out_dir"
