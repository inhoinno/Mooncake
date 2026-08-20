#!/usr/bin/env bash
# Single-node CXL->GPU throughput scaling sweep: one master, N client processes
# sharing one GPU, reading 16 MiB(-4KiB) blocks from the shared CXL pool into
# device memory. Sweeps client count and reports per-client + aggregate GB/s.
#
# Prereqs on the GPU host (solab-m3): the CUDA build with cxl_transport, the
# staged mooncake wheel (mooncake.store importable), torch+CUDA, a real /dev/dax,
# and aiohttp (for the HTTP metadata server).
#
# This starts and stops the HTTP metadata server and the mooncake master itself,
# keeps them local, and never kills unrelated processes.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---- knobs (env-overridable) -------------------------------------------------
DEV_PATH="${MC_CXL_DEV_PATH:-/dev/dax24.0}"
DEV_SIZE="${MC_CXL_DEV_SIZE:-107374182400}"          # 100 GiB slice of the pool
POOL_ID="${MC_CXL_POOL_ID:-cxl-gpu-perf-pool}"
MASTER_HOST="${CXLPERF_MASTER_HOST:-127.0.0.1}"
MASTER_PORT="${CXLPERF_MASTER_PORT:-50051}"
METRICS_PORT="${CXLPERF_METRICS_PORT:-19003}"
META_PORT="${CXLPERF_META_PORT:-8080}"
GPU_ID="${CXLPERF_GPU_ID:-0}"
BLOCK_BYTES="${CXLPERF_BLOCK_BYTES:-16773120}"       # 16 MiB - 4 KiB (Store cap)
NUM_OBJECTS="${CXLPERF_NUM_OBJECTS:-64}"
BATCH_SIZE="${CXLPERF_BATCH_SIZE:-8}"
RUNTIME="${CXLPERF_RUNTIME:-10}"
WARMUP="${CXLPERF_WARMUP:-3}"
CLIENTS="${CXLPERF_CLIENTS:-1 2 4 8}"                 # the sweep
LEASE_TTL="${CXLPERF_LEASE_TTL:-1h}"
OUT_DIR="${CXLPERF_OUT_DIR:-/tmp/cxl-gpu-perf}"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-gpu-multipath}"

fatal() { echo "[FATAL] $*" >&2; exit 2; }
[ "$(uname -s)" = "Linux" ] || fatal "requires the Linux GPU host"
[ -c "$DEV_PATH" ] || fatal "$DEV_PATH is not a devdax char device"

# python + wheel
if [ -n "${PYTHON_BIN:-}" ]; then python_bin="$PYTHON_BIN"
elif [ -x "$repo_dir/$build_dir_name/.venv/bin/python" ]; then python_bin="$repo_dir/$build_dir_name/.venv/bin/python"
else python_bin="$(command -v python3)"; fi
pkg_root="$repo_dir/mooncake-wheel"
[ -f "$pkg_root/mooncake/store.so" ] || fatal "missing mooncake.store; build the wheel"
export PYTHONPATH="$pkg_root${PYTHONPATH:+:$PYTHONPATH}"
master_bin="$pkg_root/mooncake/mooncake_master"
[ -x "$master_bin" ] || fatal "missing mooncake_master; build the wheel"
meta_py="$pkg_root/mooncake/http_metadata_server.py"
[ -f "$meta_py" ] || fatal "missing http_metadata_server.py"

mkdir -p "$OUT_DIR"
META_PID=""; MASTER_PID=""; WRITER_PID=""
cleanup() {
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null || true
  [ -n "$MASTER_PID" ] && kill "$MASTER_PID" 2>/dev/null || true
  [ -n "$META_PID" ] && kill "$META_PID" 2>/dev/null || true
}
trap cleanup EXIT

wait_port() { # host port timeout_s
  local t=0; while ! (exec 9<>"/dev/tcp/$1/$2") 2>/dev/null; do
    sleep 0.2; t=$((t+1)); [ "$t" -gt "${3:-50}" ] && return 1; done; return 0
}

echo "[perf] metadata server :$META_PORT"
"$python_bin" "$meta_py" --port "$META_PORT" >"$OUT_DIR/metadata.log" 2>&1 &
META_PID=$!
wait_port 127.0.0.1 "$META_PORT" 50 || fatal "metadata server did not start"

echo "[perf] master :$MASTER_PORT (cxl enabled, $DEV_PATH, $DEV_SIZE bytes)"
MC_CXL_PROVIDER=faketract MC_CXL_BACKEND_KIND=devdax MC_CXL_POOL_ID="$POOL_ID" \
MC_CXL_DEV_PATH="$DEV_PATH" MC_CXL_DEV_SIZE="$DEV_SIZE" \
  "$master_bin" --rpc_port="$MASTER_PORT" --metrics_port="$METRICS_PORT" \
    --enable_metric_reporting=false --enable_cxl=true --allocation_strategy=cxl \
    --cxl_path="$DEV_PATH" --cxl_size="$DEV_SIZE" \
    --default_kv_lease_ttl="$LEASE_TTL" >"$OUT_DIR/master.log" 2>&1 &
MASTER_PID=$!
wait_port "$MASTER_HOST" "$MASTER_PORT" 50 || fatal "master did not start"

common=( --store-module mooncake.store
         --master-server "$MASTER_HOST:$MASTER_PORT"
         --metadata-server "http://127.0.0.1:$META_PORT/metadata"
         --device-name "$DEV_PATH" --dev-size "$DEV_SIZE" --pool-id "$POOL_ID"
         --provider faketract --backend-kind devdax
         --block-bytes "$BLOCK_BYTES" --num-objects "$NUM_OBJECTS" )

run_id="$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo run)"
writer_log="$OUT_DIR/${run_id}-writer.log"
echo "[perf] writer(server): writing $NUM_OBJECTS x $BLOCK_BYTES blocks; stays alive"
# The writer must persist: a CXL replica references the allocating segment's
# endpoint, so readers 404 if the writer exits and its descriptor is deleted.
"$python_bin" "$repo_dir/scripts/cxl_gpu_perf.py" --role server \
  --local-hostname "$MASTER_HOST:50060" --key-prefix "$run_id" \
  "${common[@]}" >"$writer_log" 2>&1 &
WRITER_PID=$!
t=0; until grep -q '"status": "READY"' "$writer_log" 2>/dev/null; do
  sleep 0.3; t=$((t+1))
  kill -0 "$WRITER_PID" 2>/dev/null || fatal "writer died; see $writer_log"
  [ "$t" -gt 200 ] && fatal "writer not READY; see $writer_log"
done
echo "[perf] writer READY (objects resident, segment held open)"

echo
printf '%-8s %-14s %-16s\n' "clients" "aggregate_GB/s" "per_client_GB/s(avg)"
for n in $CLIENTS; do
  pids=(); files=()
  for i in $(seq 0 $((n-1))); do
    f="$OUT_DIR/${run_id}-n${n}-c${i}.json"; files+=("$f")
    "$python_bin" "$repo_dir/scripts/cxl_gpu_perf.py" --role client \
      --local-hostname "$MASTER_HOST:$((50100 + i))" \
      --client-id "n${n}-c${i}" --gpu-id "$GPU_ID" \
      --batch-size "$BATCH_SIZE" --runtime "$RUNTIME" --warmup "$WARMUP" \
      --key-prefix "$run_id" --summary-json "$f" \
      "${common[@]}" >"$OUT_DIR/${run_id}-n${n}-c${i}.log" 2>&1 &
    pids+=($!)
  done
  ok=1; for pid in "${pids[@]}"; do wait "$pid" || ok=0; done
  [ "$ok" -eq 1 ] || { echo "[WARN] a client in the n=$n round failed; see $OUT_DIR"; }
  agg="$("$python_bin" - "${files[@]}" <<'PY'
import json,sys
tot=0.0; k=0
for p in sys.argv[1:]:
    try:
        d=json.load(open(p))
        if d.get("status")=="PASS": tot+=d["throughput_GBps"]; k+=1
    except Exception: pass
print(f"{tot:.3f} {(tot/k if k else 0):.3f} {k}")
PY
)"
  read -r a per k <<<"$agg"
  printf '%-8s %-14s %-16s (%s/%s ok)\n' "$n" "$a" "$per" "$k" "$n"
done
echo
echo "[perf] summaries in $OUT_DIR ; master.log / metadata.log there too"
