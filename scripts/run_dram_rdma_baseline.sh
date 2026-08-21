#!/usr/bin/env bash
# TODO#Extra baseline: GPU <- RDMA fetching using the Mooncake lib.
#
# One master + N DRAM-backed server clients (the object shards across them), one
# consumer that times: (1) fetch to local DRAM, (2) fetch to GPU (staged),
# (3) fetch to GPU (GPUDirect, MC_STORE_RDMA_GPU_DIRECT=1). Sweeps block size.
#
# Single node by default (loopback RDMA, N processes). For a real multi-node run,
# start `dram_rdma_perf.py --role server` on each peer node pointing at this
# master/metadata, then run the consumer here.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

N_SERVERS="${RDMA_N_SERVERS:-4}"                       # distributed /N
BLOCKS="${RDMA_BLOCKS:-1 2 4 8 16}"                    # GiB block sizes to sweep
SEGMENT_BYTES="${RDMA_SEGMENT_BYTES:-$((8 * 1024*1024*1024))}"   # DRAM per server
CONSUMER_BUF="${RDMA_CONSUMER_BUF:-$((20 * 1024*1024*1024))}"
DEVICE_NAME="${RDMA_DEVICE_NAME:-}"                    # RDMA NIC; blank = auto
GPU_ID="${RDMA_GPU_ID:-0}"
MASTER_HOST="${RDMA_MASTER_HOST:-127.0.0.1}"
MASTER_PORT="${RDMA_MASTER_PORT:-50051}"
METRICS_PORT="${RDMA_METRICS_PORT:-19004}"
META_PORT="${RDMA_META_PORT:-8080}"
LEASE_TTL="${RDMA_LEASE_TTL:-1h}"
OUT_DIR="${RDMA_OUT_DIR:-/tmp/dram-rdma-baseline}"
WITH_GDR="${RDMA_WITH_GDR:-1}"                         # also try GPUDirect
build_dir_name="${MOONCAKE_BUILD_DIR:-build-gpu-multipath}"

fatal() { echo "[FATAL] $*" >&2; exit 2; }
[ "$(uname -s)" = "Linux" ] || fatal "requires the Linux GPU host"

if [ -n "${PYTHON_BIN:-}" ]; then python_bin="$PYTHON_BIN"
elif [ -x "$repo_dir/$build_dir_name/.venv/bin/python" ]; then python_bin="$repo_dir/$build_dir_name/.venv/bin/python"
else python_bin="$(command -v python3)"; fi
pkg_root="$repo_dir/mooncake-wheel"
[ -f "$pkg_root/mooncake/store.so" ] || fatal "missing mooncake.store; build the CUDA wheel"
export PYTHONPATH="$pkg_root${PYTHONPATH:+:$PYTHONPATH}"
master_bin="$pkg_root/mooncake/mooncake_master"
meta_py="$pkg_root/mooncake/http_metadata_server.py"
[ -x "$master_bin" ] && [ -f "$meta_py" ] || fatal "missing master/metadata artifacts"

mkdir -p "$OUT_DIR"
META_PID=""; MASTER_PID=""; SERVER_PIDS=()
cleanup() {
  for pid in "${SERVER_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  [ -n "$MASTER_PID" ] && kill "$MASTER_PID" 2>/dev/null || true
  [ -n "$META_PID" ] && kill "$META_PID" 2>/dev/null || true
}
trap cleanup EXIT
wait_port() { local t=0; while ! (exec 9<>"/dev/tcp/$1/$2") 2>/dev/null; do
  sleep 0.2; t=$((t+1)); [ "$t" -gt "${3:-50}" ] && return 1; done; return 0; }

"$python_bin" -c 'import aiohttp' 2>/dev/null || \
  fatal "the HTTP metadata server needs aiohttp: $python_bin -m pip install aiohttp (do not run the metadata server under sudo/root, which lacks it)"
echo "[rdma] metadata :$META_PORT ; master :$MASTER_PORT"
"$python_bin" "$meta_py" --port "$META_PORT" >"$OUT_DIR/metadata.log" 2>&1 & META_PID=$!
wait_port 127.0.0.1 "$META_PORT" 50 || fatal "metadata did not start"
"$master_bin" --rpc_port="$MASTER_PORT" --metrics_port="$METRICS_PORT" \
  --enable_metric_reporting=false --default_kv_lease_ttl="$LEASE_TTL" \
  >"$OUT_DIR/master.log" 2>&1 & MASTER_PID=$!
wait_port "$MASTER_HOST" "$MASTER_PORT" 50 || fatal "master did not start"

common=( --store-module mooncake.store
         --master-server "$MASTER_HOST:$MASTER_PORT"
         --metadata-server "http://127.0.0.1:$META_PORT/metadata"
         --device-name "$DEVICE_NAME" )

echo "[rdma] launching $N_SERVERS DRAM servers ($((SEGMENT_BYTES/1024/1024/1024)) GiB each)"
for i in $(seq 0 $((N_SERVERS-1))); do
  "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role server \
    --local-hostname "$MASTER_HOST:$((50200 + i))" \
    --segment-bytes "$SEGMENT_BYTES" "${common[@]}" \
    >"$OUT_DIR/server-$i.log" 2>&1 &
  SERVER_PIDS+=($!)
done
# Wait for all servers to print READY.
for i in $(seq 0 $((N_SERVERS-1))); do
  t=0; until grep -q '"status": "READY"' "$OUT_DIR/server-$i.log" 2>/dev/null; do
    sleep 0.3; t=$((t+1)); [ "$t" -gt 100 ] && fatal "server $i not READY (see $OUT_DIR/server-$i.log)"; done
done
echo "[rdma] all servers READY"

printf '\n%-10s %-22s %-22s %-22s\n' "block" "to_DRAM_GB/s" "to_GPU_staged_GB/s" "to_GPU_gpudirect_GB/s"
for g in $BLOCKS; do
  block=$(( g * 1024*1024*1024 ))
  key="blk-${g}g"
  "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role prep \
    --local-hostname "$MASTER_HOST:50190" --key "$key" --block-bytes "$block" \
    "${common[@]}" >"$OUT_DIR/prep-${g}g.log" 2>&1 || { echo "[WARN] prep ${g}g failed"; continue; }

  cf="$OUT_DIR/consumer-${g}g-staged.json"
  MC_STORE_RDMA_GPU_DIRECT=0 "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role consumer \
    --local-hostname "$MASTER_HOST:50180" --key "$key" --block-bytes "$block" \
    --consumer-buffer-bytes "$CONSUMER_BUF" --gpu-id "$GPU_ID" \
    --summary-json "$cf" "${common[@]}" >"$OUT_DIR/consumer-${g}g-staged.log" 2>&1 || true

  gf=""
  if [ "$WITH_GDR" = "1" ]; then
    gf="$OUT_DIR/consumer-${g}g-gdr.json"
    MC_STORE_RDMA_GPU_DIRECT=1 "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role consumer \
      --local-hostname "$MASTER_HOST:50181" --key "$key" --block-bytes "$block" \
      --consumer-buffer-bytes "$CONSUMER_BUF" --gpu-id "$GPU_ID" --skip-dram \
      --summary-json "$gf" "${common[@]}" >"$OUT_DIR/consumer-${g}g-gdr.log" 2>&1 || true
  fi

  read -r dram staged gdr <<<"$("$python_bin" - "$cf" "$gf" <<'PY'
import json,sys
def rate(p,k):
    if not p: return "-"
    try:
        d=json.load(open(p))
        return str(d.get(k,{}).get("GBps","-")) if d.get("status")=="PASS" else "FAIL"
    except Exception: return "-"
c=sys.argv[1]; g=sys.argv[2] if len(sys.argv)>2 else ""
print(rate(c,"to_local_dram"), rate(c,"to_gpu_staged"), rate(g,"to_gpu_gpudirect"))
PY
)"
  printf '%-10s %-22s %-22s %-22s\n' "${g}GiB" "$dram" "$staged" "$gdr"
done

echo
echo "[rdma] JSON + logs in $OUT_DIR"
echo "[rdma] GPUDirect note: to_GPU_gpudirect reflects MC_STORE_RDMA_GPU_DIRECT=1;"
echo "       the selected path has no automatic fallback. Unsupported device-MR"
echo "       registration should FAIL; confirm path=rdma_gpu_direct in the log."
