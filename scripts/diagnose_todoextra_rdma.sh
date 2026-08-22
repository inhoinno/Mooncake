#!/usr/bin/env bash
# One-shot TODO Extra RDMA diagnosis. Run `source` on the source host and
# `prep` on the producer/GPU host while Master and source clients remain alive.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
role="${1:-}"
case "$role" in master|source|prep) ;; *)
  echo "Usage: bash scripts/diagnose_todoextra_rdma.sh <master|source|prep>" >&2
  exit 2
esac

master_address="${TODOEXTRA_MASTER_ADDRESS:-192.168.5.43:50051}"
master_host="${master_address%:*}"
master_port="${master_address##*:}"
metadata_url="${TODOEXTRA_METADATA_URL:-http://$master_host:18080/metadata}"
local_ip="${TODOEXTRA_LOCAL_IP:-}"
peer_ip="${TODOEXTRA_PEER_IP:-}"
device="${RDMA_DEVICE_NAME:-mlx5_0}"
source_endpoint="${TODOEXTRA_SOURCE_ENDPOINT:-192.168.5.44:50200}"
out_dir="${TODOEXTRA_OUT_DIR:-/tmp/todoextra-rdma}"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-gpu-multipath}"
if [ -n "${PYTHON_BIN:-}" ]; then python_bin="$PYTHON_BIN"
elif [ -x "$repo_dir/$build_dir_name/.venv/bin/python" ]; then
  python_bin="$repo_dir/$build_dir_name/.venv/bin/python"
else python_bin="$(command -v python3)"; fi

mkdir -p "$out_dir"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
report="$out_dir/diagnose-${role}-$(hostname)-${stamp}.log"
exec > >(tee "$report") 2>&1

section() { printf '\n===== %s =====\n' "$*"; }
run() {
  printf '+ '
  printf '%q ' "$@"
  printf '\n'
  "$@" || printf '[WARN] command failed rc=%s\n' "$?"
}

section identity
echo "role=$role host=$(hostname) local_ip=${local_ip:-unset} peer_ip=${peer_ip:-unset}"
echo "master=$master_address metadata=$metadata_url rdma_device=$device"
echo "source_endpoint=$source_endpoint report=$report"
run uname -a
run git -C "$repo_dir" rev-parse --short HEAD

section artifacts
for artifact in \
  "$repo_dir/mooncake-wheel/mooncake/mooncake_master" \
  "$repo_dir/mooncake-wheel/mooncake/store.so" \
  "$repo_dir/$build_dir_name/mooncake-store/src/mooncake_master"; do
  if [ -e "$artifact" ]; then
    run stat -c '%y %s %n' "$artifact"
    run sha256sum "$artifact"
  else
    echo "[FAIL] missing artifact=$artifact"
  fi
done
export PYTHONPATH="$repo_dir/mooncake-wheel${PYTHONPATH:+:$PYTHONPATH}"
run "$python_bin" -c \
  'from mooncake import store; s=store.MooncakeDistributedStore(); print("store_module=",store.__file__); print("profile_api=",hasattr(s,"batch_get_into_profiled"))'

section control_plane
run curl -fsS --max-time 3 "${metadata_url}?key=health-probe"
run nc -vz -w 3 "$master_host" "$master_port"
run ss -ltnp

section rdma_inventory
run ibv_devices
run ibv_devinfo -d "$device"
run ibdev2netdev
run rdma link show
run ip -br link
run ip -br address
netdev="$(ibdev2netdev 2>/dev/null | awk -v dev="$device" '$1 == dev {print $5; exit}')"
if [ -n "$netdev" ]; then
  echo "resolved_netdev=$netdev"
  run ip -s -d link show dev "$netdev"
  run ethtool "$netdev"
  run ethtool -S "$netdev"
fi
if [ -n "$peer_ip" ]; then
  run ip route get "$peer_ip"
  run ping -c 3 -W 1 "$peer_ip"
fi
run dmesg --level=err,warn

section processes_and_logs
run pgrep -a -f 'run_dram_rdma_distributed|dram_rdma_perf.py|mooncake_master|http_metadata_server'
if compgen -G "$out_dir/source-*.log" >/dev/null; then
  run grep -H -E '"status": "READY"|"status": "FAIL"|RDMA device:|Found [0-9]+ HCAs|retry counter exceeded|ERROR|FATAL' "$out_dir"/source-*.log
fi

section source_metadata
metadata_key="$($python_bin -c 'import sys,urllib.parse; print(urllib.parse.quote("mooncake/ram/"+sys.argv[1], safe=""))' "$source_endpoint")"
run curl -fsS --max-time 3 "${metadata_url}?key=${metadata_key}"

if [ "$role" = prep ]; then
  section active_16m_store_probe
  probe_key="todoextra-rdma-diag-${stamp}"
  common=(--store-module mooncake.store --master-server "$master_address"
          --metadata-server "$metadata_url" --device-name "$device"
          --key "$probe_key" --block-bytes 16773120)
  set +e
  "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role prep \
    --local-hostname "${local_ip}:50390" --min-source-segments 1 \
    --summary-json "$out_dir/diagnose-prep-${stamp}.json" "${common[@]}"
  prep_rc=$?
  if [ "$prep_rc" -eq 0 ]; then
    "$python_bin" "$repo_dir/scripts/dram_rdma_perf.py" --role consumer \
      --mode dram --local-hostname "${local_ip}:50380" --iterations 1 \
      --warmup 0 --consumer-buffer-bytes 67108864 \
      --summary-json "$out_dir/diagnose-consumer-${stamp}.json" \
      "${common[@]}"
    consumer_rc=$?
  else
    consumer_rc=99
  fi
  set -e
  echo "active_probe prep_rc=$prep_rc consumer_rc=$consumer_rc key=$probe_key"
  if [ "$prep_rc" -eq 0 ] && [ "$consumer_rc" -eq 0 ]; then
    echo "[PASS] 16MiB Mooncake RDMA PUT/GET data plane works"
    echo "[DIAG] the 8GiB failure is size/concurrency/task-pressure specific"
  else
    echo "[FAIL] 16MiB Mooncake RDMA data plane also fails"
    echo "[DIAG] inspect QP/GID/MTU/firewall/source liveness before performance testing"
  fi
fi

section failure_signatures
if grep -Rqs 'transport retry counter exceeded' "$out_dir"; then
  echo "[MATCH] transport retry exceeded: remote QP did not acknowledge RDMA work"
  echo "        check source process lifetime, GID/route/MTU, firewall, and NIC errors"
fi
if grep -Rqs "source_protocols.*tcp\|non-RDMA segment" "$out_dir"; then
  echo "[MATCH] tcp replica metadata found: Master protocol propagation/artifact mismatch"
fi
echo "[DONE] consolidated report=$report"
