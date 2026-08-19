#!/usr/bin/env bash
# Run legacy single-node Mooncake Store CXL Put/Get and BatchPut/BatchGet for
# the mandatory 4 KiB, 64 KiB, 1 MiB, and 16 MiB object-size matrix.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
case_count="${CXL_MATRIX_NUM_OBJECTS:-16}"
pool_size="${CXL_MATRIX_POOL_SIZE_BYTES:-2147483648}"
batch_size="${CXL_MATRIX_BATCH_SIZE:-8}"

fatal() {
  echo "[FATAL] $*" >&2
  exit 2
}

for pair in \
  "CXL_MATRIX_NUM_OBJECTS:$case_count" \
  "CXL_MATRIX_POOL_SIZE_BYTES:$pool_size" \
  "CXL_MATRIX_BATCH_SIZE:$batch_size"; do
  name="${pair%%:*}"
  value="${pair#*:}"
  case "$value" in
    ""|*[!0-9]*|0) fatal "$name must be a positive decimal integer" ;;
  esac
done

output_root="${CXL_MATRIX_OUTPUT_DIR:-}"
if [ -z "$output_root" ]; then
  output_root="$(mktemp -d /tmp/callosum-cxl-store-matrix.XXXXXX)"
else
  mkdir -p "$output_root"
fi

object_sizes=(4096 65536 1048576 16773120)  # 16 MiB - 4 KiB: under kMaxSliceSize (Slab::kSize - 16)
apis=(single batch)
summaries=()

echo "[preflight] test=legacy_single_node_cxl_matrix"
echo "[preflight] object_sizes=${object_sizes[*]} apis=${apis[*]}"
echo "[preflight] objects_per_case=$case_count output_dir=$output_root"

for api in "${apis[@]}"; do
  if [ "$api" = "single" ]; then
    requested_batch=1
  else
    requested_batch="$batch_size"
  fi
  for object_size in "${object_sizes[@]}"; do
    case_dir="$output_root/${api}-${object_size}"
    summary="$case_dir/summary.json"
    echo "[case] api=$api object_bytes=$object_size"
    CXL_BENCH_OUTPUT_DIR="$case_dir" \
    CXL_BENCH_NUM_OBJECTS="$case_count" \
    CXL_BENCH_VALUE_SIZE="$object_size" \
    CXL_BENCH_BATCH_SIZE="$requested_batch" \
    CXL_BENCH_POOL_SIZE_BYTES="$pool_size" \
      bash "$repo_dir/scripts/run_todo1_cxl_store_bench.sh"
    summaries+=("$summary")
  done
done

python_bin="${PYTHON_BIN:-$(command -v python3 || true)}"
[ -n "$python_bin" ] || fatal "python3 is required to validate matrix summaries"
"$python_bin" - "${summaries[@]}" <<'PY'
import json
import pathlib
import sys

summaries = []
for argument in sys.argv[1:]:
    path = pathlib.Path(argument)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not value.get("ok"):
        raise SystemExit(f"matrix case failed: {path}: {value}")
    summaries.append(str(path))
print(json.dumps({"status": "PASS", "case_count": len(summaries), "summaries": summaries}, sort_keys=True))
PY

echo "[PASS] legacy Mooncake CXL matrix completed cases=${#summaries[@]}"
echo "[result] output_dir=$output_root"
