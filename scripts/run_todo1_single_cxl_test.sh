#!/usr/bin/env bash
# Run the shortest real Mooncake Store -> CXL integration path from any cwd.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-todo1-cpu}"
pool_size="${MC_CXL_DEV_SIZE:-1073741824}"

case "$build_dir_name" in
  ""|/*|..|../*|*/../*|*/..)
    echo "[FATAL] MOONCAKE_BUILD_DIR must stay inside $repo_dir" >&2
    exit 2
    ;;
esac
case "$pool_size" in
  ""|*[!0-9]*|0)
    echo "[FATAL] MC_CXL_DEV_SIZE must be a positive decimal byte count" >&2
    exit 2
    ;;
esac

test_binary="$repo_dir/$build_dir_name/mooncake-store/tests/cxl_client_integration_test"
if [ ! -x "$test_binary" ]; then
  echo "[FATAL] integration test binary is missing: $test_binary" >&2
  echo "        Run: bash scripts/bootstrap_todo1_lab.sh" >&2
  exit 2
fi

owns_pool_file=0
if [ -n "${MC_CXL_TEST_FILE:-}" ]; then
  pool_file="$MC_CXL_TEST_FILE"
  case "$pool_file" in
    /dev/*)
      echo "[FATAL] this file-backed test must not target a device node" >&2
      exit 2
      ;;
  esac
  if [ -e "$pool_file" ] && [ ! -f "$pool_file" ]; then
    echo "[FATAL] MC_CXL_TEST_FILE must be a disposable regular file" >&2
    exit 2
  fi
  echo "[WARNING] the test will truncate and unlink $pool_file"
else
  pool_file="$(mktemp /tmp/callosum-cxl-single.XXXXXX)"
  owns_pool_file=1
fi

cleanup() {
  # The test normally unlinks its file. Remove only the exact temporary path
  # created by this script if setup or execution failed before test teardown.
  if [ "$owns_pool_file" -eq 1 ] && [ -e "$pool_file" ]; then
    rm -f -- "$pool_file"
  fi
}
trap cleanup EXIT

echo "[preflight] tier=T0 mode=single-process provider=faketract"
echo "[preflight] binary=$test_binary"
echo "[preflight] pool_file=$pool_file pool_bytes=$pool_size"

MC_CXL_PROVIDER=faketract \
MC_CXL_BACKEND_KIND=file \
MC_CXL_POOL_ID="${MC_CXL_POOL_ID:-callosum-single-pool}" \
MC_CXL_DEV_PATH="$pool_file" \
MC_CXL_DEV_SIZE="$pool_size" \
MOONCAKE_STORE_CHECKSUM=1 \
DEFAULT_KV_LEASE_TTL="${DEFAULT_KV_LEASE_TTL:-1}" \
  "$test_binary" \
    --protocol=cxl \
    --cxl_device_name="$pool_file" \
    --cxl_device_size="$pool_size" \
    --transfer_engine_metadata_url=P2PHANDSHAKE \
    --gtest_filter='ClientIntegrationTestCxl.BasicPutGetOperations:ClientIntegrationTestCxl.BatchPutGetOperations'

echo "[PASS] single-process Mooncake CXL Put/Get and BatchPut/BatchGet"
