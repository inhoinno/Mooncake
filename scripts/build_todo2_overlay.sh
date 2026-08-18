#!/usr/bin/env bash
# Build the CXL-enabled Mooncake overlay, preserve TODO #1 regressions, then
# run the four ownership categories plus the native Store integration gate.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export MOONCAKE_BUILD_DIR="${MOONCAKE_BUILD_DIR:-build-todo2-cpu}"
export MOONCAKE_USE_CUDA="${MOONCAKE_USE_CUDA:-OFF}"

bash "$repo_dir/scripts/build_todo1_overlay.sh"
ctest --test-dir "$repo_dir/$MOONCAKE_BUILD_DIR" --output-on-failure -L todo2

echo "[PASS] TODO #2 native CXL software gates completed"
