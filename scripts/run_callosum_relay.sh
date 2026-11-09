#!/usr/bin/env bash
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "$script_dir/.." && pwd)
python_bin=${PYTHON_BIN:-python3}
native_build=${CALLOSUM_NATIVE_BUILD:-$repo_dir/build-callosum-relay}
if [[ ${1:-} == lab ]]; then
  shift
  lab_root=${CALLOSUM_ROOT:-$repo_dir/..}
  exec "$python_bin" "$script_dir/callosum_lab.py" --action node \
    --root "$lab_root" --node-id "${NODE_ID:-}" \
    --config "${CONFIG:-$lab_root/lab.json}" --token-file "${TOKEN_FILE:-$lab_root/control.token}" "$@"
fi
if [[ ${1:-} == build ]]; then
  cmake -S "$repo_dir/mooncake-integration/pool_relay" -B "$native_build" \
    -DPython3_EXECUTABLE="$(command -v "$python_bin")"
  cmake --build "$native_build" -j "${BUILD_JOBS:-4}"
  exit
fi
export PYTHONPATH="$native_build:$script_dir:${PYTHONPATH:-}"
if [[ ${1:-} != --help && ${1:-} != -h ]]; then
  "$python_bin" -c 'import callosum_relay_native as n; assert n.ABI_VERSION == 2, "rebuild native relay module"'
fi
if [[ ${1:-} == test ]]; then
  shift
  exec ctest --test-dir "$native_build" --output-on-failure --no-tests=error "$@"
fi
exec "$python_bin" "$script_dir/callosum_runtime.py" "$@"
