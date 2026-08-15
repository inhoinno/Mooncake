#!/usr/bin/env bash
# Build and run the isolated CachelibBufferAllocator preflight + benchmark.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir_name="${CACHELIB_BENCH_BUILD_DIR:-build-cachelib-bench}"
build_jobs="${CACHELIB_BENCH_BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"

case "$build_dir_name" in
  /*|../*|*/../*|*/..)
    echo "[FATAL] CACHELIB_BENCH_BUILD_DIR must stay inside $repo_dir" >&2
    exit 2
    ;;
esac
if [ "$(uname -s)" != "Linux" ]; then
  echo "[FATAL] build this Mooncake benchmark in the Linux lab/container environment" >&2
  exit 2
fi

build_dir="$repo_dir/$build_dir_name"
cmake_args=(
  -S "$repo_dir"
  -B "$build_dir"
  -DCMAKE_BUILD_TYPE=RelWithDebInfo
  -DWITH_STORE=ON
  -DWITH_STORE_RUST=OFF
  -DBUILD_UNIT_TESTS=OFF
  -DBUILD_EXAMPLES=OFF
  -DBUILD_BENCHMARK=ON
  -DUSE_CUDA=OFF
)
if [ ! -f "$build_dir/CMakeCache.txt" ] && command -v ninja >/dev/null 2>&1; then
  cmake_args+=( -G Ninja )
fi

cmake "${cmake_args[@]}"
cmake --build "$build_dir" --target cachelib_allocator_bench \
  --parallel "$build_jobs"

benchmark="$build_dir/mooncake-store/benchmarks/cachelib_allocator_bench"
echo "[preflight] running four allocator checks"
"$benchmark" --self_test

benchmark_args=(
  --num_objects="${CACHELIB_BENCH_NUM_OBJECTS:-1000000}"
  --object_size="${CACHELIB_BENCH_OBJECT_SIZE:-4096}"
  --pool_size_bytes="${CACHELIB_BENCH_POOL_SIZE_BYTES:-8589934592}"
)
if [ "${CACHELIB_BENCH_TOUCH_MEMORY:-0}" = "1" ]; then
  benchmark_args+=( --touch_memory )
fi
echo "[benchmark] ${benchmark_args[*]}"
"$benchmark" "${benchmark_args[@]}"
