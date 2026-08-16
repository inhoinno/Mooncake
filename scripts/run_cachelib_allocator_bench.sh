#!/usr/bin/env bash
# Build and run the isolated CachelibBufferAllocator preflight + benchmark.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir_name="${CACHELIB_BENCH_BUILD_DIR:-build-cachelib-bench}"
build_jobs="${CACHELIB_BENCH_BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
todo1_build_dir_name="${MOONCAKE_TODO1_BUILD_DIR:-build-todo1-cpu}"

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
ylt_source_dir="$repo_dir/extern/yalantinglibs"
ylt_build_dir="$build_dir/_deps/yalantinglibs-build"
ylt_prefix="$build_dir/_deps/yalantinglibs-install"
ylt_local_config_dir="$ylt_prefix/lib/cmake/yalantinglibs"
ylt_todo1_config_dir="$repo_dir/$todo1_build_dir_name/_deps/yalantinglibs-install/lib/cmake/yalantinglibs"

if [ ! -f "$repo_dir/extern/pybind11/CMakeLists.txt" ]; then
  echo "[FATAL] incomplete pybind11 checkout" >&2
  echo "        Run: git submodule update --init --recursive extern/pybind11" >&2
  exit 2
fi

if [ -n "${MOONCAKE_YALANTINGLIBS_DIR:-}" ]; then
  ylt_config_dir="$MOONCAKE_YALANTINGLIBS_DIR"
elif [ -f "$ylt_local_config_dir/yalantinglibsConfig.cmake" ]; then
  ylt_config_dir="$ylt_local_config_dir"
elif [ -f "$ylt_todo1_config_dir/yalantinglibsConfig.cmake" ]; then
  # The consolidated TODO#1 bootstrap installs the pinned package here. Reuse
  # it instead of rebuilding the same dependency for an allocator-only probe.
  ylt_config_dir="$ylt_todo1_config_dir"
else
  ylt_config_dir="$ylt_local_config_dir"
  ylt_required_files=( CMakeLists.txt cmake/build.cmake cmake/install.cmake )
  for ylt_required_file in "${ylt_required_files[@]}"; do
    if [ ! -f "$ylt_source_dir/$ylt_required_file" ]; then
      echo "[FATAL] incomplete yalantinglibs checkout: missing $ylt_source_dir/$ylt_required_file" >&2
      echo "        Run: git submodule update --init --recursive extern/yalantinglibs" >&2
      exit 2
    fi
  done

  echo "[build] bootstrapping yalantinglibs into $ylt_prefix"
  ylt_cmake_args=(
    -S "$ylt_source_dir"
    -B "$ylt_build_dir"
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX="$ylt_prefix"
    -DYLT_ENABLE_CUDA=OFF
    -DBUILD_EXAMPLES=OFF
    -DBUILD_BENCHMARK=OFF
    -DBUILD_UNIT_TESTS=OFF
  )
  if [ ! -f "$ylt_build_dir/CMakeCache.txt" ] && command -v ninja >/dev/null 2>&1; then
    ylt_cmake_args+=( -G Ninja )
  fi
  cmake "${ylt_cmake_args[@]}"
  cmake --build "$ylt_build_dir" --parallel "$build_jobs"
  cmake --install "$ylt_build_dir"
fi

if [ ! -f "$ylt_config_dir/yalantinglibsConfig.cmake" ]; then
  echo "[FATAL] yalantinglibs package config is missing: $ylt_config_dir/yalantinglibsConfig.cmake" >&2
  echo "        Run the TODO#1 bootstrap or set MOONCAKE_YALANTINGLIBS_DIR." >&2
  exit 2
fi

echo "[preflight] yalantinglibs_DIR=$ylt_config_dir"
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
  -DUSE_NVMEOF=OFF
  -DUSE_MNNVL=OFF
  -DUSE_VRAM_SEGMENT=OFF
  -DUSE_NCCL_DEVICE=OFF
  -DUSE_NCCL_HOST=OFF
  -DUSE_MUSA=OFF
  -DUSE_MACA=OFF
  -Dyalantinglibs_DIR="$ylt_config_dir"
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
