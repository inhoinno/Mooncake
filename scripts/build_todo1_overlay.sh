#!/usr/bin/env bash
# Build the Mooncake CXL TODO#1 code, run its portable gates, and stage the
# complete Python package consumed by the HBF directory-overlay launchers.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-todo1}"
build_jobs="${MOONCAKE_BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
use_cuda="${MOONCAKE_USE_CUDA:-ON}"

case "$build_dir_name" in
  /*|../*|*/../*|*/..)
    echo "[FATAL] MOONCAKE_BUILD_DIR must stay inside $repo_dir" >&2
    exit 2
    ;;
esac

if [ "$(uname -s)" != "Linux" ]; then
  echo "[FATAL] the CXL/RDMA overlay must be built on Linux in an ABI-compatible" >&2
  echo "        environment (preferably the same image used by the launcher)." >&2
  exit 2
fi

pybind_source_dir="$repo_dir/extern/pybind11"
if [ ! -f "$pybind_source_dir/CMakeLists.txt" ]; then
  echo "[FATAL] incomplete pybind11 checkout: missing $pybind_source_dir/CMakeLists.txt" >&2
  echo "        Run: git submodule update --init --recursive extern/pybind11" >&2
  exit 2
fi

# USE_HTTP is required by this Store build, so Mooncake's transfer engine calls
# find_package(CURL REQUIRED). The curl command alone is insufficient: the
# development package supplies curl-config, headers, and the link library.
if ! command -v curl-config >/dev/null 2>&1; then
  echo "[FATAL] libcurl development files are required because USE_HTTP=ON" >&2
  echo "        Ubuntu/Debian: sudo apt-get install -y libcurl4-openssl-dev" >&2
  echo "        RHEL/Fedora:   sudo dnf install -y libcurl-devel" >&2
  exit 2
fi

build_dir="$repo_dir/$build_dir_name"
ylt_source_dir="$repo_dir/extern/yalantinglibs"
ylt_build_dir="$build_dir/_deps/yalantinglibs-build"
ylt_prefix="$build_dir/_deps/yalantinglibs-install"
ylt_config_dir="${MOONCAKE_YALANTINGLIBS_DIR:-$ylt_prefix/lib/cmake/yalantinglibs}"

# Mooncake consumes yalantinglibs as an installed CMake CONFIG package. Merely
# initializing the git submodule is insufficient. Keep the pinned dependency
# inside this build tree so the lab build is reproducible and needs no sudo.
if [ -z "${MOONCAKE_YALANTINGLIBS_DIR:-}" ] &&
   [ ! -f "$ylt_config_dir/yalantinglibsConfig.cmake" ]; then
  ylt_required_files=( CMakeLists.txt cmake/build.cmake cmake/install.cmake )
  for ylt_required_file in "${ylt_required_files[@]}"; do
    if [ ! -f "$ylt_source_dir/$ylt_required_file" ]; then
      echo "[FATAL] incomplete yalantinglibs checkout: missing $ylt_source_dir/$ylt_required_file" >&2
      echo "        Initialize the submodule, then restore that tracked file:" >&2
      echo "        git submodule update --init --recursive extern/yalantinglibs" >&2
      echo "        git -C extern/yalantinglibs restore --source=HEAD --worktree -- $ylt_required_file" >&2
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
  echo "[FATAL] yalantinglibs package config is missing from $ylt_config_dir" >&2
  echo "        Set MOONCAKE_YALANTINGLIBS_DIR to its lib/cmake/yalantinglibs directory." >&2
  exit 2
fi

cmake_args=(
  -S "$repo_dir"
  -B "$build_dir"
  -DCMAKE_BUILD_TYPE=RelWithDebInfo
  -DUSE_CXL=ON
  -DUSE_HTTP=ON
  -DUSE_CUDA="$use_cuda"
  -DWITH_STORE=ON
  -DWITH_STORE_RUST=OFF
  -DBUILD_UNIT_TESTS=ON
  -DBUILD_EXAMPLES=ON
  -DBUILD_BENCHMARK=OFF
  -Dyalantinglibs_DIR="$ylt_config_dir"
)

if [ ! -f "$build_dir/CMakeCache.txt" ] && command -v ninja >/dev/null 2>&1; then
  cmake_args+=( -G Ninja )
fi
if [ -n "${MOONCAKE_CMAKE_ARGS:-}" ]; then
  read -r -a extra_cmake_args <<<"$MOONCAKE_CMAKE_ARGS"
  cmake_args+=( "${extra_cmake_args[@]}" )
fi

echo "[build] source=$repo_dir build=$build_dir jobs=$build_jobs USE_CUDA=$use_cuda"
cmake "${cmake_args[@]}"
cmake --build "$build_dir" --parallel "$build_jobs"
ctest --test-dir "$build_dir" --output-on-failure -L todo1

(
  cd "$repo_dir"
  BUILD_DIR="$build_dir_name" ./scripts/build_wheel.sh
)

package_dir="$repo_dir/mooncake-wheel/mooncake"
for artifact in engine.so store.so mooncake_master mooncake_client; do
  if [ ! -e "$package_dir/$artifact" ]; then
    echo "[FATAL] overlay package is missing $package_dir/$artifact" >&2
    exit 1
  fi
done

echo "[PASS] TODO#1 build, tests, and overlay staging completed"
echo "[PASS] MOONCAKE_PACKAGE_HOST=$package_dir"
