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

if ! printf '#include <xxhash.h>\n' |
     "${CXX:-c++}" -E -x c++ - >/dev/null 2>&1; then
  echo "[FATAL] xxHash development files are required by Mooncake Store checksums" >&2
  echo "        Ubuntu/Debian: sudo apt-get install -y libxxhash-dev" >&2
  echo "        RHEL/Fedora:   sudo dnf install -y xxhash-devel" >&2
  exit 2
fi

build_dir="$repo_dir/$build_dir_name"
ylt_prefix_root="$build_dir/_deps/yalantinglibs-install"
ylt_config_dir="${MOONCAKE_YALANTINGLIBS_DIR:-$ylt_prefix_root/current/lib/cmake/yalantinglibs}"

# Mooncake consumes yalantinglibs as an installed CMake CONFIG package. Merely
# initializing the git submodule is insufficient. TODO#0 builds a normalized
# source snapshot in a fresh Makefiles tree, then publishes a revision-keyed
# install. This prevents copied/future-dated Ninja manifests from looping.
if [ -z "${MOONCAKE_YALANTINGLIBS_DIR:-}" ]; then
  bash "$repo_dir/scripts/bootstrap_todo0_lab.sh" \
    --prefix-root "$ylt_prefix_root" \
    --jobs "$build_jobs"
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

# Mooncake can turn USE_CUDA back on when any of these cached features is ON.
# A CPU-only TODO#1 build must exclude the CUDA device transport completely;
# otherwise older CUDA headers fail on fabric-handle symbols even though the
# caller explicitly supplied MOONCAKE_USE_CUDA=OFF.
case "$use_cuda" in
  OFF|off|FALSE|false|NO|no|0)
    cmake_args+=(
      -DUSE_NVMEOF=OFF
      -DUSE_MNNVL=OFF
      -DUSE_VRAM_SEGMENT=OFF
      -DUSE_NCCL_DEVICE=OFF
      -DUSE_NCCL_HOST=OFF
      -DUSE_MUSA=OFF
      -DUSE_MACA=OFF
    )
    ;;
esac

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
ctest --test-dir "$build_dir" --output-on-failure -L todo1 -LE hardware

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
