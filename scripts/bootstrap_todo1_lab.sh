#!/usr/bin/env bash
# Bootstrap, build, package, and validate the CPU-only Mooncake CXL TODO#1
# stack on an Ubuntu/Debian lab host.
#
# This script is deliberately resumable. It does not pull source, delete build
# directories, modify the system Python environment, or reuse the CUDA cache.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="all"
build_dir_name="${MOONCAKE_BUILD_DIR:-build-todo1-cpu}"
skip_apt="${MOONCAKE_SETUP_SKIP_APT:-0}"
skip_submodules="${MOONCAKE_SETUP_SKIP_SUBMODULES:-0}"
apt_source_override_dir=""
apt_source_args=()

cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
if [ "$cpu_count" -gt 16 ]; then
  default_jobs=16
else
  default_jobs="$cpu_count"
fi
build_jobs="${MOONCAKE_BUILD_JOBS:-$default_jobs}"

usage() {
  cat <<'EOF'
Usage: bash scripts/bootstrap_todo1_lab.sh [options]

Default action: install dependencies, initialize pinned submodules, create a
private Python virtualenv, build/package the CPU-only overlay, and run TODO#1
validation.

Options:
  --preflight          Check dependencies/source without apt, git, or builds.
  --status             Report an existing build's cache, tests, and artifacts.
  --skip-apt           Do not invoke apt; still fail if packages are missing.
  --skip-submodules    Do not update submodules; still verify their contents.
  --build-dir NAME     In-repository build directory (default: build-todo1-cpu).
  --jobs N             Compile parallelism (default: min(host CPUs, 16)).
  -h, --help           Show this help.

Environment equivalents:
  MOONCAKE_BUILD_DIR, MOONCAKE_BUILD_JOBS, MOONCAKE_SETUP_SKIP_APT=1,
  MOONCAKE_SETUP_SKIP_SUBMODULES=1, and MOONCAKE_CMAKE_ARGS.

Safe recovery choices built into this script:
  * uses a dedicated CPU cache and forces CUDA-triggering features OFF;
  * installs yalantinglibs under the build tree, not system-wide;
  * uses <build-dir>/.venv, avoiding Ubuntu PEP 668 system-pip failures;
  * passes the same Python interpreter to CMake and wheel packaging;
  * records a timestamped log under <build-dir>/setup-logs/;
  * never runs git pull, rm -rf on a build tree, or --break-system-packages.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --preflight)
      mode="preflight"
      ;;
    --status)
      mode="status"
      ;;
    --skip-apt)
      skip_apt=1
      ;;
    --skip-submodules)
      skip_submodules=1
      ;;
    --build-dir)
      [ "$#" -ge 2 ] || { echo "[FATAL] --build-dir requires a value" >&2; exit 2; }
      build_dir_name="$2"
      shift
      ;;
    --jobs)
      [ "$#" -ge 2 ] || { echo "[FATAL] --jobs requires a value" >&2; exit 2; }
      build_jobs="$2"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[FATAL] unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

case "$build_dir_name" in
  ""|/*|..|../*|*/../*|*/..)
    echo "[FATAL] --build-dir must stay inside $repo_dir" >&2
    exit 2
    ;;
esac
case "$build_jobs" in
  ""|*[!0-9]*|0)
    echo "[FATAL] --jobs must be a positive integer" >&2
    exit 2
    ;;
esac

build_dir="$repo_dir/$build_dir_name"
venv_dir="$build_dir/.venv"
log_dir="$build_dir/setup-logs"
mkdir -p "$log_dir"
log_file="$log_dir/bootstrap-$(date -u +%Y%m%dT%H%M%SZ).log"
latest_log="$log_dir/latest.log"
stage="initialization"

# Mirror output without Bash /dev/fd process substitution. Some restricted lab
# shells deny access to those descriptors even though ordinary FIFOs work.
log_pipe="$log_dir/.bootstrap-log.$$.fifo"
mkfifo "$log_pipe"
exec 3>&1 4>&2
tee "$log_file" < "$log_pipe" >&3 &
tee_pid=$!
exec > "$log_pipe" 2>&1

cleanup_logging() {
  local status="$?"
  trap - EXIT
  exec 1>&3 2>&4
  wait "$tee_pid" 2>/dev/null || true
  rm -f "$log_pipe"
  if [[ "$apt_source_override_dir" == /tmp/mooncake-apt-sources.* ]]; then
    rm -rf -- "$apt_source_override_dir"
  fi
  exec 3>&- 4>&-
  exit "$status"
}
trap cleanup_logging EXIT

ln -sfn "$(basename "$log_file")" "$latest_log"

print_diagnostics() {
  local status="$1"
  local line="$2"
  trap - ERR
  set +e
  echo
  echo "[FAIL] stage=$stage exit=$status line=$line"
  echo "[FAIL] complete_log=$log_file"
  echo "[diagnose] matching failure lines (last 80):"
  grep -nE 'FAILED:|fatal error:|CMake Error|error:|undefined reference|Killed signal|externally-managed-environment|command not found|Failed to fetch|403 +Forbidden|repository .* no longer signed' \
    "$log_file" | tail -80
  cat <<'EOF'
[diagnose] known installation signatures handled by this bootstrap:
  yaml-cppConfig.cmake missing
    -> libyaml-cpp-dev is installed before CMake configuration.
  "Could not find pybind11"
    -> the pinned pybind11 submodule is initialized and integrity-checked.
  yalantinglibsConfig.cmake missing
    -> the pinned submodule is built and installed inside the CPU build tree.
  extern/yalantinglibs/cmake/build.cmake missing
    -> the checkout was copied with an unsafe build* exclusion; use a real git
       clone or restore the tracked submodule file.
  xxhash.h or curl development files missing
    -> libxxhash-dev and libcurl4-openssl-dev are in the apt manifest.
  "ninja: error: loading build.ninja"
    -> Ninja was run after a failed CMake configure. This script stops at the
       configuration error and never launches a nonexistent build graph.
  "manifest 'build.ninja' still dirty after 100 tries"
    -> a reused or future-dated yalantinglibs manifest kept regenerating.
       TODO#0 now builds a normalized snapshot in a fresh Makefiles tree and
       publishes a revision-keyed install for TODO#1.
  CUmemFabricHandle / CU_MEM_HANDLE_TYPE_FABRIC compile errors
    -> a CUDA feature contaminated a CPU cache; this script uses a separate
       build-todo1-cpu cache and forces every known GPU trigger OFF.
  CXL Put TRANSFER_FAIL with connect() to the logical client port
    -> the binary predates mounted-endpoint propagation. Rebuild so the CXL
       replica carries Transfer Engine's dynamic P2P handshake endpoint.
  "python: command not found"
    -> Python is selected explicitly; no /usr/bin/python compatibility link is
       required.
  "externally-managed-environment" (PEP 668)
    -> packaging runs inside <build-dir>/.venv, never against system Python.
  "venv: command not found"
    -> venv is a Python module, not an activation command. This script installs
       python3-venv and invokes python3 -m venv automatically.
  "MOONCAKE_USE_CUDA=OFF: command not found"
    -> hidden Unicode preceded a pasted environment assignment. Invoke this
       script directly; no prefixed assignment is needed.
  security.ubuntu.com InRelease returns HTTP 403
    -> official Ubuntu HTTP sources are copied to a temporary HTTPS-only apt
       configuration; /etc/apt is not modified.
  warnings from yalantinglibs
    -> warnings are not the build failure; inspect the first FAILED/error line.
EOF
  exit "$status"
}
trap 'print_diagnostics "$?" "$LINENO"' ERR

pass() {
  echo "[PASS] gate=$1${2:+ detail=$2}"
}

fatal() {
  echo "[FATAL] $*" >&2
  return 1
}

is_true() {
  case "${1:-}" in
    1|ON|on|TRUE|true|YES|yes) return 0 ;;
    *) return 1 ;;
  esac
}

require_linux_debian() {
  stage="platform preflight"
  [ "$(uname -s)" = "Linux" ] ||
    fatal "TODO#1 binaries must be built on an ABI-compatible Linux host"
  [ -r /etc/os-release ] || fatal "cannot identify Linux distribution"
  # shellcheck disable=SC1091
  . /etc/os-release
  case "${ID:-}" in
    ubuntu|debian) ;;
    *) fatal "this bootstrap supports Ubuntu/Debian apt hosts; detected ${ID:-unknown}" ;;
  esac
  command -v apt-get >/dev/null 2>&1 || fatal "apt-get is unavailable"
  command -v dpkg-query >/dev/null 2>&1 || fatal "dpkg-query is unavailable"
  pass "platform" "os=${PRETTY_NAME:-$ID} arch=$(uname -m)"
}

# Keep this manifest explicit. It combines Mooncake's base native dependencies
# with the packages needed by CXL TODO#1, CTest, Python bindings, and auditwheel.
apt_packages=(
  build-essential
  ca-certificates
  cmake
  curl
  git
  ninja-build
  patchelf
  pkg-config
  python3
  python3-dev
  python3-pip
  python3-venv
  unzip
  wget
  libasio-dev
  libboost-all-dev
  libcurl4-openssl-dev
  libgflags-dev
  libgoogle-glog-dev
  libgrpc++-dev
  libgrpc-dev
  libhiredis-dev
  libibverbs-dev
  libjemalloc-dev
  libjsoncpp-dev
  libmsgpack-dev
  libnuma-dev
  libprotobuf-dev
  libssl-dev
  libunwind-dev
  liburing-dev
  libxxhash-dev
  libyaml-cpp-dev
  libzmq3-dev
  libzstd-dev
  protobuf-compiler-grpc
)

missing_apt_packages=()
find_missing_packages() {
  local package
  missing_apt_packages=()
  for package in "${apt_packages[@]}"; do
    if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null |
         grep -q '^install ok installed$'; then
      missing_apt_packages+=( "$package" )
    fi
  done
}

is_official_ubuntu_http_source() {
  grep -Eq \
    'http://(security\.ubuntu\.com/ubuntu|([[:alnum:]-]+\.)?archive\.ubuntu\.com/ubuntu|ports\.ubuntu\.com/ubuntu-ports)([ /]|$)' \
    "$1"
}

rewrite_official_ubuntu_sources_to_https() {
  sed -E -i \
    -e 's#http://security\.ubuntu\.com/ubuntu#https://security.ubuntu.com/ubuntu#g' \
    -e 's#http://(([[:alnum:]-]+\.)?archive\.ubuntu\.com/ubuntu)#https://\1#g' \
    -e 's#http://ports\.ubuntu\.com/ubuntu-ports#https://ports.ubuntu.com/ubuntu-ports#g' \
    "$1"
}

prepare_apt_sources() {
  local source_files=( /etc/apt/sources.list )
  local source_file
  local needs_https_override=0

  for source_file in /etc/apt/sources.list.d/*.list \
                     /etc/apt/sources.list.d/*.sources; do
    [ -f "$source_file" ] && source_files+=( "$source_file" )
  done

  for source_file in "${source_files[@]}"; do
    if [ -f "$source_file" ] && is_official_ubuntu_http_source "$source_file"; then
      needs_https_override=1
      break
    fi
  done
  # Returning the status of a failed test here aborts the entire bootstrap
  # under `set -e`. No override is a successful/common path: it means the host
  # already uses HTTPS, a non-Ubuntu mirror, or no matching Ubuntu source.
  if [ "$needs_https_override" -ne 1 ]; then
    return 0
  fi

  apt_source_override_dir="$(mktemp -d /tmp/mooncake-apt-sources.XXXXXX)"
  mkdir -p "$apt_source_override_dir/sources.list.d"
  chmod 0755 "$apt_source_override_dir" "$apt_source_override_dir/sources.list.d"

  if [ -f /etc/apt/sources.list ]; then
    cp -p /etc/apt/sources.list "$apt_source_override_dir/sources.list"
  else
    : > "$apt_source_override_dir/sources.list"
  fi
  rewrite_official_ubuntu_sources_to_https \
    "$apt_source_override_dir/sources.list"

  for source_file in /etc/apt/sources.list.d/*.list \
                     /etc/apt/sources.list.d/*.sources; do
    [ -f "$source_file" ] || continue
    cp -p "$source_file" \
      "$apt_source_override_dir/sources.list.d/$(basename "$source_file")"
    rewrite_official_ubuntu_sources_to_https \
      "$apt_source_override_dir/sources.list.d/$(basename "$source_file")"
  done

  apt_source_args=(
    -o "Dir::Etc::sourcelist=$apt_source_override_dir/sources.list"
    -o "Dir::Etc::sourceparts=$apt_source_override_dir/sources.list.d"
  )
  echo "[setup] official Ubuntu HTTP apt sources redirected to HTTPS for this run"
}

install_packages() {
  stage="system package installation"
  find_missing_packages
  if [ "${#missing_apt_packages[@]}" -eq 0 ]; then
    pass "apt_packages" "all ${#apt_packages[@]} packages already installed"
    return
  fi

  echo "[setup] missing apt packages: ${missing_apt_packages[*]}"
  if is_true "$skip_apt"; then
    fatal "apt installation was skipped but required packages are missing"
  fi

  local apt_prefix=()
  if [ "${EUID:-$(id -u)}" -ne 0 ]; then
    command -v sudo >/dev/null 2>&1 ||
      fatal "sudo is required to install missing apt packages"
    apt_prefix=( sudo )
  fi

  prepare_apt_sources
  "${apt_prefix[@]}" apt-get "${apt_source_args[@]}" update
  "${apt_prefix[@]}" apt-get "${apt_source_args[@]}" \
    install -y --no-install-recommends \
    "${missing_apt_packages[@]}"

  find_missing_packages
  [ "${#missing_apt_packages[@]}" -eq 0 ] ||
    fatal "packages remain missing after apt: ${missing_apt_packages[*]}"
  pass "apt_packages" "installed and verified ${#apt_packages[@]} packages"
}

verify_packages_only() {
  stage="system package preflight"
  find_missing_packages
  [ "${#missing_apt_packages[@]}" -eq 0 ] ||
    fatal "missing apt packages: ${missing_apt_packages[*]}"
  pass "apt_packages" "all ${#apt_packages[@]} packages installed"
}

verify_repository() {
  stage="repository preflight"
  [ -f "$repo_dir/CMakeLists.txt" ] || fatal "missing repository CMakeLists.txt"
  [ -f "$repo_dir/scripts/build_todo1_overlay.sh" ] ||
    fatal "missing scripts/build_todo1_overlay.sh"
  git -C "$repo_dir" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
    fatal "use a real git clone; a flattened source copy cannot restore submodules safely"

  local required_files=(
    extern/pybind11/CMakeLists.txt
    extern/yalantinglibs/CMakeLists.txt
    extern/yalantinglibs/cmake/build.cmake
    extern/yalantinglibs/cmake/install.cmake
  )
  local required_file
  for required_file in "${required_files[@]}"; do
    [ -f "$repo_dir/$required_file" ] ||
      fatal "incomplete submodule checkout: missing $required_file"
  done
  pass "repository" "commit=$(git -C "$repo_dir" rev-parse --short HEAD) submodules=complete"
}

update_submodules() {
  stage="submodule initialization"
  if is_true "$skip_submodules"; then
    echo "[setup] submodule update skipped by request"
  else
    git -C "$repo_dir" submodule sync --recursive
    git -C "$repo_dir" submodule update --init --recursive \
      extern/pybind11 extern/yalantinglibs
  fi
  verify_repository
}

verify_toolchain() {
  stage="toolchain preflight"
  local commands=( cmake ninja c++ git curl-config patchelf python3 )
  local command_name
  for command_name in "${commands[@]}"; do
    command -v "$command_name" >/dev/null 2>&1 ||
      fatal "required command is unavailable: $command_name"
  done
  pass "toolchain" "cmake=$(cmake --version | head -1) python=$(python3 --version 2>&1)"
}

verify_native_headers() {
  stage="native dependency preflight"
  local headers=(
    asio.hpp
    curl/curl.h
    gflags/gflags.h
    glog/logging.h
    infiniband/verbs.h
    jsoncpp/json/json.h
    numa.h
    xxhash.h
    yaml-cpp/yaml.h
  )
  local header
  for header in "${headers[@]}"; do
    if ! printf '#include <%s>\n' "$header" |
         "${CXX:-c++}" -E -x c++ - >/dev/null 2>&1; then
      fatal "native development header is unavailable: $header"
    fi
  done
  pass "native_headers" "verified ${#headers[@]} required headers"
}

verify_python() {
  stage="Python preflight"
  python3 -c 'import sys; assert sys.version_info >= (3, 10), sys.version'
  python3 -m venv --help >/dev/null
  pass "python" "interpreter=$(command -v python3) version=$(python3 -c 'import platform; print(platform.python_version())')"
}

run_preflight() {
  require_linux_debian
  verify_packages_only
  verify_repository
  verify_toolchain
  verify_native_headers
  verify_python
  echo "[PASS] preflight complete"
}

create_python_environment() {
  stage="private Python environment"
  if [ ! -x "$venv_dir/bin/python" ]; then
    python3 -m venv "$venv_dir"
  fi
  # auditwheel >= 6 requires patchelf >= 0.14.5, but Ubuntu 22.04 apt ships
  # 0.14.3, which fails the wheel-repair step. The PyPI patchelf package bundles
  # a modern static binary into the venv; putting $venv_dir/bin ahead of PATH in
  # the build step (see run_all) makes both auditwheel and the direct patchelf
  # calls in build_wheel.sh use it instead of the too-old system copy.
  "$venv_dir/bin/python" -m pip install --upgrade \
    pip build setuptools wheel auditwheel patchelf
  "$venv_dir/bin/python" -c 'import auditwheel, build, setuptools, wheel'
  local venv_patchelf_version
  venv_patchelf_version="$("$venv_dir/bin/patchelf" --version 2>/dev/null |
    grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1)"
  [ -n "$venv_patchelf_version" ] ||
    fatal "patchelf did not install into $venv_dir/bin"
  pass "python_venv" "path=$venv_dir version=$("$venv_dir/bin/python" -c 'import platform; print(platform.python_version())') patchelf=$venv_patchelf_version"
}

cache_value() {
  local key="$1"
  sed -n "s/^${key}:[^=]*=//p" "$build_dir/CMakeCache.txt" | tail -1
}

verify_cpu_cache() {
  stage="CPU CMake cache validation"
  [ -f "$build_dir/CMakeCache.txt" ] || fatal "missing $build_dir/CMakeCache.txt"
  local off_flags=(
    USE_CUDA USE_NVMEOF USE_MNNVL USE_VRAM_SEGMENT USE_NCCL_DEVICE
    USE_NCCL_HOST USE_MUSA USE_MACA
  )
  local flag value
  for flag in "${off_flags[@]}"; do
    value="$(cache_value "$flag")"
    [ "$value" = "OFF" ] ||
      fatal "CPU build cache is contaminated: $flag=${value:-unset}"
  done
  [ "$(cache_value USE_CXL)" = "ON" ] || fatal "USE_CXL is not ON"
  [ "$(cache_value WITH_STORE)" = "ON" ] || fatal "WITH_STORE is not ON"
  pass "cpu_cache" "CXL=ON Store=ON CUDA_and_GPU_triggers=OFF"
}

verify_artifacts() {
  stage="artifact and wheel validation"
  local package_dir="$repo_dir/mooncake-wheel/mooncake"
  local artifacts=( engine.so store.so mooncake_master mooncake_client )
  local artifact
  for artifact in "${artifacts[@]}"; do
    [ -e "$package_dir/$artifact" ] || fatal "missing overlay artifact: $package_dir/$artifact"
  done

  local wheels=( "$repo_dir"/mooncake-wheel/dist/*.whl )
  [ -e "${wheels[0]}" ] || fatal "no repaired wheel found under mooncake-wheel/dist"
  pass "artifacts" "overlay=$package_dir wheel=$(basename "${wheels[0]}")"
}

report_status() {
  stage="status report"
  echo "[status] repository=$repo_dir"
  echo "[status] commit=$(git -C "$repo_dir" rev-parse --short HEAD 2>/dev/null || echo unavailable)"
  echo "[status] build=$build_dir"
  echo "[status] venv=$venv_dir"
  echo "[status] latest_log=$latest_log"
  if [ -f "$build_dir/CMakeCache.txt" ]; then
    local flag
    for flag in USE_CXL WITH_STORE USE_CUDA USE_NVMEOF USE_MNNVL \
                USE_VRAM_SEGMENT USE_NCCL_DEVICE USE_NCCL_HOST USE_MUSA USE_MACA; do
      echo "[status] cmake.$flag=$(cache_value "$flag")"
    done
    ctest --test-dir "$build_dir" -N -L 'todo1|multisource'
  else
    echo "[status] cmake_cache=not_built"
  fi

  local package_dir="$repo_dir/mooncake-wheel/mooncake"
  local artifact
  for artifact in engine.so store.so mooncake_master mooncake_client; do
    if [ -e "$package_dir/$artifact" ]; then
      echo "[status] artifact.$artifact=present"
    else
      echo "[status] artifact.$artifact=missing"
    fi
  done
  local wheels=( "$repo_dir"/mooncake-wheel/dist/*.whl )
  if [ -e "${wheels[0]}" ]; then
    printf '[status] wheel=%s\n' "${wheels[@]}"
  else
    echo "[status] wheel=missing"
  fi
}

run_all() {
  require_linux_debian
  install_packages
  update_submodules
  verify_toolchain
  verify_native_headers
  verify_python
  create_python_environment

  stage="Mooncake TODO#1 CPU build, CTests, and wheel"
  local python_version
  python_version="$("$venv_dir/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  local cmake_extra="-DPython3_EXECUTABLE=$venv_dir/bin/python"
  if [ -n "${MOONCAKE_CMAKE_ARGS:-}" ]; then
    cmake_extra="$cmake_extra $MOONCAKE_CMAKE_ARGS"
  fi

  # Prefer the venv's modern patchelf over the system one so auditwheel repair
  # (which resolves patchelf through PATH) and build_wheel.sh's direct patchelf
  # calls both meet auditwheel's >= 0.14.5 requirement.
  MOONCAKE_USE_CUDA=OFF \
  MOONCAKE_BUILD_DIR="$build_dir_name" \
  MOONCAKE_BUILD_JOBS="$build_jobs" \
  MOONCAKE_CMAKE_ARGS="$cmake_extra" \
  PYTHON_BIN="$venv_dir/bin/python" \
  PYTHON_VERSION="$python_version" \
  PATH="$venv_dir/bin:$PATH" \
    bash "$repo_dir/scripts/build_todo1_overlay.sh"

  verify_cpu_cache
  stage="final TODO#1 test gate"
  # Include the multisource/GPU-path policy tests (gpu_transfer_policy_test,
  # multipath_placement_test, CxlAware allocation): they are CPU-only logic
  # gates for the CXL+RDMA->GPU multipath story and were previously hidden
  # because they carry the "multisource" label, not "todo1".
  ctest --test-dir "$build_dir" --output-on-failure -L 'todo1|multisource' -LE hardware
  pass "todo1_ctest" "TODO#1 and multisource GPU-path policy tests passed"
  verify_artifacts
  report_status

  echo
  echo "[PASS] lab bootstrap, build, package, and validation completed"
  echo "[PASS] complete_log=$log_file"
  echo "[PASS] rerun=bash scripts/bootstrap_todo1_lab.sh"
}

echo "[setup] mode=$mode source=$repo_dir build=$build_dir jobs=$build_jobs"
echo "[setup] log=$log_file"
case "$mode" in
  all) run_all ;;
  preflight) run_preflight ;;
  status) report_status ;;
esac
