#!/usr/bin/env bash
# Prepare a stable, reusable yalantinglibs installation for Mooncake lab builds.
#
# This deliberately does not reuse a CMake build tree. A copied build directory
# or source files dated ahead of the host clock can make Ninja regenerate its
# manifest forever ("build.ninja still dirty after 100 tries"). Instead, TODO#0
# configures a timestamp-normalized source snapshot with Unix Makefiles and
# atomically publishes a revision-keyed install prefix.
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ylt_source_dir="$repo_dir/extern/yalantinglibs"
target_build_dir="${MOONCAKE_BUILD_DIR:-build-todo1-cpu}"
prefix_root="${MOONCAKE_YALANTINGLIBS_PREFIX_ROOT:-$repo_dir/$target_build_dir/_deps/yalantinglibs-install}"

cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
if [ "$cpu_count" -gt 16 ]; then
  default_jobs=16
else
  default_jobs="$cpu_count"
fi
build_jobs="${MOONCAKE_BUILD_JOBS:-$default_jobs}"

usage() {
  cat <<'EOF'
Usage: bash scripts/bootstrap_todo0_lab.sh [options]

Build and persist the pinned yalantinglibs dependency without reusing a CMake
build manifest. The resulting installation is safe to share with TODO#1.

Options:
  --prefix-root DIR   Revision-keyed install root. Relative paths are resolved
                      inside the repository (default: build-todo1-cpu/_deps/
                      yalantinglibs-install).
  --jobs N            Build parallelism (default: min(host CPUs, 16)).
  -h, --help          Show this help.

When called by build_todo1_overlay.sh, the prefix root is placed under that
build's _deps directory and selected automatically; no environment activation
or system-wide installation is required.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix-root)
      [ "$#" -ge 2 ] || { echo "[FATAL] --prefix-root requires a value" >&2; exit 2; }
      prefix_root="$2"
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

case "$build_jobs" in
  ""|*[!0-9]*|0)
    echo "[FATAL] --jobs must be a positive integer" >&2
    exit 2
    ;;
esac

case "$prefix_root" in
  /*) ;;
  *) prefix_root="$repo_dir/$prefix_root" ;;
esac
case "$prefix_root/" in
  "$repo_dir/"*) ;;
  *)
    echo "[FATAL] --prefix-root must stay inside $repo_dir" >&2
    exit 2
    ;;
esac

[ "$(uname -s)" = "Linux" ] || {
  echo "[FATAL] TODO#0 must run on the Linux lab host" >&2
  exit 2
}
for command_name in cmake make c++ git sha256sum; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "[FATAL] required command is unavailable: $command_name" >&2
    echo "        Run scripts/bootstrap_todo1_lab.sh to install system packages." >&2
    exit 2
  }
done

ylt_required_files=( CMakeLists.txt cmake/build.cmake cmake/install.cmake )
for required_file in "${ylt_required_files[@]}"; do
  [ -f "$ylt_source_dir/$required_file" ] || {
    echo "[FATAL] incomplete yalantinglibs checkout: $required_file" >&2
    echo "        git submodule update --init --recursive extern/yalantinglibs" >&2
    exit 2
  }
done

git -C "$ylt_source_dir" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
  echo "[FATAL] yalantinglibs must be an initialized Git submodule" >&2
  exit 2
}
source_revision="$(git -C "$ylt_source_dir" rev-parse --verify HEAD)"
source_revision="${source_revision:0:16}"
source_key="${source_revision}-todo0-v1"
if ! git -C "$ylt_source_dir" diff --quiet HEAD --; then
  dirty_hash="$(git -C "$ylt_source_dir" diff --no-ext-diff --binary HEAD -- |
    sha256sum | cut -c1-12)"
  source_key="${source_revision}-dirty-${dirty_hash}-todo0-v1"
fi
version_prefix="$prefix_root/$source_key"
config_dir="$version_prefix/lib/cmake/yalantinglibs"
current_link="$prefix_root/current"

normalize_future_install_mtimes() {
  local install_root="$1"
  local now future_reference future_count

  [ -d "$install_root" ] || return 0
  now="$(date +%s)"
  future_reference="$(mktemp /tmp/mooncake-ylt-future-mtime.XXXXXX)"
  touch -d "@$((now + 5))" "$future_reference"
  future_count="$(find "$install_root" -type f -newer "$future_reference" |
    wc -l)"
  if [ "$future_count" -gt 0 ]; then
    find "$install_root" -type f -newer "$future_reference" \
      -exec touch -m -- {} +
    echo "[setup] normalized future yalantinglibs install mtimes: files=$future_count"
  fi
  rm -f "$future_reference"
}

publish_current_link() {
  if [ -e "$current_link" ] && [ ! -L "$current_link" ]; then
    echo "[FATAL] refusing to replace non-symlink path: $current_link" >&2
    return 1
  fi
  if [ -L "$current_link" ] &&
     [ "$(readlink "$current_link")" = "$source_key" ]; then
    return 0
  fi
  ln -sfn "$source_key" "$current_link"
}

mkdir -p "$prefix_root"
if [ -f "$config_dir/yalantinglibsConfig.cmake" ]; then
  normalize_future_install_mtimes "$version_prefix"
  publish_current_link
  echo "[PASS] gate=yalantinglibs detail=reused=$config_dir revision=$source_revision"
  exit 0
fi

scratch_root="$repo_dir/build-todo0-cpu/_scratch"
mkdir -p "$scratch_root"
scratch_dir="$(mktemp -d "$scratch_root/yalantinglibs.${source_key}.XXXXXX")"
snapshot_dir="$scratch_dir/source"
build_dir="$scratch_dir/build"
staged_prefix="$scratch_dir/install"
completed=0

cleanup() {
  local status="$?"
  trap - EXIT
  if [ "$completed" -eq 1 ]; then
    rm -rf -- "$scratch_dir"
  else
    echo "[diagnose] preserved TODO#0 scratch directory: $scratch_dir" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$snapshot_dir"
cp -R "$ylt_source_dir/." "$snapshot_dir/"
rm -f "$snapshot_dir/.git"
# A copied checkout can contain future mtimes from another lab node. Normalize
# regular files before CMake observes them; the original checkout is untouched.
find "$snapshot_dir" -type f -exec touch {} +

echo "[setup] TODO#0 source=$ylt_source_dir revision=$source_revision"
echo "[setup] TODO#0 scratch=$scratch_dir prefix=$version_prefix jobs=$build_jobs"
cmake \
  -S "$snapshot_dir" \
  -B "$build_dir" \
  -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$staged_prefix" \
  -DYLT_ENABLE_CUDA=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DBUILD_BENCHMARK=OFF \
  -DBUILD_UNIT_TESTS=OFF
cmake --build "$build_dir" --parallel "$build_jobs"
cmake --install "$build_dir"

staged_config="$staged_prefix/lib/cmake/yalantinglibs/yalantinglibsConfig.cmake"
[ -f "$staged_config" ] || {
  echo "[FATAL] staged yalantinglibs config is missing: $staged_config" >&2
  exit 1
}

if [ -e "$version_prefix" ]; then
  invalid_prefix="${version_prefix}.incomplete.$(date -u +%Y%m%dT%H%M%SZ)"
  mv "$version_prefix" "$invalid_prefix"
  echo "[setup] preserved incomplete install as $invalid_prefix"
fi
mv "$staged_prefix" "$version_prefix"
publish_current_link
completed=1

echo "[PASS] gate=yalantinglibs detail=installed=$config_dir revision=$source_revision"
echo "[PASS] cmake_dir=$prefix_root/current/lib/cmake/yalantinglibs"
