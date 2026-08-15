#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "${script_dir}/../../.." && pwd)
build_dir="${TMPDIR:-/tmp}/mooncake-cxl-todo1-tests"
cxx=${CXX:-c++}

all_targets=(
  cxl_pool_backend_preflight_test
  cxl_pool_backend_functional_test
  cxl_pool_backend_failure_test
  cxl_pool_backend_status_test
  cxl_pool_backend_devdax_test
)

portable_targets=(
  cxl_pool_backend_preflight_test
  cxl_pool_backend_functional_test
  cxl_pool_backend_failure_test
  cxl_pool_backend_status_test
)

if [[ $# -gt 0 ]]; then
  targets=("$@")
else
  targets=("${portable_targets[@]}")
fi

mkdir -p "${build_dir}"
for target in "${targets[@]}"; do
  known=0
  for candidate in "${all_targets[@]}"; do
    if [[ "${candidate}" == "${target}" ]]; then
      known=1
      break
    fi
  done
  if [[ ${known} -ne 1 ]]; then
    echo "Unknown TODO 1 test target: ${target}" >&2
    exit 2
  fi

  tier=T0
  if [[ "${target}" == "cxl_pool_backend_devdax_test" ]]; then
    tier=T2
  fi
  echo "BUILD target=${target} tier=${tier} compiler=${cxx}"
  "${cxx}" -std=c++20 -Wall -Wextra -Werror -pthread \
    -I"${repo_dir}/mooncake-common/include" \
    -I"${script_dir}" \
    "${repo_dir}/mooncake-common/src/cxl_pool_backend.cpp" \
    "${script_dir}/${target}.cpp" \
    -o "${build_dir}/${target}"
  "${build_dir}/${target}"
done
