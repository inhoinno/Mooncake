#!/usr/bin/env bash
# Run as a child process. Do not source this file into a login shell.
# Compatibility alias. The canonical launcher owns the interface.
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
exec bash "$script_dir/run_callosum_relay.sh" lab "$@"
