#!/bin/sh
set -eu

. "$(dirname -- "$0")/monitor-common.sh"

ensure_repo_root
stop_monitor_process
stop_monitor_stack
printf '%s\n' "Monitor stack stopped."
