#!/bin/sh
set -eu

. "$(dirname -- "$0")/monitor-common.sh"

ensure_repo_root
cargo_exe=$(resolve_cargo)

stop_monitor_process
sleep 1

start_monitor_stack
printf '%s\n' "Monitor ready at $MONITOR_URL"
exec "$cargo_exe" run -p spreadeater-monitor -- serve --database-url "$DATABASE_URL"
