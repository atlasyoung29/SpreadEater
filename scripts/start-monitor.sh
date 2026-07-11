#!/bin/sh
set -eu

. "$(dirname -- "$0")/monitor-common.sh"

ensure_repo_root
cargo_exe=$(resolve_cargo)

if is_monitor_running; then
  printf '%s\n' "Monitor already running at $MONITOR_URL"
  exit 0
fi

start_monitor_stack
printf '%s\n' "Monitor ready at $MONITOR_URL"
exec "$cargo_exe" run -p spreadeater-monitor -- serve --database-url "$DATABASE_URL"
