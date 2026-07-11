#!/bin/sh

MONITOR_URL="http://127.0.0.1:8080"
DATABASE_URL="postgres://postgres:postgres@127.0.0.1:54329/spreadeater_monitor"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

ensure_repo_root() {
  cd "$REPO_ROOT" || exit 1
}

ensure_cargo_path() {
  if command -v cargo >/dev/null 2>&1; then
    return 0
  fi

  for cargo_dir in /opt/homebrew/opt/rustup/bin "$HOME/.cargo/bin" /usr/local/bin; do
    if [ -x "$cargo_dir/cargo" ]; then
      PATH="$cargo_dir:$PATH"
      export PATH
      return 0
    fi
  done

  return 1
}

resolve_cargo() {
  ensure_cargo_path || {
    printf '%s\n' "cargo was not found. Install Rust or add cargo to PATH." >&2
    return 1
  }

  command -v cargo
}

is_monitor_running() {
  pgrep -x spreadeater-monitor >/dev/null 2>&1
}

stop_monitor_process() {
  pkill -x spreadeater-monitor >/dev/null 2>&1 || true
}

start_monitor_stack() {
  docker compose -f ./docker-compose.monitor.yml up -d
}

stop_monitor_stack() {
  docker compose -f ./docker-compose.monitor.yml down
}
