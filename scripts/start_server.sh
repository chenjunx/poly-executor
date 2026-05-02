#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RELEASE_DIR="$PROJECT_DIR/target/release"
APP_BIN="$RELEASE_DIR/poly-executor"
PID_FILE="$RELEASE_DIR/poly-executor.pid"
STDOUT_LOG="$RELEASE_DIR/poly-executor.stdout.log"

copy_required_file() {
  local src="$1"
  if [[ ! -f "$src" ]]; then
    echo "Missing required file: $src" >&2
    exit 1
  fi
  cp -f "$src" "$RELEASE_DIR/"
}

stop_existing_processes() {
  local pids=""

  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      pids="$pids $pid"
    fi
  fi

  local matched
  matched="$(pgrep -f "$APP_BIN" || true)"
  if [[ -n "$matched" ]]; then
    pids="$pids $matched"
  fi

  pids="$(printf '%s\n' $pids | awk 'NF && !seen[$1]++')"
  if [[ -z "$pids" ]]; then
    rm -f "$PID_FILE"
    return
  fi

  echo "Stopping existing poly-executor process(es): $pids"
  kill $pids 2>/dev/null || true

  for _ in $(seq 1 20); do
    local alive=""
    for pid in $pids; do
      if kill -0 "$pid" 2>/dev/null; then
        alive="$alive $pid"
      fi
    done
    if [[ -z "$alive" ]]; then
      rm -f "$PID_FILE"
      return
    fi
    sleep 1
  done

  echo "Force killing poly-executor process(es): $pids"
  kill -9 $pids 2>/dev/null || true
  rm -f "$PID_FILE"
}

mkdir -p "$RELEASE_DIR"

if [[ ! -x "$APP_BIN" ]]; then
  echo "Missing executable binary: $APP_BIN" >&2
  echo "Please build first: cargo build --release" >&2
  exit 1
fi

copy_required_file "$PROJECT_DIR/assets.csv"
copy_required_file "$PROJECT_DIR/config.toml"
copy_required_file "$PROJECT_DIR/config.local.toml"

shopt -s nullglob
liquidity_files=("$PROJECT_DIR"/liquidity*.csv "$PROJECT_DIR"/liuqui*.csv)
shopt -u nullglob

if ((${#liquidity_files[@]} == 0)); then
  echo "Missing required liquidity reward csv file: $PROJECT_DIR/liquidity*.csv" >&2
  exit 1
fi

for src in "${liquidity_files[@]}"; do
  cp -f "$src" "$RELEASE_DIR/"
done

if [[ -f "$SCRIPT_DIR/logrotate.conf" ]] && command -v logrotate &>/dev/null; then
  cp "$SCRIPT_DIR/logrotate.conf" /etc/logrotate.d/poly-executor 2>/dev/null || true
fi

stop_existing_processes

cd "$RELEASE_DIR"
nohup "$APP_BIN" >> "$STDOUT_LOG" 2>&1 &
new_pid=$!
printf '%s\n' "$new_pid" > "$PID_FILE"

sleep 2
if ! kill -0 "$new_pid" 2>/dev/null; then
  echo "poly-executor failed to stay alive after start. Check log: $STDOUT_LOG" >&2
  exit 1
fi

echo "Started poly-executor with PID $new_pid"
echo "Runtime dir: $RELEASE_DIR"
echo "Stdout log: $STDOUT_LOG"
