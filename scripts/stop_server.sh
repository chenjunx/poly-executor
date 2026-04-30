#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RELEASE_DIR="$PROJECT_DIR/target/release"
APP_BIN="$RELEASE_DIR/poly-executor"
PID_FILE="$RELEASE_DIR/poly-executor.pid"

pids=""

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    pids="$pids $pid"
  fi
fi

matched="$(pgrep -f "$APP_BIN" || true)"
if [[ -n "$matched" ]]; then
  pids="$pids $matched"
fi

pids="$(printf '%s\n' $pids | awk 'NF && !seen[$1]++')"
if [[ -z "$pids" ]]; then
  rm -f "$PID_FILE"
  echo "poly-executor is not running."
  exit 0
fi

echo "Stopping poly-executor process(es): $pids"
kill $pids 2>/dev/null || true

for _ in $(seq 1 20); do
  alive=""
  for pid in $pids; do
    if kill -0 "$pid" 2>/dev/null; then
      alive="$alive $pid"
    fi
  done
  if [[ -z "$alive" ]]; then
    rm -f "$PID_FILE"
    echo "poly-executor stopped."
    exit 0
  fi
  sleep 1
done

echo "Force killing poly-executor process(es): $pids"
kill -9 $pids 2>/dev/null || true
rm -f "$PID_FILE"
echo "poly-executor stopped."
