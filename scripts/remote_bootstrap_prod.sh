#!/usr/bin/env bash
set -euo pipefail

HOST="root@155.138.132.182"
REMOTE_DIR="/root/poly-executor"
REMOTE_BIN_DIR="$REMOTE_DIR/bin"
CONFIRM=0
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: bash scripts/remote_bootstrap_prod.sh --confirm-prod [--dry-run]

Bootstrap the fixed production host so it can deploy from git and manage the process with fixed scripts.
EOF
}

while (($# > 0)); do
  case "$1" in
    --confirm-prod)
      CONFIRM=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unsupported argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ $CONFIRM -ne 1 ]]; then
  echo "Refusing production bootstrap without --confirm-prod." >&2
  exit 1
fi

origin_url="$(git remote get-url origin 2>/dev/null || true)"
if [[ -z "$origin_url" ]]; then
  echo "Git remote 'origin' is not configured. Configure it locally before bootstrapping production." >&2
  exit 1
fi

read -r -d '' REMOTE_SCRIPT <<'EOF' || true
#!/usr/bin/env bash
set -euo pipefail

REMOTE_DIR="/root/poly-executor"
BIN_DIR="$REMOTE_DIR/bin"
RUN_DIR="$REMOTE_DIR/run"
LOG_DIR="$REMOTE_DIR/logs"
PID_FILE="$RUN_DIR/poly-executor.pid"
STDOUT_LOG="$LOG_DIR/stdout.log"
ALERT_LOG="$REMOTE_DIR/alerts.log"
ORDER_LOG="$REMOTE_DIR/orders.log"
REPO_URL="$1"

mkdir -p "$BIN_DIR" "$RUN_DIR" "$LOG_DIR"
cd "$REMOTE_DIR"

if [[ ! -d .git ]]; then
  git init
fi

current_origin="$(git remote get-url origin 2>/dev/null || true)"
if [[ -z "$current_origin" ]]; then
  git remote add origin "$REPO_URL"
elif [[ "$current_origin" != "$REPO_URL" ]]; then
  git remote set-url origin "$REPO_URL"
fi

cat > "$BIN_DIR/start_poly_executor.sh" <<'INNER'
#!/usr/bin/env bash
set -euo pipefail

REMOTE_DIR="/root/poly-executor"
RUN_DIR="$REMOTE_DIR/run"
LOG_DIR="$REMOTE_DIR/logs"
PID_FILE="$RUN_DIR/poly-executor.pid"
STDOUT_LOG="$LOG_DIR/stdout.log"
APP_BIN="$REMOTE_DIR/target/release/poly-executor"

mkdir -p "$RUN_DIR" "$LOG_DIR"
cd "$REMOTE_DIR"

required_files=("config.toml" "assets.csv")
for path in "${required_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    echo "Missing required file: $REMOTE_DIR/$path" >&2
    exit 1
  fi
done

if grep -Eq '^[[:space:]]*enabled[[:space:]]*=[[:space:]]*true' "$REMOTE_DIR/config.toml" && grep -Eq '^\[liquidity_reward\]' "$REMOTE_DIR/config.toml"; then
  if [[ ! -f "$REMOTE_DIR/liquidity_reward.csv" ]]; then
    echo "Missing required file: $REMOTE_DIR/liquidity_reward.csv" >&2
    exit 1
  fi
fi

if [[ ! -x "$APP_BIN" ]]; then
  echo "Missing built binary: $APP_BIN" >&2
  exit 1
fi

if [[ -f "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE")"
  if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
    echo "Process already running with PID $old_pid" >&2
    exit 1
  fi
  rm -f "$PID_FILE"
fi

nohup "$APP_BIN" >> "$STDOUT_LOG" 2>&1 &
new_pid=$!
printf '%s\n' "$new_pid" > "$PID_FILE"
sleep 2
if ! kill -0 "$new_pid" 2>/dev/null; then
  echo "Process failed to stay alive after start." >&2
  exit 1
fi

echo "Started poly-executor with PID $new_pid"
INNER
chmod +x "$BIN_DIR/start_poly_executor.sh"

cat > "$BIN_DIR/restart_poly_executor.sh" <<'INNER'
#!/usr/bin/env bash
set -euo pipefail

REMOTE_DIR="/root/poly-executor"
RUN_DIR="$REMOTE_DIR/run"
PID_FILE="$RUN_DIR/poly-executor.pid"
START_SCRIPT="$REMOTE_DIR/bin/start_poly_executor.sh"

if [[ -f "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE")"
  if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
    kill "$old_pid"
    for _ in $(seq 1 20); do
      if ! kill -0 "$old_pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "Old process $old_pid did not exit cleanly." >&2
      exit 1
    fi
  fi
  rm -f "$PID_FILE"
fi

bash "$START_SCRIPT"
INNER
chmod +x "$BIN_DIR/restart_poly_executor.sh"

cat > "$BIN_DIR/ops_poly_executor.sh" <<'INNER'
#!/usr/bin/env bash
set -euo pipefail

REMOTE_DIR="/root/poly-executor"
RUN_DIR="$REMOTE_DIR/run"
PID_FILE="$RUN_DIR/poly-executor.pid"
ALERT_LOG="$REMOTE_DIR/alerts.log"
ORDER_LOG="$REMOTE_DIR/orders.log"
ACTION="${1:-}"
LINES="${2:-50}"

status() {
  cd "$REMOTE_DIR"
  commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "status=running pid=$pid commit=$commit"
      exit 0
    fi
    echo "status=stale-pid pid=$pid commit=$commit"
    exit 1
  fi
  echo "status=stopped commit=$commit"
  exit 1
}

case "$ACTION" in
  status)
    status
    ;;
  health)
    status
    ;;
  logs-alerts)
    tail -n "$LINES" "$ALERT_LOG"
    ;;
  logs-orders)
    tail -n "$LINES" "$ORDER_LOG"
    ;;
  restart)
    bash "$REMOTE_DIR/bin/restart_poly_executor.sh"
    ;;
  *)
    echo "Unsupported action: $ACTION" >&2
    exit 1
    ;;
esac
INNER
chmod +x "$BIN_DIR/ops_poly_executor.sh"

cat > "$BIN_DIR/deploy_poly_executor.sh" <<'INNER'
#!/usr/bin/env bash
set -euo pipefail

REMOTE_DIR="/root/poly-executor"
DEPLOY_SHA="${1:-}"
RESTART_SCRIPT="$REMOTE_DIR/bin/restart_poly_executor.sh"
OPS_SCRIPT="$REMOTE_DIR/bin/ops_poly_executor.sh"

if [[ -z "$DEPLOY_SHA" ]]; then
  echo "Usage: bash deploy_poly_executor.sh <commit-sha>" >&2
  exit 1
fi

cd "$REMOTE_DIR"
required_files=("config.toml" "assets.csv")
for path in "${required_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    echo "Missing required file: $REMOTE_DIR/$path" >&2
    exit 1
  fi
done

if grep -Eq '^\[liquidity_reward\]' "$REMOTE_DIR/config.toml" && grep -Eq '^[[:space:]]*enabled[[:space:]]*=[[:space:]]*true' "$REMOTE_DIR/config.toml"; then
  if [[ ! -f "$REMOTE_DIR/liquidity_reward.csv" ]]; then
    echo "Missing required file: $REMOTE_DIR/liquidity_reward.csv" >&2
    exit 1
  fi
fi

git fetch origin
if ! git rev-parse --verify "$DEPLOY_SHA^{commit}" >/dev/null 2>&1; then
  echo "Commit not available after fetch: $DEPLOY_SHA" >&2
  exit 1
fi

git checkout --detach "$DEPLOY_SHA"
cargo build --release

if [[ -f "$REMOTE_DIR/scripts/logrotate.conf" ]]; then
  cp "$REMOTE_DIR/scripts/logrotate.conf" /etc/logrotate.d/poly-executor
fi

bash "$RESTART_SCRIPT"
bash "$OPS_SCRIPT" status
INNER
chmod +x "$BIN_DIR/deploy_poly_executor.sh"

cat > /etc/logrotate.d/poly-executor <<'LOGROTATE'
/root/poly-executor/orders.log /root/poly-executor/alerts.log {
    size 20M
    rotate 5
    copytruncate
    compress
    delaycompress
    missingok
    notifempty
}
LOGROTATE

echo "Bootstrap complete in $REMOTE_DIR"
EOF

if [[ $DRY_RUN -eq 1 ]]; then
  echo "Bootstrap target: $HOST:$REMOTE_DIR"
  echo "Origin URL: $origin_url"
  echo "Dry run only. Skipping remote changes."
  exit 0
fi

ssh "$HOST" "bash -s -- '$origin_url'" <<<"$REMOTE_SCRIPT"
