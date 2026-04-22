#!/usr/bin/env bash
set -euo pipefail

HOST="root@155.138.132.182"
REMOTE_DIR="/root/poly-executor"
REMOTE_OPS_SCRIPT="$REMOTE_DIR/bin/ops_poly_executor.sh"
ACTION=""
LINES=50
CONFIRM=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/ops_prod.sh status
  bash scripts/ops_prod.sh health
  bash scripts/ops_prod.sh logs-alerts [--lines N]
  bash scripts/ops_prod.sh logs-orders [--lines N]
  bash scripts/ops_prod.sh restart --confirm-prod
EOF
}

if (($# == 0)); then
  usage >&2
  exit 1
fi

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 0
fi

ACTION="$1"
shift

while (($# > 0)); do
  case "$1" in
    --lines)
      if (($# < 2)); then
        echo "Missing value for --lines." >&2
        exit 1
      fi
      LINES="$2"
      shift 2
      ;;
    --confirm-prod)
      CONFIRM=1
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

case "$ACTION" in
  status|health)
    ssh "$HOST" "bash '$REMOTE_OPS_SCRIPT' '$ACTION'"
    ;;
  logs-alerts|logs-orders)
    if ! [[ "$LINES" =~ ^[0-9]+$ ]]; then
      echo "--lines must be an integer." >&2
      exit 1
    fi
    if ((LINES < 1 || LINES > 200)); then
      echo "--lines must be between 1 and 200." >&2
      exit 1
    fi
    ssh "$HOST" "bash '$REMOTE_OPS_SCRIPT' '$ACTION' '$LINES'"
    ;;
  restart)
    if [[ $CONFIRM -ne 1 ]]; then
      echo "Refusing production restart without --confirm-prod." >&2
      exit 1
    fi
    ssh "$HOST" "bash '$REMOTE_OPS_SCRIPT' restart"
    ;;
  *)
    echo "Unsupported action: $ACTION" >&2
    usage >&2
    exit 1
    ;;
esac
