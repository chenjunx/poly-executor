#!/usr/bin/env bash
set -euo pipefail

HOST="root@155.138.132.182"
REMOTE_DIR="/root/poly-executor"
REMOTE_DEPLOY_SCRIPT="$REMOTE_DIR/bin/deploy_poly_executor.sh"
CONFIRM=0
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: bash scripts/deploy_prod.sh --confirm-prod [--dry-run]

Deploy the current committed HEAD to the fixed production host.
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
  echo "Refusing production deploy without --confirm-prod." >&2
  exit 1
fi

status_output="$(git status --short)"
commit_sha="$(git rev-parse HEAD)"
if [[ -z "$commit_sha" ]]; then
  echo "Failed to resolve HEAD commit." >&2
  exit 1
fi

echo "Deploy target: $HOST:$REMOTE_DIR"
echo "Commit: $commit_sha"

if [[ $DRY_RUN -eq 1 ]]; then
  if [[ -n "$status_output" ]]; then
    echo "Working tree is not clean. A real deploy would be refused."
    echo "$status_output"
  else
    echo "Working tree is clean."
  fi
  echo "Dry run only. Skipping local build and remote deploy."
  exit 0
fi

if [[ -n "$status_output" ]]; then
  echo "Refusing deploy because the working tree is not clean." >&2
  echo "$status_output" >&2
  exit 1
fi

cargo build --release
ssh "$HOST" "bash '$REMOTE_DEPLOY_SCRIPT' '$commit_sha'"
