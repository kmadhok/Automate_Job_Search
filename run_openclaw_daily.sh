#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_PATH="${VENV_PATH:-$SCRIPT_DIR/.venv311}"
PYTHON_BIN="${PYTHON_BIN:-$VENV_PATH/bin/python}"
LOCK_DIR="${LOCK_DIR:-$SCRIPT_DIR/.run_lock}"
LOCK_TTL_SECONDS="${LOCK_TTL_SECONDS:-21600}"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/output/logs}"

if [[ "$LOCK_DIR" != /* ]]; then
  LOCK_DIR="$SCRIPT_DIR/$LOCK_DIR"
fi

if [[ "$LOCK_DIR" == "/" || -z "$LOCK_DIR" ]]; then
  echo "ERROR: Refusing unsafe LOCK_DIR value: '$LOCK_DIR'"
  exit 2
fi

mkdir -p "$LOG_DIR" "$SCRIPT_DIR/output"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "ERROR: Python executable not found at $PYTHON_BIN"
  exit 2
fi

if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
  set +a
fi

export GMAIL_LABEL="${GMAIL_LABEL:-Linkedln Jobs}"
export MAX_EMAILS="${MAX_EMAILS:-5}"
export MAX_JOB_URLS="${MAX_JOB_URLS:-25}"
export GMAIL_INTERACTIVE_AUTH="${GMAIL_INTERACTIVE_AUTH:-0}"
export JOB_TAGS="${JOB_TAGS:-data scientist,data engineer,ai engineer,data analyst}"

acquire_lock() {
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$$" > "$LOCK_DIR/pid"
    date +%s > "$LOCK_DIR/started_at"
    return 0
  fi

  local now existing_ts age
  now="$(date +%s)"
  existing_ts=0
  if [[ -f "$LOCK_DIR/started_at" ]]; then
    existing_ts="$(cat "$LOCK_DIR/started_at" 2>/dev/null || echo 0)"
  fi

  age=$((now - existing_ts))
  if (( age > LOCK_TTL_SECONDS )); then
    rm -rf "$LOCK_DIR"
    mkdir "$LOCK_DIR"
    echo "$$" > "$LOCK_DIR/pid"
    date +%s > "$LOCK_DIR/started_at"
    return 0
  fi

  echo "Another run is active (lock age: ${age}s). Skipping."
  return 1
}

cleanup_lock() {
  rm -rf "$LOCK_DIR" || true
}

if ! acquire_lock; then
  exit 0
fi

trap cleanup_lock EXIT

timestamp="$(date '+%Y%m%d_%H%M%S')"
log_file="$LOG_DIR/openclaw_run_${timestamp}.log"

{
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Starting OpenClaw daily run"
  echo "GMAIL_LABEL=$GMAIL_LABEL MAX_EMAILS=$MAX_EMAILS MAX_JOB_URLS=$MAX_JOB_URLS"
  echo "JOB_TAGS=$JOB_TAGS"
} >> "$log_file"

set +e
"$PYTHON_BIN" main.py >> "$log_file" 2>&1
status=$?
set -e

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Finished with status=$status" >> "$log_file"
echo "Run log: $log_file"
exit "$status"
