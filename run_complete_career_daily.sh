#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
  set +a
fi

resolve_python_bin() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    printf '%s\n' "$PYTHON_BIN"
    return 0
  fi

  if [[ -n "${VENV_PATH:-}" && -x "${VENV_PATH}/bin/python" ]]; then
    printf '%s\n' "${VENV_PATH}/bin/python"
    return 0
  fi

  if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
    printf '%s\n' "${VIRTUAL_ENV}/bin/python"
    return 0
  fi

  if [[ -x "$SCRIPT_DIR/.venv/bin/python" ]]; then
    printf '%s\n' "$SCRIPT_DIR/.venv/bin/python"
    return 0
  fi

  if [[ -x "$SCRIPT_DIR/.venv311/bin/python" ]]; then
    printf '%s\n' "$SCRIPT_DIR/.venv311/bin/python"
    return 0
  fi

  command -v python3 || true
}

PYTHON_BIN="$(resolve_python_bin)"
LOCK_DIR="${LOCK_DIR:-$SCRIPT_DIR/.run_lock_complete}"
LOCK_TTL_SECONDS="${LOCK_TTL_SECONDS:-43200}"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/output/logs}"

if [[ "$LOCK_DIR" != /* ]]; then
  LOCK_DIR="$SCRIPT_DIR/$LOCK_DIR"
fi

if [[ "$LOCK_DIR" == "/" || -z "$LOCK_DIR" ]]; then
  echo "ERROR: Refusing unsafe LOCK_DIR value: '$LOCK_DIR'"
  exit 2
fi

mkdir -p "$LOG_DIR" "$SCRIPT_DIR/output"

if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
  echo "ERROR: Python executable not found. Set PYTHON_BIN or VENV_PATH in .env."
  exit 2
fi

export GMAIL_INTERACTIVE_AUTH="${GMAIL_INTERACTIVE_AUTH:-0}"
COMPLETE_PIPELINE_TRIGGER="${COMPLETE_PIPELINE_TRIGGER:-run complete package}"
COMPLETE_PIPELINE_SHEET="${COMPLETE_PIPELINE_SHEET:-}"
COMPLETE_PIPELINE_NON_INTERACTIVE_SHEETS="${COMPLETE_PIPELINE_NON_INTERACTIVE_SHEETS:-1}"
COMPLETE_PIPELINE_JOBS_TAB="${COMPLETE_PIPELINE_JOBS_TAB:-jobs}"
COMPLETE_PIPELINE_JOB_CONTACTS_TAB="${COMPLETE_PIPELINE_JOB_CONTACTS_TAB:-job_contacts}"
COMPLETE_PIPELINE_JOB_MESSAGES_TAB="${COMPLETE_PIPELINE_JOB_MESSAGES_TAB:-job_messages}"
COMPLETE_PIPELINE_RESUME_TAB="${COMPLETE_PIPELINE_RESUME_TAB:-resume_tailoring}"
COMPLETE_PIPELINE_NETWORKING_CONTACTS_TAB="${COMPLETE_PIPELINE_NETWORKING_CONTACTS_TAB:-networking_contacts}"
COMPLETE_PIPELINE_NETWORKING_MESSAGES_TAB="${COMPLETE_PIPELINE_NETWORKING_MESSAGES_TAB:-networking_messages}"

if [[ -z "$COMPLETE_PIPELINE_SHEET" ]]; then
  echo "ERROR: COMPLETE_PIPELINE_SHEET is not set. Add it to .env."
  exit 2
fi

if [[ -z "${SERPER_DEV_API_KEY:-}" && -z "${SERPER_API_KEY:-}" ]]; then
  echo "ERROR: SERPER_DEV_API_KEY or SERPER_API_KEY must be set for the complete pipeline."
  exit 2
fi

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

  echo "Another complete pipeline run is active (lock age: ${age}s). Skipping."
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
log_file="$LOG_DIR/complete_pipeline_${timestamp}.log"

cmd=(
  "$PYTHON_BIN"
  "$SCRIPT_DIR/run_complete_career_pipeline.py"
  --trigger "$COMPLETE_PIPELINE_TRIGGER"
  --sheet "$COMPLETE_PIPELINE_SHEET"
  --jobs-tab "$COMPLETE_PIPELINE_JOBS_TAB"
  --job-contacts-tab "$COMPLETE_PIPELINE_JOB_CONTACTS_TAB"
  --job-messages-tab "$COMPLETE_PIPELINE_JOB_MESSAGES_TAB"
  --resume-tab "$COMPLETE_PIPELINE_RESUME_TAB"
  --networking-contacts-tab "$COMPLETE_PIPELINE_NETWORKING_CONTACTS_TAB"
  --networking-messages-tab "$COMPLETE_PIPELINE_NETWORKING_MESSAGES_TAB"
)

if [[ "$COMPLETE_PIPELINE_NON_INTERACTIVE_SHEETS" == "1" ]]; then
  cmd+=(--non-interactive-sheets)
fi

{
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Starting complete career pipeline"
  echo "GMAIL_INTERACTIVE_AUTH=$GMAIL_INTERACTIVE_AUTH"
  echo "COMPLETE_PIPELINE_SHEET=$COMPLETE_PIPELINE_SHEET"
  echo "COMPLETE_PIPELINE_TRIGGER=$COMPLETE_PIPELINE_TRIGGER"
  echo "COMPLETE_PIPELINE_NON_INTERACTIVE_SHEETS=$COMPLETE_PIPELINE_NON_INTERACTIVE_SHEETS"
  printf 'COMMAND='
  printf '%q ' "${cmd[@]}"
  printf '\n'
} >> "$log_file"

set +e
"${cmd[@]}" >> "$log_file" 2>&1
status=$?
set -e

echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Finished with status=$status" >> "$log_file"
echo "Run log: $log_file"
exit "$status"
