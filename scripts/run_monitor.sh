#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib_paths.sh"

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"
exec "${PYTHON_BIN}" "${ROOT_DIR}/scripts/gmail_monitor_supervisor.py" -- \
  "${PYTHON_BIN}" "${ROOT_DIR}/scripts/gmail_monitor.py" --mode monitor
