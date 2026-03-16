#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib_paths.sh"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${ROOT_DIR}/.venv"
MANAGE_PY="${ROOT_DIR}/portal/manage.py"

if [ ! -d "${VENV_DIR}" ]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

"${VENV_DIR}/bin/pip" install --upgrade pip wheel
"${VENV_DIR}/bin/pip" install -r "${ROOT_DIR}/portal/requirements.txt"

if [ ! -f "${ROOT_DIR}/.env" ] && [ -f "${ROOT_DIR}/.env.example" ]; then
  cp "${ROOT_DIR}/.env.example" "${ROOT_DIR}/.env"
fi

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

"${VENV_DIR}/bin/python" "${MANAGE_PY}" migrate
"${VENV_DIR}/bin/python" "${MANAGE_PY}" collectstatic --noinput
"${VENV_DIR}/bin/python" "${MANAGE_PY}" ensure_superuser

echo "Native bootstrap completed."
