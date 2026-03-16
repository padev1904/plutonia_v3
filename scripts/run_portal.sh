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
GUNICORN_BIN="${GUNICORN_BIN:-${ROOT_DIR}/.venv/bin/gunicorn}"

cd "${ROOT_DIR}/portal"
"${PYTHON_BIN}" manage.py migrate
"${PYTHON_BIN}" manage.py collectstatic --noinput
"${PYTHON_BIN}" manage.py ensure_superuser
exec "${GUNICORN_BIN}" config.wsgi:application --bind 127.0.0.1:8000 --workers 4 --timeout 120
