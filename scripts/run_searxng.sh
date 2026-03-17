#!/usr/bin/env bash
# Arranca o SearXNG nativo.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEARXNG_SRC="${ROOT_DIR}/searxng/src"
SEARXNG_VENV="${ROOT_DIR}/searxng/.venv"

# Carregar .env
if [ -f "${ROOT_DIR}/.env" ]; then
    set -a; source "${ROOT_DIR}/.env"; set +a
fi

export SEARXNG_SETTINGS_PATH="${ROOT_DIR}/searxng/settings.yml"

cd "${SEARXNG_SRC}"
exec "${SEARXNG_VENV}/bin/python" -m searx.webapp
