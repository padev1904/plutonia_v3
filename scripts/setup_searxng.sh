#!/usr/bin/env bash
# Instala o SearXNG nativamente dentro do projecto plutonia_v3.
# Clonar código → criar venv dedicado → instalar dependências.
# Executar uma vez no servidor Linux após deploy.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEARXNG_SRC="${ROOT_DIR}/searxng/src"
SEARXNG_VENV="${ROOT_DIR}/searxng/.venv"
SETTINGS_YML="${ROOT_DIR}/searxng/settings.yml"

echo "=== Setup SearXNG nativo em ${SEARXNG_SRC} ==="

# 1. Clonar repo SearXNG se ainda não existir
if [ ! -d "${SEARXNG_SRC}/.git" ]; then
    echo "→ Cloning SearXNG..."
    git clone --depth=1 https://github.com/searxng/searxng.git "${SEARXNG_SRC}"
else
    echo "→ SearXNG já clonado, a actualizar..."
    git -C "${SEARXNG_SRC}" pull --ff-only
fi

# 2. Criar venv dedicado para o SearXNG
if [ ! -d "${SEARXNG_VENV}" ]; then
    echo "→ Criar venv: ${SEARXNG_VENV}"
    python3 -m venv "${SEARXNG_VENV}"
fi

echo "→ Instalar dependências SearXNG..."
"${SEARXNG_VENV}/bin/pip" install --upgrade pip wheel
"${SEARXNG_VENV}/bin/pip" install -r "${SEARXNG_SRC}/requirements.txt"

# 3. Ligar o settings.yml do projecto ao diretório do SearXNG
LINKED="${SEARXNG_SRC}/searx/settings.yml"
if [ ! -L "${LINKED}" ] && [ ! -f "${LINKED}" ]; then
    ln -sf "${SETTINGS_YML}" "${LINKED}"
    echo "→ settings.yml linked"
fi

# 4. Substituir secret_key placeholder se SEARXNG_SECRET estiver no .env
if [ -f "${ROOT_DIR}/.env" ]; then
    # shellcheck disable=SC1091
    set -a; source "${ROOT_DIR}/.env"; set +a
fi

if [ -n "${SEARXNG_SECRET:-}" ]; then
    # Injectar a variável de ambiente — o settings.yml já usa ${SEARXNG_SECRET}
    echo "→ SEARXNG_SECRET disponível (${#SEARXNG_SECRET} chars)"
fi

echo ""
echo "=== SearXNG instalado. Para arrancar manualmente: ==="
echo "    ${ROOT_DIR}/scripts/run_searxng.sh"
echo ""
echo "=== Para instalar como serviço systemd: ==="
echo "    sudo cp ${ROOT_DIR}/deploy/systemd/plutonia-searxng.service /etc/systemd/system/"
echo "    sudo systemctl daemon-reload"
echo "    sudo systemctl enable --now plutonia-searxng"
