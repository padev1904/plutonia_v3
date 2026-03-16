#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"

install -m 0644 "${ROOT_DIR}/deploy/systemd/plutonia-portal.service" "${SYSTEMD_DIR}/plutonia-portal.service"
install -m 0644 "${ROOT_DIR}/deploy/systemd/plutonia-monitor.service" "${SYSTEMD_DIR}/plutonia-monitor.service"

systemctl daemon-reload
systemctl enable plutonia-portal.service
systemctl enable plutonia-monitor.service

echo "Installed systemd units into ${SYSTEMD_DIR}."
