#!/bin/sh
set -eu

mkdir -p /etc/searxng
cp /usr/local/share/searxng/settings.yml /etc/searxng/settings.yml

exec /usr/local/searxng/entrypoint.sh "$@"
