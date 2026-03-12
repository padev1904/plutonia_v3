#!/bin/sh
set -eu

/usr/local/bin/bootstrap_workspace_repo.sh
/usr/local/bin/seed_workspace.sh
node /usr/local/bin/init_openclaw_config.mjs

exec "$@"
