#!/bin/sh
set -eu

workspace_dir="${OPENCLAW_AGENT_WORKSPACE:-${OPENCLAW_WORKSPACE_DIR:-/root/.openclaw/workspace}}"
template_dir="/opt/openclaw-workspace"

mkdir -p "$workspace_dir"
cp -R "$template_dir"/. "$workspace_dir"/

if [ -d "$workspace_dir/bin" ]; then
  find "$workspace_dir/bin" -type f -exec chmod +x {} \;
fi
