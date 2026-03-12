#!/bin/sh
set -eu

workspace_dir="${OPENCLAW_AGENT_WORKSPACE:-${OPENCLAW_WORKSPACE_DIR:-/root/.openclaw/workspace}}"
template_dir="/opt/openclaw-workspace"

mkdir -p "$workspace_dir"
cp -R "$template_dir"/. "$workspace_dir"/

if [ -d "$workspace_dir/bin" ]; then
  find "$workspace_dir/bin" -type f -exec chmod +x {} \;
fi

for agent_name in main coder reviewer editorial router; do
  template_agent_dir="$template_dir/agents/$agent_name"
  agent_dir="$workspace_dir/agents/$agent_name"
  mkdir -p "$agent_dir"
  if [ -f "$template_agent_dir/AGENTS.md" ]; then
    cp "$template_agent_dir/AGENTS.md" "$agent_dir/AGENTS.md"
  fi
  for shared_name in TOOLS.md HEARTBEAT.md SOUL.md; do
    if [ -f "$workspace_dir/$shared_name" ]; then
      rm -f "$agent_dir/$shared_name"
      cp "$workspace_dir/$shared_name" "$agent_dir/$shared_name"
    fi
  done
  if [ -d "$workspace_dir/bin" ]; then
    ln -sfn "$workspace_dir/bin" "$agent_dir/bin"
  fi
  if [ -d "$workspace_dir/skills" ]; then
    rm -rf "$agent_dir/skills"
    cp -R "$workspace_dir/skills" "$agent_dir/skills"
  fi
  if [ -d "$workspace_dir/repo" ]; then
    ln -sfn "$workspace_dir/repo" "$agent_dir/repo"
  fi
done
