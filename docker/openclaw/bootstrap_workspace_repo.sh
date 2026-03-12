#!/bin/sh
set -eu

repo_url="${OPENCLAW_BOOTSTRAP_REPO_URL:-}"
repo_branch="${OPENCLAW_BOOTSTRAP_REPO_BRANCH:-main}"
repo_depth="${OPENCLAW_BOOTSTRAP_REPO_DEPTH:-1}"
repo_auto_update="${OPENCLAW_BOOTSTRAP_REPO_AUTO_UPDATE:-false}"
workspace_dir="${OPENCLAW_AGENT_WORKSPACE:-${OPENCLAW_WORKSPACE_DIR:-/root/.openclaw/workspace}}"
repo_dir="${OPENCLAW_BOOTSTRAP_REPO_DIR:-$workspace_dir/repo}"

if [ -z "$repo_url" ]; then
  exit 0
fi

mkdir -p "$workspace_dir"
mkdir -p "$(dirname "$repo_dir")"
git config --global --add safe.directory "$repo_dir" || true

depth_args=""
case "$(printf '%s' "$repo_depth" | tr -d '[:space:]')" in
  ""|0) depth_args="" ;;
  *) depth_args="--depth $repo_depth" ;;
esac

if [ -d "$repo_dir/.git" ]; then
  case "$(printf '%s' "$repo_auto_update" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) ;;
    *) exit 0 ;;
  esac
  if [ -n "$(git -C "$repo_dir" status --porcelain 2>/dev/null || true)" ]; then
    echo "openclaw workspace repo is dirty; skipping auto-update: $repo_dir" >&2
    exit 0
  fi
  git -C "$repo_dir" remote set-url origin "$repo_url" || true
  if [ -n "$depth_args" ]; then
    git -C "$repo_dir" fetch $depth_args origin "$repo_branch"
  else
    git -C "$repo_dir" fetch --tags --prune origin "$repo_branch"
  fi
  git -C "$repo_dir" checkout -B "$repo_branch" "origin/$repo_branch"
  git -C "$repo_dir" reset --hard "origin/$repo_branch"
  git -C "$repo_dir" clean -fd
  exit 0
fi

if [ -e "$repo_dir" ] && [ -n "$(ls -A "$repo_dir" 2>/dev/null || true)" ]; then
  echo "openclaw workspace repo dir is not empty; skipping clone: $repo_dir" >&2
  exit 0
fi

rm -rf "$repo_dir"
if [ -n "$depth_args" ]; then
  git clone $depth_args --branch "$repo_branch" "$repo_url" "$repo_dir"
else
  git clone --branch "$repo_branch" "$repo_url" "$repo_dir"
fi
