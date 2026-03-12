# Internal Endpoints

- Ops status: `GET http://ainews-gmail-monitor:8001/api/ops/status`
- Ops action: `POST http://ainews-gmail-monitor:8001/api/ops/action`
- Editorial session: `GET http://ainews-gmail-monitor:8001/api/editorial/session`
- Editorial action: `POST http://ainews-gmail-monitor:8001/api/editorial/action`
- Review API health: `GET http://ainews-gmail-monitor:8001/healthz`
- Portal health: `GET http://portal:8000/healthz`
- SearXNG: `http://searxng:8080`
- Ollama: `http://ollama:11434`

Local authority inside the container:
- Writable workspace: `/workspace`
- Writable review volume: `/review`
- Writable shared logs: `/logs`
- Available CLI tools: `bash`, `git`, `ssh`, `curl`, `jq`, `rg`, `python3`, `pip`, `psql`, `nc`, `ping`, `rsync`
- Editorial helper scripts:
  - `python3 /workspace/bin/editorial_session.py`
  - `python3 /workspace/bin/editorial_session.py --text`
  - `python3 /workspace/bin/editorial_action.py <action> ...`
    - `approve_preview` and `reject_article` require `--user-request "<latest user message exactly>"`
    - generic "next/continue/resume" requests must not be treated as approval/rejection
  - `python3 /workspace/bin/portal_public_link.py --text-only`

Use header `X-Ops-Token: $OPS_API_TOKEN` for ops endpoints when `OPS_API_TOKEN` is set.

Safe runtime actions currently available:
- `restart_monitor`
- `notify_next_review`
- `notify_next_resource_review`
- `sync_gmail_labels`
- `recover_review_gate`
