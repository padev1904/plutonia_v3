# Internal Endpoints

- Ops status: `GET http://ainews-gmail-monitor:8001/api/ops/status`
- Ops action: `POST http://ainews-gmail-monitor:8001/api/ops/action`
- Editorial session: `GET http://ainews-gmail-monitor:8001/api/editorial/session`
- Editorial action: `POST http://ainews-gmail-monitor:8001/api/editorial/action`
- Review API health: `GET http://ainews-gmail-monitor:8001/healthz`
- Portal health: `GET http://portal:8000/healthz`
- Ops runner health: `GET http://ops-runner:8011/healthz`
- Ops runner status: `GET http://ops-runner:8011/status`
- Ops runner deploy: `POST http://ops-runner:8011/deploy`
- Ops runner rollback: `POST http://ops-runner:8011/rollback`
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
  - `python3 /workspace/bin/repo_status.py --text-only`
  - `python3 /workspace/bin/promote_stack.py --message "..." --service portal --health-url http://portal:8000/healthz`
    - waits for health and auto-rolls back to the previous live ref if final health verification fails
  - `python3 /workspace/bin/repo_commit_push.py --message "<commit message>"`
  - `python3 /workspace/bin/deploy_stack.py --service <service> [--service <service> ...]`
  - `python3 /workspace/bin/rollback_stack.py --ref <git-ref> --service <service> [--service <service> ...]`

Use header `X-Ops-Token: $OPS_API_TOKEN` for ops endpoints when `OPS_API_TOKEN` is set.
Use header `X-Ops-Runner-Token: $OPS_RUNNER_TOKEN` for ops-runner endpoints when `OPS_RUNNER_TOKEN` is set.

Safe runtime actions currently available:
- `restart_monitor`
- `notify_next_review`
- `notify_next_resource_review`
- `sync_gmail_labels`
- `recover_review_gate`

Mandatory promotion flow for production changes:
1. Edit code under `/workspace/repo`.
2. Before editing broadly, map the requested behavior to the exact files that must change.
3. Implement one vertical slice at a time. Avoid leaving placeholder templates/routes/buttons without the matching backend behavior.
4. Run the relevant validation commands locally in the workspace after each substantive edit batch.
5. Inspect the exact diff for the touched files before expanding scope.
6. If a touched file is inconsistent or fails validation twice, repair that file before editing other files.
7. Prefer `promote_stack.py` for commit/push/deploy in one step.
8. If you split the flow manually, commit and push with `repo_commit_push.py`.
9. Deploy with `deploy_stack.py`.
10. Confirm live status with `repo_status.py` and health endpoints.
11. If health degrades, roll back with `rollback_stack.py`.
