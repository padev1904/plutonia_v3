# Plutonia Ops

Use this skill when the user asks you to inspect, monitor, or unblock the Plutonia production workflow.

## Workspace
- Repository checkout: `./repo`
- Runtime status endpoint: `http://ainews-gmail-monitor:8001/api/ops/status`
- Runtime action endpoint: `http://ainews-gmail-monitor:8001/api/ops/action`
- Editorial session endpoint: `http://ainews-gmail-monitor:8001/api/editorial/session`
- Editorial action endpoint: `http://ainews-gmail-monitor:8001/api/editorial/action`

## Default workflow
1. Fetch the current runtime snapshot from the ops status endpoint.
2. Fetch the editorial session with `python3 /workspace/bin/editorial_session.py` before replying in Telegram.
3. If an editorial session is active, keep the user in that flow and trigger backend actions with `python3 /workspace/bin/editorial_action.py`.
   Irreversible actions are gated: `approve_preview` and `reject_article` require `--user-request "<latest user message exactly>"`.
   Do not treat "next/continue/resume/retoma/envia a próxima" as approval or rejection.
   If the user asks for the public portal/article link, use `python3 /workspace/bin/portal_public_link.py --text-only`.
4. If runtime action is needed, prefer the ops action endpoint over ad-hoc process intervention.
5. Use shell, git, Python, ripgrep, HTTP fetch, and Docker-internal services freely inside the container when they help.
6. Only edit code under `./repo` when the user is asking for a code or config change.
7. Keep responses concise and include what changed or what remains blocked.

## Editorial shortcuts
- `python3 /workspace/bin/editorial_action.py prepare_preview_process --article-id <id>`
- `python3 /workspace/bin/editorial_action.py prepare_preview_manual --article-id <id> --manual-url <url>`
- `python3 /workspace/bin/editorial_action.py request_changes --article-id <id> --instructions "..." --text-only`
- `python3 /workspace/bin/editorial_action.py approve_preview --article-id <id> --user-request "aprova a publicação" --text-only`
- `python3 /workspace/bin/editorial_action.py reject_article --article-id <id> --user-request "rejeita este artigo" --text-only`
- `python3 /workspace/bin/portal_public_link.py --text-only`

## Safe actions
- `restart_monitor`: request a controlled restart of the gmail monitor loop.
- `notify_next_review`: resend the next article review notification.
- `notify_next_resource_review`: resend the next resource review notification.
- `sync_gmail_labels`: reconcile Gmail labels for `review` or `completed`.
- `recover_review_gate`: repair stale review state and resend notifications when needed.

## Guardrails
- You have full authority inside this container and on mounted Docker volumes.
- Do not assume host Docker access.
- Do not assume host filesystem or host SSH access.
- Do not bypass editorial approval.
- Do not mutate production state unless the user asked for intervention or the workflow is technically blocked.
- If the user asks to advance to the next newsletter while an editorial session is active, explain the current session and ask for an explicit decision instead of auto-approving.
