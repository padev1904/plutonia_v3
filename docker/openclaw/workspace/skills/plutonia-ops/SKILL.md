# Plutonia Ops

Use this skill when the user asks you to inspect, monitor, or unblock the Plutonia production workflow.

## Workspace
- Repository checkout: `./repo`
- Runtime status endpoint: `http://ainews-gmail-monitor:8001/api/ops/status`
- Runtime action endpoint: `http://ainews-gmail-monitor:8001/api/ops/action`
- Editorial session endpoint: `http://ainews-gmail-monitor:8001/api/editorial/session`
- Editorial action endpoint: `http://ainews-gmail-monitor:8001/api/editorial/action`

## Default workflow
1. Fetch the current runtime snapshot with `python3 /workspace/bin/ops_status.py`.
2. Fetch the editorial session with `python3 /workspace/bin/editorial_session.py` before replying in Telegram.
3. If an editorial session is active, keep the user in that flow and trigger backend actions with `python3 /workspace/bin/editorial_action.py`.
   Irreversible actions are gated: `approve_preview` and `reject_article` require `--user-request "<latest user message exactly>"`.
   Do not treat "next/continue/resume/retoma/envia a próxima" as approval or rejection.
   If the user asks for the public portal/article link, use `python3 /workspace/bin/portal_public_link.py --text-only`.
4. If runtime action is needed, prefer `python3 /workspace/bin/ops_action.py ...` over raw HTTP or ad-hoc process intervention.
5. Use shell, git, Python, ripgrep, HTTP fetch, and Docker-internal services freely inside the container when they help.
6. Only edit code under `./repo` when the user is asking for a code or config change.
7. For multi-file changes, map the requested behavior to the exact files first. Then complete one vertical slice at a time instead of scattering placeholder changes across the repo.
8. After each substantive edit batch, inspect the diff for the touched files and run targeted validation before expanding scope.
9. If a touched file becomes inconsistent or fails validation twice, stop broadening the change. Repair that file first.
10. For production code changes, follow this order strictly:
   `edit -> validate -> promote_stack.py -> verify`.
   Use `repo_commit_push.py` plus `deploy_stack.py` separately only when you intentionally need the split flow.
11. If the repo is clean but `git status -sb` shows `ahead`, treat the unpublished local commit as the current work item. Inspect it with `git show --stat -1` and `git diff --name-only origin/main..HEAD` before deciding what to do next.
12. When the user gives corrective feedback on unpublished work, fix the existing local change set first. Remove out-of-scope files from `origin/main..HEAD` before any push or deploy.
13. For mixed planning + implementation tasks, stay in the controller role and delegate sub-work when helpful:
   - coder agent for repo edits
   - reviewer agent for diff review and completeness checks
   - editorial agent for writing tasks
   - router agent for lightweight classification only
   Use `python3 /workspace/bin/delegate_agent.py --agent <id> --task-file <path>`.
14. Keep responses concise and include what changed or what remains blocked.

## Editorial shortcuts
- `python3 /workspace/bin/editorial_action.py prepare_preview_process --article-id <id>`
- `python3 /workspace/bin/editorial_action.py prepare_preview_manual --article-id <id> --manual-url <url>`
- `python3 /workspace/bin/editorial_action.py request_changes --article-id <id> --instructions "..." --text-only`
- `python3 /workspace/bin/editorial_action.py approve_preview --article-id <id> --user-request "aprova a publicação" --text-only`
- `python3 /workspace/bin/editorial_action.py reject_article --article-id <id> --user-request "rejeita este artigo" --text-only`
- `python3 /workspace/bin/portal_public_link.py --text-only`
- `python3 /workspace/bin/ops_status.py --text-only`
- `python3 /workspace/bin/ops_action.py restart_monitor --reason "..." --text-only`
- `python3 /workspace/bin/repo_status.py --text-only`
- `python3 /workspace/bin/promote_stack.py --message "..." --service portal --health-url http://portal:8000/healthz --text-only`
  - waits for health and auto-rolls back to the previous live ref if final health verification fails
- `python3 /workspace/bin/repo_commit_push.py --message "..." --text-only`
- `python3 /workspace/bin/deploy_stack.py --service portal --text-only`
- `python3 /workspace/bin/rollback_stack.py --ref <git-ref> --service portal --text-only`

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
- Do not deploy workspace changes unless they have been committed and pushed to GitHub first.
- Do not use raw `git push` or raw `docker compose` for production promotion when the helper wrappers exist.
- Use `rollback_stack.py` if a deployment degrades health or leaves the stack in an unhealthy state.
