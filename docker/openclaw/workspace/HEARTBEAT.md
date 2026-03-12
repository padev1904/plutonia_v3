# OpenClaw Workspace Heartbeat

- Workspace root: `/workspace`
- Repository root: `/workspace/repo`
- Promotion path: `repo_commit_push.py` or `promote_stack.py`, then verify with `repo_status.py`
- Health-first rule: validate touched files before expanding scope
- Recovery rule: if a touched file breaks twice, stop and repair it before editing other files
