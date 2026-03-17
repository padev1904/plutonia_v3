# Plutonia AI News

Native Linux deployment for the AI news ingestion, editorial review, Telegram approval flow, and Django portal.

Project root:

```text
/home/python/demos/plutonia/ainews
```

Runtime layout:

```text
/home/python/demos/plutonia/ainews/.venv
/home/python/demos/plutonia/ainews/.env
/home/python/demos/plutonia/ainews/data
/home/python/demos/plutonia/ainews/logs
/home/python/demos/plutonia/ainews/run
/home/python/demos/plutonia/ainews/backups
/home/python/demos/plutonia/ainews/staticfiles
```

## Bootstrap

```bash
cd /home/python/demos/plutonia/ainews
cp .env.example .env
./scripts/bootstrap_native.sh
```

## Services

Systemd unit files are stored in:

```text
deploy/systemd/plutonia-portal.service
deploy/systemd/plutonia-monitor.service
```

Install them with:

```bash
sudo ./scripts/install_systemd_units.sh
sudo systemctl restart plutonia-portal
sudo systemctl restart plutonia-monitor
```

## Nginx

Native nginx config is stored in:

```text
deploy/nginx/plutonia.conf
```

Install it into your nginx sites configuration and point traffic to `127.0.0.1:8000`.

## Operations

Useful scripts:

```text
scripts/bootstrap_native.sh
scripts/run_portal.sh
scripts/run_monitor.sh
scripts/restart_clean.sh
scripts/health_check.sh
scripts/backup_db.sh
```

## Parser Replay

Cache the useful `text/html` or `text/plain` body part of the first 1000 eligible mailbox emails locally:

```bash
python scripts/parser_replay.py cache-imap --start-index 0 --limit 1000
```

Then run the parser analysis offline against the cache:

```bash
python scripts/parser_replay.py analyze-cache
```

By default the cache is stored under `data/parser_replay/cache` and the analysis report under `data/parser_replay/reports/parser_replay_report.json`.
