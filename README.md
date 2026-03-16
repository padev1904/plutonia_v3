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
