# Airflow 3.0 Runtime Breaking Changes + Spark Healthcheck Fix

## Summary
During `docker compose up`, discovered 6 runtime issues with Airflow 3.0 and Spark healthcheck. All fixed. All 6 services now running and healthy.

## Issues Found and Fixed

| # | Issue | Root Cause | Fix |
|---|---|---|---|
| 1 | Spark master unhealthy | Healthcheck checked RPC port 7077 on 127.0.0.1, but Spark binds RPC to container's Docker network IP (172.18.0.x). Web UI (8080) binds to 0.0.0.0. | Changed healthcheck to check port 8080 (Web UI) |
| 2 | Airflow webserver crashes | Airflow 3.0 removed `airflow webserver` command | Changed to `command: api-server` |
| 3 | Airflow scheduler crashes | Airflow 3.0 image has no default CMD | Added `command: scheduler` |
| 4 | passwords.json PermissionError | SimpleAuthManager opens with `a+` mode; file was root-owned | `chmod 666 airflow/passwords.json` |
| 5 | Healthcheck 404 on /health | Airflow 3.0 moved to `/api/v2/monitor/health` | Updated healthcheck URL |
| 6 | WEB_SERVER_PORT deprecated | Moved from [webserver] to [api] section | Changed to `AIRFLOW__API__PORT` |

## Files Modified
- `docker-compose.yml` — spark-master healthcheck (port 8080), airflow-webserver command (api-server) + healthcheck URL, airflow-scheduler command (scheduler), env var AIRFLOW__API__PORT
- `.env.example` — `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` → `AIRFLOW__API__PORT`
- `airflow/passwords.json` — `chmod 666` for airflow user write access
- `changelog.md` — new entry with all 6 issues, breaking changes table, lessons
- `docs/knowledge.md` — new "Airflow 3.0 Runtime Breaking Changes" section
- `docs/operations-performed.md` — updated descriptions

## Verification
All 6 services running and verified:
- postgres: healthy, 3 schemas (raw, staging, mart) confirmed
- spark-master: healthy, UI on port 8180
- spark-worker: running, UI on port 8081
- airflow-init: exited (0) — migrations complete
- airflow-webserver: healthy, UI on port 8080 (admin/admin)
- airflow-scheduler: running, heartbeat active

## Key Context
- Airflow 3.0 is NOT a drop-in upgrade. Beyond auth (SimpleAuthManager), the webserver command, health endpoint, config sections, and default CMD all changed.
- Spark master binds RPC to Docker network IP (not localhost). Web UI binds to 0.0.0.0. Healthchecks should check Web UI port.
- Bind-mounted files need permissions for the container user. `chmod 666` on host = readable/writable by any UID in container.
