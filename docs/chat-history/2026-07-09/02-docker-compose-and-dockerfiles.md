# Docker Compose + Dockerfiles Created

## Summary
Created `docker-compose.yml` (6 services), custom Airflow Dockerfile, custom Spark Dockerfile, and directory placeholders. Used YAML anchors for shared Airflow config. Baked JDBC driver into Spark image.

## Decisions Made
- **Spark UI remapped to 8180** — port 8080 conflicts with Airflow webserver
- **JDBC driver baked into Spark image** — more reliable than `--packages` at runtime (works offline, faster startup)
- **DockerOperator via docker CLI + docker.sock mount** — Airflow container needs to spawn other containers
- **YAML anchors (`x-airflow-common`)** — share env vars + volumes across 3 Airflow services, avoids repeating 10+ lines 3 times
- **`airflow-init` as one-shot service** — runs migrations, then exits. webserver + scheduler use `service_completed_successfully` condition.
- **`$$` in compose command** — escapes `$` so Compose doesn't interpolate; bash reads env vars instead
- **Spark worker: 2G memory, 2 cores** — enough for ~8M crime rows, leaves resources for Postgres + Airflow on i7-7700HQ

## Files Created/Modified
- `docker-compose.yml` — 6 services (postgres, spark-master, spark-worker, airflow-init, airflow-webserver, airflow-scheduler)
- `airflow/Dockerfile` — apache/airflow + docker CLI + uv pip install
- `airflow/requirements.txt` — postgres + docker providers
- `spark/Dockerfile` — bitnami/spark:3.5 + PostgreSQL JDBC driver
- `airflow/dags/.gitkeep`, `spark/jobs/.gitkeep` — directory placeholders

## Key Context
- `service_completed_successfully` is for one-shot init services (waits for exit code 0). `service_healthy` is for long-running services.
- DockerOperator needs docker.sock because Airflow runs inside a container but creates OTHER containers.
- Healthchecks: Postgres uses `pg_isready`, Spark master checks RPC port, Airflow webserver checks `/health` endpoint.

## Errors Encountered
None in this chunk.
