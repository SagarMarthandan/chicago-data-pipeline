# Bitnami Spark → apache/spark Migration

## Summary
`docker compose build` failed with `bitnami/spark:3.5: not found`. Bitnami moved their Docker images behind a commercial subscription in 2026. Switched to the official `apache/spark:3.5.1` image. This required rewriting the Spark Dockerfile and both Spark services in docker-compose.yml because the official image uses `spark-class` commands instead of Bitnami's `SPARK_MODE` env var interface.

## Decisions Made
- **apache/spark:3.5.1** (official Apache Spark image) — free, actively maintained, upstream source
- **spark-class commands instead of SPARK_MODE env vars** — the official image doesn't have Bitnami's wrapper interface
- **SPARK_MASTER_HOST=spark-master** — tells master to advertise Docker service name so workers can resolve it
- **python3 socket healthcheck** — more portable than bash `/dev/tcp` across base images

## Files Created/Modified
- `spark/Dockerfile` — base image `bitnami/spark:3.5` → `apache/spark:3.5.1`, JDBC path `/opt/bitnami/spark/jars/` → `/opt/spark/jars/`, user UID 1001 → `spark` (UID 185)
- `docker-compose.yml` — spark-master and spark-worker services rewritten with `spark-class` commands, volume paths updated to `/opt/spark/jobs/`, added `SPARK_MASTER_HOST`, removed Bitnami-specific RPC/SSL env vars
- `changelog.md` — new entry with error, root cause, breaking changes table, lesson
- `docs/operations-performed.md` — updated Spark Dockerfile description + new dated section
- `docs/knowledge.md` — new "Docker Image: apache/spark (not bitnami)" reference section with comparison table

## Key Context
- Bitnami moved ALL their Docker images behind a commercial subscription ("Bitnami Secure Images") in 2026. Free `docker.io/bitnami/*` images are gone.
- The official `apache/spark` image is designed for Kubernetes but works fine in docker-compose standalone mode with explicit `spark-class` commands.
- `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` env vars still work in the official image — `spark-class` startup scripts read them.
- Without `SPARK_MASTER_HOST`, the master advertises a random container hostname that workers can't resolve via Docker DNS.

## Errors Encountered
- **`bitnami/spark:3.5: not found`** — Bitnami images no longer free on Docker Hub. Fixed by switching to `apache/spark:3.5.1`.

## Verification
- YAML validated: `docker-compose.yml` parses cleanly
- Programmatic checks: spark-class commands present, SPARK_MASTER_HOST set, worker resources configured, no bitnami references in volumes, no SPARK_MODE env vars
