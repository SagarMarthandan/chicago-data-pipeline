# Docker Compose

### Project Names
Compose derives the project name from the directory name by default. This affects:
- Network name: `<project>_default`
- Volume names: `<project>_<volume>`
- Container names: `<project>-<service>-1`

Set it explicitly in `.env`:
```bash
COMPOSE_PROJECT_NAME=chicago-data-pipeline
```

### Common Commands
```bash
docker compose up -d              # start all services (detached)
docker compose down               # stop and remove containers
docker compose down -v            # stop and remove containers + volumes (DESTRUCTIVE)
docker compose logs -f <service>  # tail logs for a service
docker compose exec <service> bash  # shell into a running container
docker compose ps                 # list running services
docker compose build              # rebuild images
```

### Networking Between Containers
- Use **service names** as hostnames, never `localhost`
- Spark → Postgres: `jdbc:postgresql://postgres:5432/chicago_analytics`
- Airflow → Postgres: `postgres:5432`
- Kafka producer → Kafka: `kafka:9092`
- From host (DBeaver, psql): `localhost:<published_port>`

### YAML Anchors
Share config across multiple services using `x-` extension fields:
```yaml
x-airflow-common: &airflow-common
  build: ./airflow
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
  volumes:
    - ./dags:/opt/airflow/dags

services:
  airflow-webserver:
    <<: *airflow-common    # merges the anchor
    ports:
      - "8080:8080"
  airflow-scheduler:
    <<: *airflow-common    # same config, no repetition
```
- `&name` creates the anchor, `<<: *name` merges it into a service
- The `x-` prefix tells Compose this is an extension field, not a service

### `depends_on` Conditions
```yaml
depends_on:
  postgres:
    condition: service_healthy          # waits until healthcheck passes
  airflow-init:
    condition: service_completed_successfully  # waits for one-shot init to exit 0
```
- `service_healthy` — for long-running services with healthchecks
- `service_completed_successfully` — for one-shot init/migration containers
- Without a condition, `depends_on` only waits for the container to start (not ready)

### `$$` vs `$` in Compose
- `$VAR` — Compose interpolates from `.env` at compose time
- `$$VAR` — escapes to literal `$VAR`, so the container's shell expands it at runtime
- Use `$$` when you need bash to read an env var that was set via the `environment:` block

### DockerOperator + docker.sock
Airflow's DockerOperator creates containers from inside the Airflow container. To do this, it needs:
1. **Docker CLI** installed in the Airflow image (official image doesn't include it)
2. **docker.sock mounted** — `/var/run/docker.sock:/var/run/docker.sock` bridges the Airflow container to the host's Docker daemon
3. **Network access** — `network_mode: "chicago-data-pipeline_default"` so the spawned container can reach Postgres

---

---

**← Previous:** [uv](uv.md) | **Next:** [postgres](postgres.md) →
