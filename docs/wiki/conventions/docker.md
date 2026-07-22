# Docker Conventions

## docker-compose.yml

### Service Naming
- Use lowercase, hyphenated names: `spark-master`, `airflow-webserver`, `airflow-scheduler`
- Service names are DNS hostnames within the Docker network — other services connect using these names, not `localhost`

### Networking
- All services must be on the same Docker network (compose creates one by default)
- **Set a fixed project name** to keep network/volume names deterministic regardless of folder name. Add to `.env`:
  ```
  COMPOSE_PROJECT_NAME=chicago-data-pipeline
  ```
  This makes the network `chicago-data-pipeline_default` and volumes `chicago-data-pipeline_*` — lowercase, predictable, and independent of the folder name. Airflow's `DockerOperator` references these names.
- **Never use `localhost` or `127.0.0.1` to connect between containers.** Use the service name.
  - Spark → Postgres: `jdbc:postgresql://postgres:5432/...` (not `localhost`)
  - Airflow → Postgres: `postgres:5432` (not `localhost`)
  - Kafka producer → Kafka: `kafka:9092` (not `localhost`)
- To connect *from the host* (e.g., DBeaver, psql), use `localhost:<published_port>`

### Volumes
- Mount init scripts into Postgres: `./init.sql:/docker-entrypoint-initdb.d/init.sql`
- Mount JDBC drivers into Spark: `./jars/postgresql-42.x.jar:/opt/spark/jars/postgresql.jar`
- Use named volumes for persistent data (Postgres data, Kafka data, Airflow logs):
  ```yaml
  volumes:
    postgres_data:
  ```
- Never persist data in the container's writable layer — it's lost on rebuild

### Environment
- All secrets and config go in `.env` (gitignored). Compose reads it automatically.
- `.env.example` is committed and documents every variable:
  ```
  POSTGRES_USER=chicago
  POSTGRES_PASSWORD=changeme
  POSTGRES_DB=chicago_analytics
  COMPOSE_PROJECT_NAME=chicago-data-pipeline
  SOCRATA_APP_TOKEN=...
  ```
- Never hardcode credentials in compose, Dockerfiles, or code

### Healthchecks
- Every service that others depend on should have a healthcheck:
  ```yaml
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U chicago"]
    interval: 10s
    timeout: 5s
    retries: 5
  ```
- Use `depends_on` with `condition: service_healthy` to enforce startup order:
  ```yaml
  depends_on:
    postgres:
      condition: service_healthy
  ```

### Build vs Image
- Use prebuilt images for standard services (Postgres, Kafka, Airflow, Grafana)
- Build custom images only when you need to add jars, packages, or Python deps:
  ```dockerfile
  FROM bitnami/spark:3.5
  COPY jars/postgresql-42.7.3.jar /opt/spark/jars/
  ```

## Dockerfiles

- Always pin versions: `postgres:16-alpine`, not `postgres:latest`
- Use `-alpine` or `-slim` variants to keep image size down
- Multi-stage builds if the image gets large (not needed for this project yet)
- Never run as root — add a non-root user if the base image doesn't have one

## WSL-Specific Notes

- Docker Desktop with WSL2 backend: Docker is available inside WSL, no extra setup
- File mounts from Windows to WSL: use WSL paths (`/mnt/c/...`) or keep the project inside the WSL filesystem for better performance
- **Performance tip:** keep the repo inside the WSL filesystem (`~/chicago-data-pipeline`), not on `/mnt/c/`. Cross-filesystem mounts are slow for file-heavy operations (Spark, Parquet I/O).

## Common Mistakes to Expect

1. **Container can't reach another container** → you used `localhost` instead of the service name
2. **Postgres init script didn't run** → file isn't in `/docker-entrypoint-initdb.d/` or isn't executable
3. **Spark can't find JDBC driver** → JAR isn't in `/opt/spark/jars/` or path is wrong
4. **Data disappears on rebuild** → didn't use a named volume for Postgres data
5. **Port conflict on host** → another service is using the same host port. Change the host-side mapping: `"5433:5432"`
