# Knowledge Base

Reference material, useful commands, and explanations accumulated throughout the project. Not a tutorial — a quick lookup for things you've already learned but might forget.

---

## WSL

### Useful Commands
```bash
# Check WSL version
wsl -l -v

# Access Windows files from WSL
ls /mnt/c/Users/sagar/

# Keep projects on WSL filesystem for performance (not /mnt/c/)
# Good:  ~/chicago-data-pipeline/
# Bad:   /mnt/c/Users/sagar/chicago-data-pipeline/
```

### Why WSL filesystem is faster
Cross-filesystem mounts (`/mnt/c/...`) go through the 9P protocol between WSL and Windows. File-heavy operations (Spark, Parquet I/O, git) are significantly slower. Keep the repo inside `~/` (WSL ext4 filesystem).

### Devin IDE + OMP sync
Devin IDE caches the file tree on open and doesn't watch for external changes. If you edit files via OMP, close and reopen Devin (or the affected file tabs) to see updates.

---

## Docker Compose

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

## Postgres

### Useful Commands
```bash
# Connect via psql
psql -h localhost -p 5432 -U chicago -d chicago_analytics

# Inside psql
\dt raw.*          # list tables in raw schema
\dt mart.*         # list tables in mart schema
\d raw.crime_events  # describe table structure
\dn                # list schemas
\q                 # quit
```

### Schemas
- `raw` — landing zone, untransformed data from Spark/Kafka
- `staging` — DBT staging layer: light cleaning, renaming, type casting (1:1 with source tables)
- `mart` — DBT final output: facts + dimensions, analytics-ready

### Schema vs DBT Layer
Postgres schemas are **physical namespaces** in the database. DBT layers are **logical transformation stages** (folders in your dbt project). They're different concepts:
- You can have 3 DBT model layers mapped to 3 Postgres schemas (this project's approach)
- Or all DBT output in one schema (simpler, less separation)
- Schema-per-layer gives clearer separation and finer-grained access control (e.g., grant analysts access to `mart` only)

### Init Scripts (`/docker-entrypoint-initdb.d/`)
- Scripts in this directory run **once** on first container startup (when the data volume is empty)
- Supported formats: `.sql` (run as SQL), `.sh` (run as shell script), `.sql.gz` (decompressed then run)
- Run in alphabetical order as the `POSTGRES_USER` connected to `POSTGRES_DB`
- **Changing init.sql after first run does nothing** — must destroy the volume: `docker compose down -v`
- SQL files **cannot read `.env` variables** — only `docker-compose.yml` can interpolate `${VAR}`. Use a `.sh` script if you need env vars in init logic.

### Postgres "If Not Exists" Workarounds
Postgres lacks `CREATE USER IF NOT EXISTS` and `CREATE DATABASE IF NOT EXISTS`. Workarounds:

```sql
-- User: use DO block with pg_roles check
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'myuser') THEN
        CREATE ROLE myuser WITH LOGIN PASSWORD 'mypass';
    END IF;
END
$$;

-- Database: use \gexec (psql meta-command)
-- CREATE DATABASE can't run inside a transaction, so IF NOT EXISTS patterns don't work
SELECT 'CREATE DATABASE mydb OWNER myuser'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mydb')\gexec
```

- `$$` are dollar quotes — tell Postgres "treat everything between as a string body"
- `\gexec` takes the query result (a SQL string) and executes it as a new command

### Cast Syntax
```sql
-- Postgres: use :: for casting
SELECT '2024-01-15'::date;
SELECT '123'::integer;

-- Postgres does NOT have TRY_CAST (that's Snowflake/DuckDB)
-- Use CASE/REGEXP guards or clean data upstream in Spark

-- EXTRACT fields: year, month, day, hour, minute, second, dow, epoch
-- 'date' is NOT a valid EXTRACT field
SELECT EXTRACT(year FROM occurred_at);  -- valid
SELECT occurred_at::date;               -- use this for date casting
```

---

## DBT

### Key Jinja Variables
| Variable | Purpose |
|---|---|
| `adapter.type()` | Check warehouse type for macro dispatch (`'postgres'`, `'bigquery'`) |
| `this` | Reference to the current model |
| `ref('model_name')` | Reference another model (builds dependency graph) |
| `source('schema', 'table')` | Reference a source table |

### Common Commands
```bash
dbt run                          # run all models
dbt run --select staging         # run only staging models
dbt run --select marts           # run only mart models
dbt test                         # run all tests
dbt test --select stg_crime_events  # test one model
dbt compile                      # compile SQL without running
dbt debug                        # check connection/config
dbt docs generate && dbt docs serve  # generate and view docs
```

### Model Layers
- **staging** — rename, type-cast, light cleaning (1:1 with source tables) → written to `staging` schema
- **marts** — final analytics tables (facts + dims) → written to `mart` schema
- **intermediate** (skipped for now) — joins, aggregations; can add `intermediate` schema later if needed

### Macro Dispatch Pattern
```sql
{% macro try_cast(column, target_type) %}
  {% if adapter.type() == 'postgres' %}
    {{ column }}::{{ target_type }}
  {% elif adapter.type() == 'bigquery' %}
    SAFE_CAST({{ column }} AS {{ target_type }})
  {% else %}
    {{ column }}::{{ target_type }}
  {% endif %}
{% endmacro %}
```

---

## Spark

### Useful Commands
```bash
# Submit a batch job
spark-submit --master local[*] jobs/crime_batch.py

# Submit with JDBC dependency
spark-submit --packages org.postgresql:postgresql:42.7.3 jobs/crime_batch.py

# Spark shell (PySpark)
pyspark --master local[*]
```

### JDBC Connection to Postgres
```python
(df.write
  .format("jdbc")
  .option("url", "jdbc:postgresql://postgres:5432/chicago_analytics")
  .option("dbtable", "raw.crime_events")
  .option("user", "chicago")
  .option("password", "changeme")
  .mode("overwrite")
  .save())
```

### Structured Streaming + Kafka
```python
stream = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka:9092")
  .option("subscribe", "divvy_station_status")
  .load())
```

### foreachBatch (streaming → JDBC bridge)
JDBC doesn't have a native streaming sink. Use `foreachBatch` to write each micro-batch:
```python
(df.writeStream
  .foreachBatch(lambda df, epoch: df.write.format("jdbc").option(...).save())
  .start())
```

---

## Kafka

### Useful Commands
```bash
# List topics
kafka-topics --list --bootstrap-server kafka:9092

# Consume from a topic (terminal)
kafka-console-consumer --bootstrap-server kafka:9092 --topic divvy_station_status --from-beginning

# Produce to a topic (terminal)
kafka-console-producer --bootstrap-server kafka:9092 --topic test

# Describe a topic
kafka-topics --describe --bootstrap-server kafka:9092 --topic divvy_station_status
```

### Key Concepts
- **Topic** — named stream/category (e.g., `divvy_station_status`)
- **Partition** — parallelism unit within a topic
- **Consumer Group** — group of consumers sharing partitions
- **Offset** — position within a partition; committed by consumer
- **Zookeeper** — coordination service (Kafka 3.x+ can run without it via KRaft, but learning ZK first is more educational)

---

## Airflow

### Useful Commands
```bash
# Start Airflow (via docker compose)
docker compose up airflow-webserver airflow-scheduler

# Run a DAG manually
airflow dags trigger crime_batch

# Check DAG state
airflow dags list
airflow dags state crime_batch <run_id>

# Test a single task
airflow tasks test crime_batch download_crime 2024-01-15
```

### Key Concepts
- **DAG** — Directed Acyclic Graph; defines task dependencies
- **Task** — a unit of work (operator instance)
- **Operator** — template for a task (BashOperator, DockerOperator, etc.)
- **XCom** — cross-task communication (small data only)
- **Sensor** — a special operator that waits for a condition
- **Idempotency** — re-running produces the same result; always design for this

### Executors
| Executor | What it is | When to use | Extra services |
|---|---|---|---|
| `SequentialExecutor` | One task at a time, single thread | Dev/testing only | None |
| `LocalExecutor` | Parallel tasks on one machine | Phase 1 — good fit | None (uses metadata DB) |
| `CeleryExecutor` | Distributes tasks across worker machines | Production / heavy workloads | Redis or RabbitMQ + Celery workers |

### Metadata Database
Airflow needs its OWN database to track DAG runs, task states, scheduling info, and logs. This is NOT your analytics data. If you point Airflow at your warehouse DB, it creates tables like `task_instance`, `dag_run`, `xcom` and pollutes your analytics schema. Always use a separate database (can be in the same Postgres instance, just a different DB + user).

### `.env` vs `.env.example`
- `.env.example` — committed to git, documents required variables with placeholder values
- `.env` — gitignored, contains real secrets. Compose reads it automatically at `docker compose up`
- Image names (e.g., `postgres:16-alpine`) go in `docker-compose.yml`, NOT `.env`. `.env` is for secrets and environment-specific config only.

### Scheduling
- `schedule="@daily"` — runs daily
- `schedule="@manual"` or `schedule=None` — trigger by hand (use while debugging)
- `catchup=False` — don't backfill historical runs on first deploy
- `start_date` — fixed past date, NEVER `datetime.now()`

---

## Git

### Useful Commands
```bash
git init                          # initialize repo
git branch -m main                # rename default branch to main
git add .                         # stage all changes
git commit -m "message"           # commit
git log --oneline                 # compact history
git status                        # see what changed
git remote add origin <url>       # add GitHub remote
git push -u origin main           # push to GitHub
```

---

## Data Sources Reference

### Chicago Crime (Socrata API)
- **Portal:** https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q4t2
- **API endpoint:** `https://data.cityofchicago.org/resource/ijzp-q4t2.json`
- **Auth:** Socrata App Token (free, register at https://data.cityofchicago.org/profile/edit/developer_settings)
- **Format:** JSON (convertible to CSV/Parquet)
- **Volume:** ~8M rows, decades of history
- **Update cadence:** daily/weekly drops (batch, not streaming)

### Divvy Bike Share (GBFS API)
- **Feed:** https://gbfs.divvybikes.com/gbfs/gbfs.json
- **Station status:** https://gbfs.divvybikes.com/gbfs/en/station_status.json
- **Station info:** https://gbfs.divvybikes.com/gbfs/en/station_information.json
- **Format:** JSON (GBFS — General Bikeshare Feed Specification)
- **Refresh:** ~60 seconds (genuine live stream)
- **No auth required**

---

<!-- Append new sections below as you learn new things. -->
