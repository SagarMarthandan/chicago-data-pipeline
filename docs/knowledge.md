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
- `raw` — landing zone, untransformed ingested data
- `mart` — DBT output, analytics-ready tables

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

### Model Layers
- **staging** — rename, type-cast, light cleaning (1:1 with source tables)
- **intermediate** — joins, aggregations, business logic
- **marts** — final analytics tables (facts + dims)

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
