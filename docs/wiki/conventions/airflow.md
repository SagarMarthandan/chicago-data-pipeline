# Airflow Conventions

## DAG Structure

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker import DockerOperator

default_args = {
    "owner": "chicago-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # enable in Phase 3
    "depends_on_past": False,
}

with DAG(
    dag_id="crime_batch",
    default_args=default_args,
    schedule="@daily",          # start with @manual while debugging
    start_date=datetime(2024, 1, 1),
    catchup=False,              # don't backfill — trigger manually
    tags=["crime", "batch"],
) as dag:

    download = BashOperator(
        task_id="download_crime",
        bash_command="python /opt/ingestion/download_crime.py --year 2023",
    )

    spark_batch = DockerOperator(
        task_id="spark_crime_batch",
        image="chicago-spark:latest",
        command="/opt/spark/bin/spark-submit /jobs/crime_batch.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="chicago-data-pipeline_default",  # matches COMPOSE_PROJECT_NAME in .env
        mounts=["..."],
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --select staging marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test",
    )

    download >> spark_batch >> dbt_run >> dbt_test
```

## Naming Conventions

| Element | Convention | Example |
|---|---|---|
| DAG ID | `snake_case`, descriptive | `crime_batch`, `divvy_stream_monitor` |
| Task ID | `verb_noun`, snake_case | `download_crime`, `run_dbt_models` |
| Tags | lowercase, short | `["crime", "batch"]` |

## Idempotency

**Every DAG run must produce the same result whether it runs once or ten times.**

- **Downloads:** overwrite the local file, don't append
- **Spark writes:** use `overwrite` mode (Phase 1) or upsert (Phase 2+)
- **DBT runs:** `dbt run` is idempotent by default (views recreate, tables replace)
- **Never use `append` without deduplication** somewhere in the pipeline

If a task can't be made idempotent, it doesn't belong in Airflow.

## Retries & SLAs

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
```

- `retries=3` with `retry_delay=5min` — handles transient failures (network blips, DB locks)
- Add `sla=timedelta(hours=2)` to critical tasks (Phase 3) — alerts if the task is late
- `depends_on_past=False` — each run is independent (unless your logic truly requires ordering)

## Task Dependencies

```python
# Linear (most common for this project)
download >> spark >> dbt_run >> dbt_test

# Branching (if you add data quality gates)
download >> spark >> dbt_run >> check_quality >> [dbt_test, alert_on_bad_data]
```

- Use `>>` operator, never `set_downstream()` (deprecated style)
- Keep DAGs linear until you have a real reason to branch

## Operators — When to Use What

| Operator | Use for | Notes |
|---|---|---|
| `BashOperator` | Running scripts, dbt commands | Simple, runs in the Airflow worker |
| `DockerOperator` | Running Spark jobs in isolated containers | Needs `docker.sock` mounted. Task runs in its own container. |
| `SparkSubmitOperator` | Submitting to a running Spark cluster | Use if you have spark-master running. Lighter than DockerOperator. |
| `PythonOperator` | Python functions that don't need Spark | For simple logic, sensors |
| `HttpSensor` | Waiting for an external API | Phase 3: check if Divvy API is up before streaming |
| `SqlSensor` | Waiting for data in Postgres | Phase 3: check for new rows before running DBT |

## DockerOperator Specifics

```python
DockerOperator(
    task_id="spark_crime_batch",
    image="chicago-spark:latest",
    command="spark-submit /jobs/crime_batch.py",
    docker_url="unix://var/run/docker.sock",
    network_mode="chicago-data-pipeline_default",
    mounts=[
        "chicago-data-pipeline_data:/data",
        "chicago-data-pipeline_jars:/opt/spark/jars",
    ],
    auto_remove=True,  # clean up container after task
)
```

- **`docker.sock` must be mounted** into the Airflow container for DockerOperator to work
- **`network_mode`** must match the compose network name, set via `COMPOSE_PROJECT_NAME=chicago-data-pipeline` in `.env` (see docker conventions)
- **`auto_remove=True`** prevents dead containers from piling up

## Sensors (Phase 3)

Sensors wait for a condition before proceeding. Don't use them in Phase 1.

```python
from airflow.sensors.sql import SqlSensor

wait_for_data = SqlSensor(
    task_id="wait_for_new_crime_data",
    conn_id="postgres_default",
    sql="SELECT COUNT(*) FROM raw.crime_events WHERE date >= CURRENT_DATE - 1",
    success=lambda x: x > 0,
    poke_interval=300,  # check every 5 minutes
    timeout=3600,       # give up after 1 hour
    mode="reschedule",  # release worker slot between pokes
)
```

- Use `mode="reschedule"` for long waits — don't hold a worker slot
- Use `mode="poke"` for short waits (<1 min)

## Scheduling

- **Phase 1:** `schedule="@manual"` or `schedule=None` — trigger by hand while debugging
- **Phase 2+:** `schedule="@daily"` for batch, `schedule="@hourly"` for monitoring DAGs
- **`catchup=False`** — don't backfill historical runs on first deploy
- **`start_date`** — set to a fixed past date, never `datetime.now()` (it breaks scheduling)

## Connections

Define in Airflow UI (Admin → Connections) or via environment variables:

```bash
# Postgres connection
AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://chicago:changeme@postgres:5432/chicago_analytics
```

- Connection IDs: `postgres_default`, `spark_default`, etc.
- Reference in operators: `conn_id="postgres_default"`

## Common Mistakes to Expect

1. **DockerOperator can't find docker.sock** → mount it in the Airflow container: `/var/run/docker.sock:/var/run/docker.sock`
2. **Task can't reach Postgres** → using `localhost` instead of `postgres` (Docker service name)
3. **Re-running DAG duplicates data** → using `append` mode without deduplication
4. **DAG doesn't appear in UI** → syntax error in the DAG file. Check Airflow scheduler logs.
5. **`catchup=True` floods the scheduler** → always set `catchup=False` until you intentionally want backfill
6. **`start_date=datetime.now()`** → breaks scheduling because the start date keeps moving. Use a fixed date.
7. **Task succeeds but data is wrong** → no tests after the pipeline. Always end with `dbt test`.
