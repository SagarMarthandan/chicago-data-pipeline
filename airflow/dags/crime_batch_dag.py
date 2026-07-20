"""
Orchestrates the full batch pipeline:
  1. download_crime        — Socrata API → Parquet (BashOperator, host Python in container)
  2. clear_dbt_schemas     — drop staging + mart schemas so Spark can overwrite raw (BashOperator, psql)
  3. spark_crime_batch     — Parquet → clean → Postgres raw.crime_events (BashOperator, docker exec spark-master)
  4. wait_for_stream_data  — SqlSensor: wait for raw.station_status to exist (Phase 3.3 race condition fix)
  5. dbt_build             — seed + staging + marts + tests (BashOperator, dbt in container)
  6. record_dbt_results    — parse run_results.json → observability.dbt_test_results (Phase 3.2)

Phase 3.3 additions:
  - SqlSensor (wait_for_stream_data) gates dbt_build on raw.station_status existing.
    dim_date spans both crime + station sources, so dbt build needs BOTH raw tables.
    Previously this was an IMPLICIT dependency — dbt build would fail on first try
    if divvy_stream hadn't run yet. The sensor makes it EXPLICIT.
  - on_failure_callback logs structured failure context to Airflow logs.
  - retries=3, retry_delay=5min on tasks with transient failure potential.
  - sla=30min on dbt_build — alerts if dbt takes too long.

All tasks run inside the Airflow container (scheduler). Dependencies
(pandas, pyarrow, requests, dbt-core, dbt-postgres) are installed in
the Airflow image via airflow/requirements.txt.

Spark runs via `docker exec spark-master` — the docker CLI is installed
in the Airflow image and docker.sock is mounted.

DBT uses a separate profiles.yml (airflow/dbt_profiles/profiles.yml)
with host: postgres (Docker service name) instead of localhost.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from callbacks import on_failure_callback

# ============================================================
# Configuration
# ============================================================

# Spark container — resolved at runtime by name pattern.
# Avoids hardcoding the project prefix (chicago-data-pipeline-spark-master-1)
# which breaks if COMPOSE_PROJECT_NAME changes.
SPARK_CONTAINER_CMD = "docker ps -qf name=spark-master --filter status=running | head -1"

# Paths inside the Airflow container
INGESTION_SCRIPT = "/opt/airflow/ingestion/download_crime.py"
DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

# ============================================================
# Default args
# ============================================================

default_args = {
    "owner": "chicago-pipeline",
    "retries": 3,  # Phase 3.3 — transient failures (network blips, DB locks) benefit from retries
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # no email server; on_failure_callback logs instead
    "depends_on_past": False,
    "on_failure_callback": on_failure_callback,  # Phase 3.3 — structured failure logging
}

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="crime_batch",
    default_args=default_args,
    schedule=None,  # manual trigger only — switch to "@daily" after verification
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crime", "batch", "phase-1"],
    max_active_runs=1,  # prevent overlapping runs — one DAG at a time
    doc_md=__doc__,
) as dag:

    # 1. Download crime data from Socrata API → Parquet
    download_crime = BashOperator(
        task_id="download_crime",
        bash_command=f"python {INGESTION_SCRIPT} --year 2023",
    )

    # 1b. Drop DBT schemas (staging, mart) so Spark can overwrite raw.crime_events.
    #     DBT views from a prior run depend on raw.crime_events and block
    #     Spark's DROP TABLE (mode="overwrite"). DBT rebuilds all views/tables
    #     in the next task, so dropping them here is safe and idempotent.
    clear_dbt_schemas = BashOperator(
        task_id="clear_dbt_schemas",
        bash_command=(
            'docker exec chicago-data-pipeline-postgres-1 '
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" '
            '-c "DROP SCHEMA IF EXISTS staging CASCADE;" '
            '-c "DROP SCHEMA IF EXISTS mart CASCADE;"'
        ),
    )

    # 2. Spark batch: Parquet → clean → Postgres raw.crime_events
    spark_crime_batch = BashOperator(
        task_id="spark_crime_batch",
        bash_command=(
            'SPARK_CID=$(' + SPARK_CONTAINER_CMD + ') && '
            'if [ -z "$SPARK_CID" ]; then echo "ERROR: spark-master container not running"; exit 1; fi && '
            'docker exec "$SPARK_CID" '
            '/opt/spark/bin/spark-submit --master local[*] '
            '/opt/spark/jobs/crime_batch.py'
        ),
    )

    # 3. Wait for stream data — SqlSensor gates dbt_build on raw.station_status
    #    existing (Phase 3.3 race condition fix).
    #
    #    Why: dim_date spans BOTH crime + station sources (UNION ALL of
    #    stg_crime_events + stg_station_status date ranges). dbt build with
    #    no selector builds ALL models, including stg_station_status which
    #    reads raw.station_status. If divvy_stream hasn't run yet, that table
    #    doesn't exist and dbt build fails on try 1 (succeeds on retry — a
    #    race condition). This sensor makes the dependency EXPLICIT: crime_batch
    #    waits for the stream table to exist before building marts.
    #
    #    The sensor queries to_regclass('raw.station_status') — returns the
    #    table's OID if it exists, NULL if not. SqlSensor treats any non-NULL
    #    return as success. mode="reschedule" releases the worker slot between
    #    pokes (don't hold a slot while waiting). Timeout: 1 hour — if the
    #    stream hasn't produced data in an hour, something is wrong.
    wait_for_stream_data = SqlSensor(
        task_id="wait_for_stream_data",
        conn_id="postgres_default",
        sql="SELECT to_regclass('raw.station_status')",
        # Airflow 3.0 SqlSensor.poke calls hook.get_records(sql) → list of rows,
        # then passes records[0] (the first row) to the success callable.
        # to_regclass returns a single column, so records[0] is a 1-tuple like
        # ('raw.station_status',) if the table exists, or (None,) if not.
        # We check the first element of the row.
        success=lambda row: row[0] is not None,
        poke_interval=60,  # check every 60s
        timeout=60 * 60,  # give up after 1 hour
        mode="reschedule",  # release worker slot between pokes
    )

    # 4. Run DBT build: seed + run + test (staging + marts)
    #    Runs in a separate dbt container (dbt-core 1.11's protobuf >=6.0
    #    conflicts with Airflow 3.0's protobuf 4.x, so they can't share
    #    an image). --volumes-from shares the Airflow container's bind mounts
    #    (./dbt, ./dbt_profiles, ./data) without needing host paths.
    #    --network connects to the compose network so dbt can reach Postgres.
    #    SLA: 30 minutes — dbt build should complete in <10min; 30min leaves
    #    room for slow runs. SLA miss is logged by Airflow and visible in the
    #    Grafana SLA panel (Phase 3.3).
    DBT_IMAGE = "chicago-data-pipeline-dbt:latest"
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            'docker run --rm '
            '--network chicago-data-pipeline_default '
            f'--volumes-from $HOSTNAME '
            '-e POSTGRES_USER '
            '-e POSTGRES_PASSWORD '
            '-e POSTGRES_DB '
            f'{DBT_IMAGE} '
            f'dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
        ),
        # Phase 3.3 — execution_timeout (not sla=, which is removed in Airflow 3.0).
        # sla= triggers a deprecation warning and is a no-op; Airflow 3.0 removed
        # the SLA feature (to be replaced in >=3.1). execution_timeout actually
        # fails the task if it exceeds the limit. 30min is generous — dbt build
        # typically completes in <10min.
        execution_timeout=timedelta(minutes=30),
    )
    # 4b. Record dbt test results into Postgres for Grafana observability
    #     (Phase 3.2). `dbt build` writes target/run_results.json; this task
    #     parses it and upserts one row per test into observability.dbt_test_results,
    #     which the Grafana "DBT tests" panel queries. Runs in the Airflow
    #     container (psycopg2 available via the postgres provider).
    record_dbt_results = BashOperator(
        task_id="record_dbt_results",
        bash_command=f"python /opt/airflow/scripts/record_dbt_results.py",
    )

    # Task dependencies — linear pipeline with sensor gate
    # The sensor runs AFTER spark_crime_batch (raw.crime_events loaded) and
    # BEFORE dbt_build (which needs both raw tables). It waits for
    # raw.station_status to exist — the stream table from divvy_stream DAG.
    download_crime >> clear_dbt_schemas >> spark_crime_batch >> wait_for_stream_data >> dbt_build >> record_dbt_results
