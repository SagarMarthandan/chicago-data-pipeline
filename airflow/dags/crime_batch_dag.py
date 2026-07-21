"""
Orchestrates the full batch pipeline (Phase 4.3 — cloud migration):
  1. download_crime    — Socrata API → Parquet (BashOperator, host Python in container)
  2. spark_crime_batch — Parquet → clean → GCS Parquet (BashOperator, docker exec spark-master)
  3. bq_load_crime     — GCS Parquet → BigQuery raw.crime_events (BashOperator, bq CLI)
  4. dbt_build         — seed + staging + marts + tests (BashOperator, dbt in container, BigQuery adapter)
  5. record_dbt_results — parse run_results.json → observability.dbt_test_results (Phase 3.2)

Phase 4.3 changes from Phase 1–3:
  - Spark writes to GCS (gs://chicago-divvy-pipeline-data-lake/raw/crime/) instead of Postgres
  - New bq_load_crime task loads GCS Parquet → BigQuery via `bq load --replace`
  - clear_dbt_schemas removed (bq load --replace handles table replacement)
  - wait_for_stream_data sensor removed (dim_date now spans crime dates only —
    station_status stays on local Postgres, not part of BigQuery analytics mart)
  - DBT runs against BigQuery (profiles.yml switched to bigquery adapter)
  - record_dbt_results still writes to local Postgres observability schema
    (observability stays local — it's pipeline metadata, not analytics data)

All tasks run inside the Airflow container (scheduler). GCP credentials
are mounted at /opt/airflow/gcp-credentials.json (GOOGLE_APPLICATION_CREDENTIALS).

Spark runs via `docker exec spark-master` — the docker CLI is installed
in the Airflow image and docker.sock is mounted.

DBT uses a separate profiles.yml (airflow/dbt_profiles/profiles.yml)
configured for BigQuery (service account auth via the mounted key file).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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

# GCP config (Phase 4.3) — env vars set in docker-compose.yml x-airflow-common.
# These are available inside the Airflow container at runtime.
# Using env vars (not Airflow Variables) keeps config in one place (.env).
GCP_PROJECT_ID_ENV = "$GCP_PROJECT_ID"  # shell expansion at task runtime
GCS_BUCKET_ENV = "$GCS_BUCKET"
BIGQUERY_LOCATION_ENV = "$BIGQUERY_LOCATION"

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
    tags=["crime", "batch", "phase-4"],
    max_active_runs=1,  # prevent overlapping runs — one DAG at a time
    doc_md=__doc__,
) as dag:

    # 1. Download crime data from Socrata API → Parquet (unchanged from Phase 1)
    download_crime = BashOperator(
        task_id="download_crime",
        bash_command=f"python {INGESTION_SCRIPT} --year 2023",
    )

    # 2. Spark batch: Parquet → clean → GCS Parquet (Phase 4.3 — was Postgres)
    #    crime_batch.py now writes to gs://chicago-divvy-pipeline-data-lake/raw/crime/
    #    instead of Postgres raw.crime_events. GCS connector JAR + GOOGLE_APPLICATION_CREDENTIALS
    #    handle auth inside the Spark container.
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

    # 3. Load GCS Parquet → BigQuery raw.crime_events (Phase 4.3 — NEW task)
    #    bq load replaces the Postgres write step. --replace makes it idempotent
    #    (drops + recreates the table each run). --source_format=PARQUET reads
    #    the Parquet files Spark wrote to GCS. --location=US matches the dataset.
    #
    #    Auth: the bq CLI (part of gcloud SDK) does NOT read the
    #    GOOGLE_APPLICATION_CREDENTIALS env var like the Python client does.
    #    It uses gcloud's own credential store. So we must explicitly run
    #    `gcloud auth activate-service-account --key-file=...` first.
    #    This is idempotent — re-activating the same SA is a no-op.
    #
    #    Note: $GCP_PROJECT_ID and $GCS_BUCKET are env vars from docker-compose.yml.
    #    Shell expansion happens at task runtime inside the Airflow container.
    bq_load_crime = BashOperator(
        task_id="bq_load_crime",
        bash_command=(
            'gcloud auth activate-service-account '
            '--key-file=$GOOGLE_APPLICATION_CREDENTIALS '
            '--project=$GCP_PROJECT_ID && '
            'bq load --replace --location=$BIGQUERY_LOCATION '
            '--source_format=PARQUET '
            '--project_id=$GCP_PROJECT_ID '
            'raw.crime_events '
            'gs://$GCS_BUCKET/raw/crime/*.parquet'
        ),
    )

    # 4. Run DBT build: seed + run + test (staging + marts) against BigQuery
    #    Runs in a separate dbt container (dbt-bigquery adapter). --volumes-from
    #    shares the Airflow container's bind mounts (./dbt, ./dbt_profiles, ./data)
    #    + the GCP credentials mount. --network connects to the compose network
    #    (needed for record_dbt_results to reach Postgres observability schema).
    #
    #    GCP env vars are passed through so DBT's BigQuery adapter can auth.
    #    The adapter reads GOOGLE_APPLICATION_CREDENTIALS (mounted key file).
    DBT_IMAGE = "chicago-data-pipeline-dbt:latest"
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            'docker run --rm '
            '--network chicago-data-pipeline_default '
            f'--volumes-from $HOSTNAME '
            '-e GOOGLE_APPLICATION_CREDENTIALS '
            '-e GCP_PROJECT_ID '
            '-e GCS_BUCKET '
            '-e BIGQUERY_LOCATION '
            f'{DBT_IMAGE} '
            # Exclude station_status models — the stream stays on local Postgres,
            # not BigQuery. These models (stg_station_status, fact_station_reads)
            # reference raw.station_status which doesn't exist in BigQuery.
            # They'll be revisited in Phase 4.6 when Divvy trip history is ingested.
            f'dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR} '
            '--exclude stg_station_status fact_station_reads'
        ),
        # Phase 3.3 — execution_timeout (not sla=, which is removed in Airflow 3.0).
        # 30min is generous — dbt build typically completes in <10min.
        execution_timeout=timedelta(minutes=30),
    )

    # 4b. Record dbt test results into Postgres for Grafana observability
    #     (Phase 3.2). `dbt build` writes target/run_results.json; this task
    #     parses it and upserts one row per test into observability.dbt_test_results,
    #     which the Grafana "DBT tests" panel queries. Runs in the Airflow
    #     container (psycopg2 available via the postgres provider).
    #     NOTE: observability stays on local Postgres — it's pipeline metadata,
    #     not analytics data. Only the analytics marts moved to BigQuery.
    record_dbt_results = BashOperator(
        task_id="record_dbt_results",
        bash_command=f"python /opt/airflow/scripts/record_dbt_results.py",
    )

    # Task dependencies — linear pipeline (Phase 4.3)
    # Removed from Phase 3: clear_dbt_schemas (bq load --replace handles it),
    # wait_for_stream_data sensor (dim_date no longer spans station_status).
    download_crime >> spark_crime_batch >> bq_load_crime >> dbt_build >> record_dbt_results
