"""
Orchestrates the full batch pipeline:
  1. download_crime     — Socrata API → Parquet (BashOperator, host Python in container)
  2. clear_dbt_schemas  — drop staging + mart schemas so Spark can overwrite raw (BashOperator, psql)
  3. spark_crime_batch  — Parquet → clean → Postgres raw.crime_events (BashOperator, docker exec spark-master)
  4. dbt_build          — seed + staging + marts + tests (BashOperator, dbt in container)

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
    "retries": 1,  # deterministic failures don't benefit from many retries
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,  # enable in Phase 3
    "depends_on_past": False,
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

    # 3. Run DBT build: seed + run + test (staging + marts)
    #    Runs in a separate dbt container (dbt-core 1.11's protobuf >=6.0
    #    conflicts with Airflow 3.0's protobuf 4.x, so they can't share
    #    an image). --volumes-from shares the Airflow container's bind mounts
    #    (./dbt, ./dbt_profiles, ./data) without needing host paths.
    #    --network connects to the compose network so dbt can reach Postgres.
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

    # Task dependencies — linear pipeline
    download_crime >> clear_dbt_schemas >> spark_crime_batch >> dbt_build >> record_dbt_results
