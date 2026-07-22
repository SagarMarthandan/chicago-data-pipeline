"""
Divvy Trip History Ingestion DAG (Phase 4.4)
=============================================

Orchestrates Divvy trip history ingestion from S3 → BigQuery via dlt.

Tasks:
  1. load_divvy_trips — dlt loads monthly S3 CSVs into BigQuery raw.divvy_trips
  2. dbt_build_divvy   — build Divvy-related DBT models (stg_divvy_trips,
                         dim_stations, fact_divvy_trips, fact_station_day,
                         crime_ridership_correlation) + updated dim_date
  3. record_dbt_results — parse run_results.json → observability.dbt_test_results

The dlt script (ingestion/load_divvy_trips.py) downloads monthly ZIPs from
divvy-tripdata.s3.amazonaws.com, extracts CSVs, and loads them into BigQuery
via dlt's BigQuery destination adapter. Append mode — re-running adds
duplicate rows, so this DAG should only be triggered once for full history
or with specific --month/--from/--to for incremental loads.

GCP credentials are mounted at /opt/airflow/gcp-credentials.json
(GOOGLE_APPLICATION_CREDENTIALS env var). dlt reads this for BigQuery auth.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from callbacks import on_failure_callback

# ============================================================
# Configuration
# ============================================================

INGESTION_SCRIPT = "/opt/airflow/ingestion/load_divvy_trips.py"
DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
DBT_IMAGE = "chicago-data-pipeline-dbt:latest"

# ============================================================
# Default args
# ============================================================

default_args = {
    "owner": "chicago-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
    "on_failure_callback": on_failure_callback,
}

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="divvy_trip_history",
    default_args=default_args,
    schedule=None,  # manual trigger — historical load, not recurring
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["divvy", "batch", "phase-4"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # 1. Load Divvy trip history from S3 → BigQuery via dlt
    #    --all loads all available monthly files (2020-04 to latest).
    #    For incremental loads, use --from YYYYMM --to YYYYMM instead.
    load_divvy_trips = BashOperator(
        task_id="load_divvy_trips",
        bash_command=f"python {INGESTION_SCRIPT} --all",
        # Full history (~50M rows, 81 monthly files) takes 30-60 min.
        # 2hr timeout is generous.
        execution_timeout=timedelta(hours=2),
    )

    # 2. Run DBT build for Divvy models + updated dim_date + analytics marts.
    #    Uses --select to build only Divvy-related models + their dependencies.
    #    This avoids rebuilding crime models (which now read from the public
    #    dataset and are built by the crime_batch DAG).
    #    Models built: stg_divvy_trips, dim_stations, fact_divvy_trips,
    #    dim_date (updated to span crime + Divvy), fact_station_day,
    #    crime_ridership_correlation.
    #
    #    --select fact_station_day crime_ridership_correlation pulls in
    #    all upstream dependencies (stg_divvy_trips, dim_stations,
    #    fact_divvy_trips, stg_crime_events, fact_crime_events, dim_date).
    dbt_build_divvy = BashOperator(
        task_id="dbt_build_divvy",
        bash_command=(
            'docker run --rm '
            '--network chicago-data-pipeline_default '
            f'--volumes-from $HOSTNAME '
            '-e GOOGLE_APPLICATION_CREDENTIALS '
            '-e GCP_PROJECT_ID '
            '-e GCS_BUCKET '
            '-e BIGQUERY_LOCATION '
            f'{DBT_IMAGE} '
            f'dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR} '
            '--select fact_station_day crime_ridership_correlation '
            '--exclude stg_station_status fact_station_reads'
        ),
        execution_timeout=timedelta(minutes=45),
    )

    # 3. Record dbt test results into Postgres for Grafana observability
    record_dbt_results = BashOperator(
        task_id="record_dbt_results",
        bash_command=f"python /opt/airflow/scripts/record_dbt_results.py",
    )

    # Task dependencies
    load_divvy_trips >> dbt_build_divvy >> record_dbt_results
