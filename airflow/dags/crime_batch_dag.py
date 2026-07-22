"""
Orchestrates the crime batch pipeline (Phase 4.4 — public dataset).

Phase 4.4 changes:
  - Crime data now sourced from bigquery-public-data.chicago_crime (8.6M rows,
    2001-present) instead of the Socrata API extract (263K rows, 2023 only).
  - No ingestion needed — the public dataset is referenced directly in DBT.
  - download_crime, spark_crime_batch, bq_load_crime tasks REMOVED.
  - DAG is now just: dbt_build (crime models) → record_dbt_results.

The Socrata pipeline (download_crime.py, crime_batch.py, bq_load_crime) still
exists in the codebase as a fallback, but the analytics marts use the public
dataset for full crime history (2018-present).

DBT models built by this DAG:
  - stg_crime_events (reads bigquery-public-data.chicago_crime.crime)
  - dim_community_area (seed)
  - dim_crime_type
  - fact_crime_events (partitioned by date_key, clustered by community_area)
  - dim_date (spans crime + Divvy dates — but Divvy models are built by
    divvy_trip_history DAG; this DAG builds dim_date with crime bounds only
    if Divvy models haven't been built yet)

GCP credentials: /opt/airflow/gcp-credentials.json (GOOGLE_APPLICATION_CREDENTIALS).
DBT uses airflow/dbt_profiles/profiles.yml (BigQuery adapter, service account).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from callbacks import on_failure_callback

# ============================================================
# Configuration
# ============================================================

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
    dag_id="crime_batch",
    default_args=default_args,
    schedule=None,  # manual trigger — switch to "@daily" after verification
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crime", "batch", "phase-4"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # 1. Run DBT build for crime models.
    #    --select fact_crime_events pulls in stg_crime_events, dim_community_area,
    #    dim_crime_type, dim_date (upstream deps).
    #    --exclude stg_station_status fact_station_reads (streaming models, stay on Postgres).
    #    --exclude stg_divvy_trips dim_stations fact_divvy_trips fact_station_day
    #    crime_ridership_correlation (Divvy models, built by divvy_trip_history DAG).
    #
    #    Note: dim_date depends on both stg_crime_events AND stg_divvy_trips.
    #    If divvy_trip_history hasn't run yet, dim_date will fail (stg_divvy_trips
    #    source table raw.divvy_trips won't exist). Run divvy_trip_history first.
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
            f'dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR} '
            '--select fact_crime_events '
            '--exclude stg_station_status fact_station_reads '
            'stg_divvy_trips dim_stations fact_divvy_trips '
            'fact_station_day crime_ridership_correlation'
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # 2. Record dbt test results into Postgres for Grafana observability
    record_dbt_results = BashOperator(
        task_id="record_dbt_results",
        bash_command=f"python /opt/airflow/scripts/record_dbt_results.py",
    )

    # Task dependencies
    dbt_build >> record_dbt_results
