# DBT Container — separate image to avoid protobuf conflict with Airflow 3.0
#
# dbt-core 1.11 requires protobuf >=6.0, but Airflow 3.0 pins protobuf 4.x.
# They can't coexist in the same Python environment, so dbt runs in its
# own container via `docker run --rm` from a BashOperator in the DAG.
#
# Usage (from DAG):
#   docker run --rm --network chicago-data-pipeline_default \
#     --volumes-from <airflow-scheduler-container> \
#     chicago-data-pipeline-dbt:latest \
#     dbt build --profiles-dir /opt/airflow/dbt_profiles --project-dir /opt/airflow/dbt

FROM python:3.11-slim

# Install dbt-bigquery (Phase 4.3 — switched from dbt-postgres).
# dbt-bigquery 1.12.0 pulls dbt-core 1.12.0 as a dependency.
# The BigQuery adapter uses the google-cloud-bigquery Python client,
# which auths via GOOGLE_APPLICATION_CREDENTIALS env var (mounted key file).
#
# Note: dbt-postgres is no longer installed. The analytics warehouse is
# BigQuery now. Local Postgres is only used for the streaming path
# (divvy_stream) and the observability schema (dbt_test_results) — neither
# is managed by DBT.
RUN pip install --no-cache-dir \
    dbt-bigquery==1.12.0

WORKDIR /opt/airflow/dbt
