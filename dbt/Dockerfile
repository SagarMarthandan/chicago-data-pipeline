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

# Install dbt-postgres — pinned to match host (pyproject.toml)
# dbt_packages/ in ./dbt was installed with these versions.
RUN pip install --no-cache-dir \
    dbt-core==1.11.12 \
    dbt-postgres==1.10.2

WORKDIR /opt/airflow/dbt
