FROM apache/airflow:2.8.4-python3.11

# ============================================================
# Why a custom Airflow image?
# ============================================================
# The official apache/airflow image doesn't include:
#   1. Docker CLI — needed for DockerOperator (runs Spark jobs
#      in isolated containers via docker.sock)
#   2. Airflow providers — the official image includes core only.
#      We need:
#        postgres provider — for PostgresHook, SqlSensor
#        docker provider   — for DockerOperator
# ============================================================

# Switch to root to install system packages
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user (UID 50000 in official image)
USER airflow

# Install Airflow providers from requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
