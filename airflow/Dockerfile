FROM apache/airflow:3.0.0-python3.11

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

# Install uv by copying from the official uv Docker image.
# This is a multi-stage COPY — pulls just the uv binary, no install script needed.
# Faster and more reliable than curl | sh or pip install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Airflow providers using uv pip install (10-100x faster than pip).
# --system: install into the container's system Python (no venv needed in containers).
# --no-cache-dir: don't keep downloaded wheels (saves image size).
COPY requirements.txt /requirements.txt
RUN uv pip install --system --no-cache-dir -r /requirements.txt
