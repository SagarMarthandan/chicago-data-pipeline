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

# ============================================================
# Google Cloud SDK (Phase 4.3 — bq load task)
# ============================================================
# Airflow needs the `bq` CLI to load GCS Parquet → BigQuery.
# The bq CLI is part of the Google Cloud SDK.
#
# Auth: GOOGLE_APPLICATION_CREDENTIALS env var (set in docker-compose.yml)
# points to the mounted service account key file. The bq CLI picks it up
# automatically — no `gcloud auth login` needed (that's for humans).
#
# We install the minimal gcloud SDK (no extras) to keep the image small.
# The key file is mounted read-only at /opt/airflow/gcp-credentials.json.
RUN apt-get update \
    && apt-get install -y --no-install-recommends apt-transport-https ca-certificates gnupg curl \
    && curl -sL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# Add airflow user to the docker group so it can access docker.sock.
# The docker.io package creates a "docker" group at an arbitrary GID, but
# it must match the host's docker.sock GID for the airflow user to access it.
# DOCKER_GID is a build arg — set it in .env via docker-compose build args.
# Find your GID with: stat -c '%g' /var/run/docker.sock
ARG DOCKER_GID=999
RUN groupdel docker 2>/dev/null; groupadd -g ${DOCKER_GID} docker && usermod -aG docker airflow


# Install Python dependencies as the airflow user.
# The apache/airflow image uses a virtualenv at /home/airflow/.local
# (VIRTUAL_ENV=/home/airflow/.local, pip at /home/airflow/.local/bin/pip).
# The image refuses `pip install` as root ("Please use 'airflow' user").
# Running pip as airflow (without --user — the venv IS the install target)
# installs into /home/airflow/.local/lib/python3.11/site-packages/.
# We previously used `uv pip install --system` as root to bypass the root
# guard, but uv fails to create the `kafka` directory (permission denied).
USER airflow
COPY --chown=airflow:airflow requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to airflow user for running the services
USER airflow
