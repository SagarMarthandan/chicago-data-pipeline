#!/bin/bash
set -e

# Fix checkpoint directory ownership on every container start.
# Named volumes (spark_checkpoints) mount as root:root, but Spark runs
# as the spark user. Without this chown, Structured Streaming fails with
# "mkdir of /opt/spark/checkpoints/divvy_stream failed" because the spark
# user can't create subdirectories inside a root-owned directory.
#
# This runs as root (USER root in Dockerfile) before dropping to spark.
chown -R spark:spark /opt/spark/checkpoints 2>/dev/null || true

# Drop privileges to spark user and exec the original command.
# gosu is the standard Docker privilege-dropping tool — it preserves
# signal handling and exit codes better than su/sudo.
exec gosu spark "$@"
