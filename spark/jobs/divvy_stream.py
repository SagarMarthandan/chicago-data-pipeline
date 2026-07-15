#!/usr/bin/env python3
"""
Divvy Station Status — Spark Structured Streaming Consumer (Phase 2.4)

Reads station status messages from Kafka topic `divvy_station_status`,
parses the JSON payload, casts types, filters stale stations, and writes
each micro-batch to Postgres `raw.station_status` via foreachBatch + JDBC.

Pipeline:
  Divvy GBFS API → Kafka producer → topic divvy_station_status
  → this job (readStream) → from_json → cast/filter → foreachBatch → Postgres

Why Structured Streaming + foreachBatch?
  JDBC has no native streaming sink. foreachBatch bridges the gap:
  each micro-batch is a static DataFrame, which can use the standard
  JDBC batch writer. This is the standard pattern for streaming-to-JDBC.

Why checkpointing?
  The checkpoint location stores the current Kafka offset per partition.
  If the stream restarts, it resumes from the last committed offset
  instead of re-reading from the beginning. Without it, a restart
  duplicates all previously processed messages.

Usage:
  # Continuous streaming (60s micro-batches, matches producer poll interval)
  docker compose exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] /opt/spark/jobs/divvy_stream.py

  # Single micro-batch (process available data, then stop — for testing)
  docker compose exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] /opt/spark/jobs/divvy_stream.py --once

  # Custom Kafka bootstrap server (default: kafka:9092 from env)
  docker compose exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] /opt/spark/jobs/divvy_stream.py --bootstrap kafka:9092
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
)


# ============================================================
# Configuration
# ============================================================

# Kafka — bootstrap server from env (set in docker-compose.yml)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "divvy_station_status"

# Postgres JDBC — uses Docker service name "postgres", not localhost
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "chicago_analytics")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_TABLE = "raw.station_status"

# Credentials from environment (never hardcoded — see docs/conventions/spark.md)
POSTGRES_USER = os.environ.get("POSTGRES_USER", "chicago")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

# Checkpoint location — stores Kafka offsets for fault recovery.
# Mounted as a named volume (spark_checkpoints) in docker-compose.yml.
CHECKPOINT_LOCATION = "/opt/spark/checkpoints/divvy_stream"

# Micro-batch trigger — 60 seconds matches the producer's poll interval.
# Each micro-batch processes all messages that arrived since the last batch.
TRIGGER_INTERVAL = "60 seconds"

# Stale station threshold — stations with last_reported older than 1 hour
# are likely decommissioned (one station had last_reported: 86400 = Jan 2 1970).
# We filter them out to avoid polluting the warehouse with dead-station data.
STALE_THRESHOLD_SECONDS = 3600  # 1 hour


# ============================================================
# JSON Schema for station_status payload
# ============================================================
# The producer sends the full station status dict as JSON.
# This schema tells from_json() how to parse each field.
#
# Key design decisions (from Phase 2.1 GBFS exploration):
#   1. station_id is StringType — mixed format (667 UUIDs + 1349 numeric
#      strings). Casting to bigint would fail on UUIDs.
#   2. is_installed/is_renting/is_returning are IntegerType (0/1), NOT
#      BooleanType. The GBFS spec says integer, and the API returns 0/1.
#      We cast to boolean AFTER parsing (CAST(1 AS BOOLEAN) = true).
#   3. num_scooters_available/unavailable are optional — not in all
#      stations. They're nullable in the schema; from_json returns null
#      for missing fields.
#   4. eightd_has_available_keys is a real boolean (unlike is_* fields).
# ============================================================

STATION_STATUS_SCHEMA = StructType([
    StructField("station_id", StringType(), nullable=False),
    StructField("num_bikes_available", IntegerType(), nullable=True),
    StructField("num_bikes_disabled", IntegerType(), nullable=True),
    StructField("num_docks_available", IntegerType(), nullable=True),
    StructField("num_docks_disabled", IntegerType(), nullable=True),
    StructField("is_installed", IntegerType(), nullable=True),
    StructField("is_renting", IntegerType(), nullable=True),
    StructField("is_returning", IntegerType(), nullable=True),
    StructField("last_reported", LongType(), nullable=True),
    StructField("legacy_id", StringType(), nullable=True),
    StructField("num_ebikes_available", IntegerType(), nullable=True),
    StructField("eightd_has_available_keys", BooleanType(), nullable=True),
    # Optional fields — not present in all stations
    StructField("num_scooters_available", IntegerType(), nullable=True),
    StructField("num_scooters_unavailable", IntegerType(), nullable=True),
])


# ============================================================
# foreachBatch — bridge between streaming and JDBC
# ============================================================
# JDBC has no native Structured Streaming sink. foreachBatch gives us
# each micro-batch as a static DataFrame, which we write with the standard
# JDBC batch writer. This is the official Spark-recommended pattern.
#
# The batch_id parameter is a monotonically increasing counter. We don't
# use it for upserts here (append mode only), but it's useful for logging.

def write_batch(df, batch_id):
    """
    Write one micro-batch to Postgres raw.station_status via JDBC.

    Uses append mode — each batch adds new rows. Deduplication happens
    downstream in DBT staging (stg_station_status), not here.
    """
    count = df.count()
    if count == 0:
        print(f"[batch {batch_id}] 0 rows — skipping write")
        return

    print(f"[batch {batch_id}] writing {count} rows to {POSTGRES_TABLE}")

    (df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("batchsize", 10_000)
        .mode("append")
        .save())

    print(f"[batch {batch_id}] write complete — {count} rows inserted")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Divvy station status Spark Structured Streaming consumer")
    parser.add_argument("--once", action="store_true",
                        help="Process available data and stop (for testing)")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP,
                        help=f"Kafka bootstrap server (default: {KAFKA_BOOTSTRAP})")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("divvy-stream")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(f"Divvy Station Status — Spark Structured Streaming")
    print(f"  Kafka bootstrap:  {args.bootstrap}")
    print(f"  Kafka topic:      {KAFKA_TOPIC}")
    print(f"  Postgres table:   {POSTGRES_TABLE}")
    print(f"  Checkpoint:       {CHECKPOINT_LOCATION}")
    print(f"  Trigger:          {'once' if args.once else TRIGGER_INTERVAL}")
    print(f"  Stale threshold:  {STALE_THRESHOLD_SECONDS}s")
    print("=" * 60)

    # ============================================================
    # 1. readStream from Kafka
    # ============================================================
    # format("kafka") reads from a Kafka topic as a streaming source.
    # Each row has: key (binary), value (binary), topic, partition, offset,
    # timestamp, timestampType.
    #
    # We only need the value (JSON payload) and some Kafka metadata
    # (partition, offset) for traceability.

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ============================================================
    # 2. Parse JSON + cast types + filter stale stations
    # ============================================================
    # Kafka messages arrive as binary (byte array). We convert value to
    # string, then use from_json() to parse into typed columns.
    #
    # Transformations:
    #   - from_json: parse the JSON payload using STATION_STATUS_SCHEMA
    #   - select: extract fields from the parsed struct + Kafka metadata
    #   - CAST(is_* AS BOOLEAN): 0 → false, 1 → true (GBFS uses int, not bool)
    #   - from_unixtime: convert epoch seconds to timestamp
    #   - filter: drop stale stations (last_reported > 1 hour ago)
    #   - current_timestamp: record when Spark processed this message

    parsed = (
        raw_stream
        .selectExpr(
            "CAST(value AS STRING) as json_value",
            "partition as kafka_partition",
            "offset as kafka_offset",
            "timestamp as kafka_timestamp",
        )
        .select(
            F.from_json(F.col("json_value"), STATION_STATUS_SCHEMA).alias("data"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),
        )
        .select(
            F.col("data.station_id").alias("station_id"),
            F.col("data.num_bikes_available").alias("num_bikes_available"),
            F.col("data.num_bikes_disabled").alias("num_bikes_disabled"),
            F.col("data.num_docks_available").alias("num_docks_available"),
            F.col("data.num_docks_disabled").alias("num_docks_disabled"),
            F.col("data.is_installed").cast("boolean").alias("is_installed"),
            F.col("data.is_renting").cast("boolean").alias("is_renting"),
            F.col("data.is_returning").cast("boolean").alias("is_returning"),
            F.from_unixtime(F.col("data.last_reported")).cast("timestamp").alias("last_reported"),
            F.col("data.legacy_id").alias("legacy_id"),
            F.col("data.num_ebikes_available").alias("num_ebikes_available"),
            F.col("data.eightd_has_available_keys").alias("eightd_has_available_keys"),
            F.col("data.num_scooters_available").alias("num_scooters_available"),
            F.col("data.num_scooters_unavailable").alias("num_scooters_unavailable"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),
            F.current_timestamp().alias("ingest_timestamp"),
        )
        # Filter stale stations — last_reported must be within 1 hour of now.
        # This drops the dead station with last_reported: 86400 (Jan 2 1970)
        # and any stations that stopped reporting.
        .filter(
            F.col("last_reported").isNotNull()
            & (F.col("last_reported") > F.current_timestamp() - F.expr(f"INTERVAL {STALE_THRESHOLD_SECONDS} SECONDS"))
        )
    )

    # ============================================================
    # 3. writeStream — foreachBatch to Postgres
    # ============================================================
    # foreachBatch bridges Structured Streaming and JDBC:
    #   - Each micro-batch is a static DataFrame → standard JDBC writer works
    #   - checkpointLocation stores Kafka offsets for fault recovery
    #   - trigger(processingTime="60 seconds") matches producer poll interval
    #   - outputMode("append") — only new rows, no updates/deletes

    query = (
        parsed.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
    )

    if args.once:
        # Process all available data in one micro-batch, then stop.
        # Useful for testing without waiting for the 60s trigger.
        query = query.trigger(once=True)

    stream = query.start()

    if args.once:
        # Wait for the single batch to complete, then stop.
        stream.awaitTermination()
        print("Single batch complete — stopping.")
    else:
        # Run indefinitely until terminated (SIGINT/SIGTERM).
        # awaitTermination blocks the main thread; Spark handles
        # graceful shutdown on signal.
        print("Streaming started. Press Ctrl+C to stop.")
        stream.awaitTermination()

    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    main()
