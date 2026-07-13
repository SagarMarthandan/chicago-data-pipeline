#!/usr/bin/env python3
"""
Spark Batch Job — Crime Data ETL (Phase 1.3)

Reads Chicago crime data from Parquet, cleans it, and writes to
Postgres raw.crime_events via JDBC.

Pipeline: Read Parquet → Clean → Write to Postgres

Parquet path inside the Spark container:
    /opt/spark/data/raw/crime/crime_2023.parquet
(host ./data is bind-mounted to /opt/spark/data in docker-compose.yml)

Usage (inside spark-master container, local mode for testing):
    spark-submit --master local[*] /opt/spark/jobs/crime_batch.py

Usage (from host via docker exec):
    docker compose exec spark-master spark-submit \
        --master local[*] /opt/spark/jobs/crime_batch.py

Usage (cluster mode — driver on master, executors on workers):
    spark-submit --master spark://spark-master:7077 \
        --deploy-mode client \
        /opt/spark/jobs/crime_batch.py

Postgres credentials come from environment variables (POSTGRES_USER,
POSTGRES_PASSWORD) which are set in the Spark services in docker-compose.yml.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Configuration
# ============================================================

# Parquet input — inside the Spark container, ./data is mounted at /opt/spark/data
PARQUET_PATH = "/opt/spark/data/raw/crime/crime_2023.parquet"

# Postgres JDBC — uses Docker service name "postgres", not localhost
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "chicago_analytics")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_TABLE = "raw.crime_events"

# Credentials from environment (never hardcoded — see docs/conventions/spark.md)
POSTGRES_USER = os.environ.get("POSTGRES_USER", "chicago")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")

# JDBC write tuning
JDBC_BATCH_SIZE = 10_000   # rows per batch insert (default 1000 is slow)
JDBC_NUM_PARTITIONS = 8    # parallel JDBC connections — must match repartition()


# ============================================================
# Cleaning
# ============================================================

def clean(df):
    """
    Clean the raw crime DataFrame.

    The ingestion script (download_crime.py) already does light cleaning
    before writing Parquet: numeric columns are pd.to_numeric'd, booleans
    are mapped, and the nested 'location' column is dropped. This function
    handles what Spark is responsible for:

    1. Cast id from string to long (Socrata returns it as a string).
       Non-numeric values become null → caught by the null-id filter.
    2. Drop rows where id is null (primary key must not be null).
    3. Deduplicate on id — do this early, before any shuffles.
    4. Parse date and updated_on from ISO 8601 strings to timestamps.
    5. Normalize primary_type: uppercase + trim (THEFT vs theft vs Theft).
    6. Cast community_area from double to int (safety net — ingestion
       already converts it to numeric, but the cast guarantees the type
       downstream and costs nothing).
    7. Keep null lat/long as-is. They are already double nulls from the
       ingestion script's pd.to_numeric(errors="coerce"). Dropping them
       would lose too many rows; DBT/Spark can flag them later.
    """
    return (
        df
        # 1. Cast id to long — non-numeric strings become null
        .withColumn("id", F.col("id").cast("long"))
        # 2. Drop rows with null id (data quality — id is the primary key)
        .filter(F.col("id").isNotNull())
        # 3. Deduplicate on id before any shuffles reduce data volume
        .dropDuplicates(["id"])
        # 4. Parse date strings to timestamps (Socrata returns ISO 8601)
        .withColumn("date", F.to_timestamp("date"))
        .withColumn("updated_on", F.to_timestamp("updated_on"))
        # 5. Normalize primary_type casing (real data has mixed casing)
        .withColumn("primary_type", F.upper(F.trim("primary_type")))
        # 6. Cast community_area to int (safety net — already double from ingestion)
        .withColumn("community_area", F.col("community_area").cast("int"))
    )


# ============================================================
# Write
# ============================================================

def write_to_postgres(df, table):
    """
    Write DataFrame to Postgres via JDBC.

    Uses overwrite mode for Phase 1 — idempotent (replaces the whole
    table each run). Switch to upsert (MERGE INTO via temp table) in
    Phase 2+ for incremental loads. See docs/conventions/spark.md.
    """
    (
        df.repartition(JDBC_NUM_PARTITIONS)
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("batchsize", JDBC_BATCH_SIZE)
        .option("numPartitions", JDBC_NUM_PARTITIONS)
        .mode("overwrite")
        .save()
    )


# ============================================================
# Main
# ============================================================

def main():
    spark = (
        SparkSession.builder
        .appName("crime-batch")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- 1. Read ---
    print(f"{'='*60}")
    print(f"  Reading Parquet: {PARQUET_PATH}")
    print(f"{'='*60}")
    df = spark.read.parquet(PARQUET_PATH)
    raw_count = df.count()
    print(f"  Raw row count: {raw_count:,}")
    print(f"  Columns ({len(df.columns)}): {df.columns}")
    df.printSchema()

    # --- 2. Clean ---
    print(f"\n{'='*60}")
    print("  Cleaning...")
    print(f"{'='*60}")
    df_clean = clean(df)
    clean_count = df_clean.count()
    dropped = raw_count - clean_count
    print(f"  Cleaned row count: {clean_count:,}")
    print(f"  Rows dropped (null id + duplicates): {dropped:,}")
    df_clean.printSchema()

    # --- 3. Write ---
    print(f"\n{'='*60}")
    print(f"  Writing to Postgres: {POSTGRES_TABLE}")
    print(f"  JDBC URL: {POSTGRES_URL}")
    print(f"{'='*60}")
    write_to_postgres(df_clean, POSTGRES_TABLE)
    print("  Write complete.")

    # --- 4. Verify ---
    print(f"\n{'='*60}")
    print("  Verifying row count in Postgres...")
    print(f"{'='*60}")
    df_verify = (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .load()
    )
    pg_count = df_verify.count()
    print(f"  Rows in Postgres {POSTGRES_TABLE}: {pg_count:,}")
    if pg_count != clean_count:
        print(f"  WARNING: mismatch! Spark={clean_count:,} vs Postgres={pg_count:,}")
    else:
        print("  Row counts match.")

    spark.stop()
    print(f"\n{'='*60}")
    print("  Done.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
