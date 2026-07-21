#!/usr/bin/env python3
"""
Spark Batch Job — Crime Data ETL (Phase 4.3 — cloud migration)

Reads Chicago crime data from Parquet, cleans it, and writes to
Google Cloud Storage (GCS) as Parquet. A separate `bq load` step
(Airflow task) then loads GCS Parquet → BigQuery raw.crime_events.

Pipeline: Read Parquet → Clean → Write to GCS (Parquet)

Parquet path inside the Spark container:
    /opt/spark/data/raw/crime/crime_2023.parquet
(host ./data is bind-mounted to /opt/spark/data in docker-compose.yml)

GCS output:
    gs://chicago-divvy-pipeline-data-lake/raw/crime/
(GCS connector JAR baked into Spark image, auth via GOOGLE_APPLICATION_CREDENTIALS)

Usage (inside spark-master container, local mode for testing):
    spark-submit --master local[*] /opt/spark/jobs/crime_batch.py

Usage (from host via docker exec):
    docker compose exec spark-master spark-submit \
        --master local[*] /opt/spark/jobs/crime_batch.py

GCP credentials come from GOOGLE_APPLICATION_CREDENTIALS env var
(set in docker-compose.yml, pointing to the mounted service account key).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Configuration
# ============================================================

# Parquet input — inside the Spark container, ./data is mounted at /opt/spark/data
PARQUET_PATH = "/opt/spark/data/raw/crime/crime_2023.parquet"

# GCS output — data lake bucket provisioned by Terraform (Phase 4.2)
# Spark writes Parquet here; Airflow's bq_load_crime task loads it into BigQuery.
GCS_BUCKET = os.environ.get("GCS_BUCKET", "chicago-divvy-pipeline-data-lake")
GCS_OUTPUT_PATH = f"gs://{GCS_BUCKET}/raw/crime/"

# Number of partitions for the GCS write — controls Parquet file count.
# Too few = large files (slow reads). Too many = small files (slow BigQuery load).
# 8 is a good default for 263K rows (~33K rows/file).
OUTPUT_PARTITIONS = 8

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

def write_to_gcs(df, path):
    """
    Write DataFrame to Google Cloud Storage as Parquet.

    Uses overwrite mode — idempotent (replaces all Parquet files in the
    target directory each run). The GCS connector JAR (baked into the
    Spark image) handles gs:// URIs. Auth via GOOGLE_APPLICATION_CREDENTIALS
    env var (set in docker-compose.yml).

    The `bq load` step (Airflow task) then loads this Parquet into BigQuery.
    """
    (
        df.repartition(OUTPUT_PARTITIONS)
        .write
        .mode("overwrite")
        .parquet(path)
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

    # --- 3. Write to GCS ---
    print(f"\n{'='*60}")
    print(f"  Writing to GCS: {GCS_OUTPUT_PATH}")
    print(f"  Partitions: {OUTPUT_PARTITIONS}")
    print(f"{'='*60}")
    write_to_gcs(df_clean, GCS_OUTPUT_PATH)
    print("  Write complete.")

    # --- 4. Verify (read back from GCS) ---
    print(f"\n{'='*60}")
    print("  Verifying row count in GCS...")
    print(f"{'='*60}")
    df_verify = spark.read.parquet(GCS_OUTPUT_PATH)
    gcs_count = df_verify.count()
    print(f"  Rows in GCS {GCS_OUTPUT_PATH}: {gcs_count:,}")
    if gcs_count != clean_count:
        print(f"  WARNING: mismatch! Spark={clean_count:,} vs GCS={gcs_count:,}")
    else:
        print("  Row counts match.")

    spark.stop()
    print(f"\n{'='*60}")
    print("  Done.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
