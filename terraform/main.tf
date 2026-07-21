# Resources — the actual cloud infrastructure Terraform will create.
#
# Phase 4.2 provisions exactly three resources (deliberately minimal):
#   1. BigQuery dataset "raw"   — landing zone for crime + Divvy trips
#   2. BigQuery dataset "mart"  — DBT-built analytics marts
#   3. GCS bucket "data_lake"   — Parquet files (Spark writes here instead of Postgres)
#
# We do NOT create BigQuery tables here. DBT (Phase 4.3+) creates tables inside
# these datasets. Terraform manages the containers; DBT manages the contents.

# ---------------------------------------------------------------------------
# BigQuery datasets
# ---------------------------------------------------------------------------
# A dataset is a container for tables. Think of it like a Postgres schema.
# We use two datasets to mirror the local Postgres schema layout (raw + mart)
# so the DBT model structure stays the same when we switch adapters.

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.region
  description = "Raw landing zone — crime (from bigquery-public-data) + Divvy trips (from S3 via Airbyte) land here"

  # Delete contents on destroy — safe for a learning project where we re-run
  # pipelines from scratch. Set to false in production to prevent data loss.
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "mart" {
  dataset_id  = "mart"
  location    = var.region
  description = "DBT-built analytics marts — fact_station_day, crime_ridership_correlation, etc."

  delete_contents_on_destroy = true
}

# ---------------------------------------------------------------------------
# GCS bucket (data lake)
# ---------------------------------------------------------------------------
# Spark writes Parquet here instead of writing directly to Postgres.
# BigQuery can query Parquet in GCS via external tables, or we load it into
# BigQuery native tables via bq load. The bucket is the staging area.

resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true # allow terraform destroy even if bucket has objects (learning project)

  # Uniform bucket-level access (recommended) — use IAM, not legacy ACLs
  uniform_bucket_level_access = true

  # Auto-delete objects older than 90 days — keeps the data lake from growing
  # unbounded. Parquet is a staging format; once loaded into BigQuery it's
  # redundant. Adjust or remove for production.
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}
