#!/usr/bin/env python3
"""
Divvy Trip History Ingestion via dlt (Phase 4.4)
=================================================

Downloads Divvy trip history CSVs from the public S3 bucket
(divvy-tripdata.s3.amazonaws.com), then uses dlt (data load tool)
to load them into BigQuery `raw.divvy_trips`.

S3 bucket structure:
  - 2020-04 onward: monthly ZIPs, e.g. 202306-divvy-tripdata.zip
  - Pre-2020: quarterly ZIPs with different schema (not handled here)

CSV schema (2020+):
  ride_id, rideable_type, started_at, ended_at,
  start_station_name, start_station_id,
  end_station_name, end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual

Usage:
  # Load a single month (sample/test):
  python load_divvy_trips.py --month 202306

  # Load a range of months:
  python load_divvy_trips.py --from 202301 --to 202312

  # Load everything available (2020-04 to latest):
  python load_divvy_trips.py --all

  # Dry run — list what would be loaded without touching BigQuery:
  python load_divvy_trips.py --all --dry-run

Environment variables (set in docker-compose.yml):
  GOOGLE_APPLICATION_CREDENTIALS — path to GCP service account key
  GCP_PROJECT_ID                 — GCP project ID
  BIGQUERY_LOCATION              — BigQuery dataset location (e.g. US)
"""

import argparse
import io
import os

import tempfile
import zipfile

from urllib.request import urlopen

import dlt

# ============================================================
# Configuration
# ============================================================

S3_BASE = "https://divvy-tripdata.s3.amazonaws.com"
DATASET_NAME = "raw"          # BigQuery dataset
TABLE_NAME = "divvy_trips"    # BigQuery table
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "chicago-divvy-pipeline")
BQ_LOCATION = os.environ.get("BIGQUERY_LOCATION", "US")


# ============================================================
# S3 file listing + download
# ============================================================

def list_available_months():
    """
    Query the S3 bucket and return a sorted list of YYYYMM strings
    for monthly Divvy tripdata ZIPs (2020-04 onward).
    """
    import xml.etree.ElementTree as ET

    resp = urlopen(f"{S3_BASE}/")
    tree = ET.parse(resp)
    root = tree.getroot()

    months = []
    # S3 XML namespace
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    for contents in root.findall("s3:Contents", ns):
        key = contents.find("s3:Key", ns).text
        # Match YYYYMM-divvy-tripdata.zip (monthly files only)
        if key and key.endswith("-divvy-tripdata.zip"):
            yyyymm = key.split("-")[0]
            if len(yyyymm) == 6 and yyyymm.isdigit():
                months.append(yyyymm)

    return sorted(months)


def download_and_extract(month, dest_dir):
    """
    Download a monthly ZIP from S3 and extract the CSV to dest_dir.
    Returns the path to the extracted CSV file.
    """
    filename = f"{month}-divvy-tripdata.zip"
    url = f"{S3_BASE}/{filename}"

    print(f"  Downloading {filename}...")
    resp = urlopen(url)
    zip_bytes = io.BytesIO(resp.read())

    with zipfile.ZipFile(zip_bytes) as zf:
        # Find the CSV file (skip __MACOSX metadata)
        csv_name = None
        for name in zf.namelist():
            if name.endswith(".csv") and "__MACOSX" not in name:
                csv_name = name
                break
        if csv_name is None:
            raise FileNotFoundError(f"No CSV found in {filename}")

        zf.extract(csv_name, dest_dir)
        return os.path.join(dest_dir, csv_name)


# ============================================================
# dlt pipeline
# ============================================================

def create_pipeline():
    """
    Create a dlt pipeline targeting BigQuery.
    Uses GOOGLE_APPLICATION_CREDENTIALS for auth (same as bq CLI + dbt).
    """
    return dlt.pipeline(
        pipeline_name="divvy_trips",
        destination=dlt.destinations.bigquery(
            credentials_path=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
            location=BQ_LOCATION,
        ),
        dataset_name=DATASET_NAME,
    )


def load_csv_to_bigquery(csv_path, pipeline, month):
    """
    Load a single CSV file into BigQuery via dlt.
    dlt infers the schema, casts types, and appends to the table.
    """
    import pandas as pd

    # Read CSV with pandas — dlt can accept a pandas DataFrame
    # or a file path. We use DataFrame for explicit type control.
    df = pd.read_csv(csv_path, dtype={
        "ride_id": "string",
        "rideable_type": "string",
        "start_station_name": "string",
        "start_station_id": "string",
        "end_station_name": "string",
        "end_station_id": "string",
        "member_casual": "string",
    })

    # Parse datetime columns
    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
    df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")

    # Add source_month for traceability (which monthly file this came from)
    df["source_month"] = month

    row_count = len(df)
    print(f"  Loaded {row_count} rows from {os.path.basename(csv_path)}")

    # Run the dlt pipeline — append mode (don't replace existing data)
    load_info = pipeline.run(
        df,
        table_name=TABLE_NAME,
        write_disposition="append",
    )
    print(f"  dlt load info: {load_info}")
    return row_count


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Load Divvy trip history from S3 into BigQuery via dlt"
    )
    parser.add_argument("--month", type=str, help="Single month YYYYMM (e.g. 202306)")
    parser.add_argument("--from", dest="from_month", type=str, help="Start month YYYYMM")
    parser.add_argument("--to", dest="to_month", type=str, help="End month YYYYMM")
    parser.add_argument("--all", action="store_true", help="Load all available months")
    parser.add_argument("--dry-run", action="store_true", help="List months without loading")
    args = parser.parse_args()

    # Determine which months to load
    if args.all:
        months = list_available_months()
        print(f"Found {len(months)} monthly files: {months[0]} to {months[-1]}")
    elif args.month:
        months = [args.month]
    elif args.from_month and args.to_month:
        all_months = list_available_months()
        months = [m for m in all_months if args.from_month <= m <= args.to_month]
    else:
        parser.error("Specify --month, --from/--to, or --all")

    if not months:
        print("No months to load.")
        return

    print(f"\nMonths to load: {len(months)}")
    for m in months:
        print(f"  {m}")

    if args.dry_run:
        print("\n--dry-run: not loading anything.")
        return

    # Create dlt pipeline
    print(f"\nCreating dlt pipeline → BigQuery {PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}")
    pipeline = create_pipeline()

    # Process each month
    total_rows = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        for i, month in enumerate(months, 1):
            print(f"\n[{i}/{len(months)}] Processing {month}...")
            try:
                csv_path = download_and_extract(month, tmpdir)
                rows = load_csv_to_bigquery(csv_path, pipeline, month)
                total_rows += rows
            except Exception as e:
                print(f"  ERROR loading {month}: {e}")
                # Continue with next month — don't fail the whole batch
                continue
            finally:
                # Clean up CSV to save disk space
                csv_file = os.path.join(tmpdir, f"{month}-divvy-tripdata.csv")
                if os.path.exists(csv_file):
                    os.remove(csv_file)

    print(f"\nDone. Total rows loaded: {total_rows:,}")


if __name__ == "__main__":
    main()
