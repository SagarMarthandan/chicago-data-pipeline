#!/usr/bin/env python3
"""
Chicago Crime Data Ingestion — Phase 1.2

Downloads crime data from the Chicago Data Portal (Socrata API)
and writes it to local Parquet files.

Dataset: "Crimes - 2001 to Present"
  Resource ID: ijzp-q8t2
  API endpoint: https://data.cityofchicago.org/resource/ijzp-q8t2.json

Usage:
  python ingestion/download_crime.py                    # downloads 2023 data (default)
  python ingestion/download_crime.py --year 2024        # downloads specific year
  python ingestion/download_crime.py --year all         # downloads all years (slow!)

The Socrata API:
  - Returns JSON rows (each row is a crime incident)
  - Supports $limit (max 50,000 per request) and $offset for pagination
  - Supports $where for filtering (e.g. year=2023)
  - Rate limits: 1,000 requests/hour anonymous, 10,000/hour with app token
  - App token is OPTIONAL — passed via X-App-Token header if set

Parquet output:
  - Columnar format, preserves types, Spark-friendly
  - Written to data/raw/crime/crime_<year>.parquet
  - Partitioned by year if downloading all years
"""

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

# ============================================================
# Configuration
# ============================================================

# Socrata API endpoint for Chicago Crimes dataset
SOCRATA_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

# Page size — Socrata allows max 50,000 rows per request
PAGE_SIZE = 50_000

# Output directory for Parquet files
DATA_DIR = Path(__file__).parent.parent / "data" / "raw" / "crime"

# Load environment variables from .env
load_dotenv(Path(__file__).parent.parent / ".env")


def get_app_token() -> str | None:
    """Get Socrata app token from environment. Returns None if not set."""
    token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    return token if token else None


def build_headers() -> dict:
    """Build HTTP headers for Socrata API request."""
    headers = {"Accept": "application/json"}
    token = get_app_token()
    if token:
        headers["X-App-Token"] = token
    return headers


def build_params(year: str, offset: int, limit: int = PAGE_SIZE) -> dict:
    """
    Build Socrata API query parameters.

    Socrata uses SoQL (Socrata Query Language) for filtering:
      $where  — filter condition (SQL-like)
      $limit  — max rows to return (max 50,000)
      $offset — skip this many rows (for pagination)
      $order  — sort order (required for stable pagination)
    """
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "id",
    }
    if year != "all":
        params["$where"] = f"year={year}"
    return params


def fetch_page(year: str, offset: int) -> list[dict]:
    """
    Fetch a single page of crime data from the Socrata API.

    Returns a list of dicts (one per crime incident).
    Raises an exception if the API returns a non-200 status code.
    """
    headers = build_headers()
    params = build_params(year, offset)

    response = requests.get(SOCRATA_URL, headers=headers, params=params, timeout=120)
    response.raise_for_status()

    return response.json()


def download_year(year: str) -> pd.DataFrame:
    """
    Download all crime data for a given year (or all years).

    Paginates through the API in chunks of PAGE_SIZE rows.
    Prints progress as it goes.
    """
    print(f"\n{'='*60}")
    print(f"  Downloading crime data for year: {year}")
    print(f"  API: {SOCRATA_URL}")
    token = get_app_token()
    print(f"  App token: {'yes (10K req/hr limit)' if token else 'no (1K req/hr limit)'}")
    print(f"  Page size: {PAGE_SIZE:,} rows")
    print(f"{'='*60}\n")

    all_rows = []
    offset = 0
    page_num = 0

    while True:
        page_num += 1
        try:
            rows = fetch_page(year, offset)
        except requests.HTTPError as e:
            print(f"  ERROR on page {page_num}: {e}")
            print(f"  Response body: {e.response.text[:500]}")
            raise

        if not rows:
            # Empty response means we've paginated past all data
            break

        all_rows.extend(rows)
        row_count = len(rows)
        total_so_far = len(all_rows)
        print(f"  Page {page_num}: fetched {row_count:,} rows (total: {total_so_far:,})")

        if row_count < PAGE_SIZE:
            # Last page (fewer than PAGE_SIZE rows means no more data)
            break

        offset += PAGE_SIZE

        # Small delay to be polite to the API
        time.sleep(0.1)

    print(f"\n  Total rows downloaded: {len(all_rows):,}")

    if not all_rows:
        print(f"  WARNING: No data found for year {year}")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)

    print(f"  Columns: {len(df.columns)}")
    print(f"  DataFrame shape: {df.shape}")

    return df

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light cleaning before writing to Parquet.

    This is NOT the full Spark cleaning (that happens in Phase 1.3).
    This just handles API-level quirks:
      - Socrata returns 'location' as a nested dict with lat/long
      - Some columns come as strings that should be numeric
      - Drop the 'location' column (we already have latitude/longitude)
      - Drop Socrata computed region columns (geocoding metadata)
    """
    # Socrata wraps coordinates in a 'location' column like:
    #   {"latitude": "41.8", "longitude": "-87.6", "human_address": "{...}"}
    # We already have separate latitude/longitude columns, so drop it.
    if "location" in df.columns:
        df = df.drop(columns=["location"])
        print("  Dropped 'location' column (redundant with latitude/longitude)")

    # Drop Socrata computed region columns (internal geocoding metadata, not useful)
    computed_cols = [c for c in df.columns if c.startswith(":@computed_region")]
    if computed_cols:
        df = df.drop(columns=computed_cols)
        print(f"  Dropped {len(computed_cols)} :@computed_region columns (geocoding metadata)")

    # Convert known numeric columns from string to numeric
    # Socrata returns everything as strings; we let Parquet handle the rest
    numeric_cols = ["latitude", "longitude", "district", "ward", "community_area", "year"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert boolean columns (Socrata returns them as strings "true"/"false")
    bool_cols = ["arrest", "domestic"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})

    return df


def write_parquet(df: pd.DataFrame, year: str) -> Path:
    """
    Write DataFrame to a Parquet file.

    Output path: data/raw/crime/crime_<year>.parquet
    Returns the path to the written file.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if year == "all":
        # Partition by year when downloading all years
        output_path = DATA_DIR / "crime_all.parquet"
        pq.write_table(pa.Table.from_pandas(df), output_path)
    else:
        output_path = DATA_DIR / f"crime_{year}.parquet"
        pq.write_table(pa.Table.from_pandas(df), output_path)

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n  Written: {output_path}")
    print(f"  File size: {file_size_mb:.1f} MB")

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Download Chicago crime data from Socrata API to Parquet"
    )
    parser.add_argument(
        "--year",
        default="2023",
        help="Year to download (e.g. 2023, 2024). Use 'all' for all years. Default: 2023",
    )
    args = parser.parse_args()

    # Download
    df = download_year(args.year)
    if df.empty:
        print("\nNo data to write. Exiting.")
        sys.exit(0)

    # Light cleaning
    print("\n  Cleaning DataFrame...")
    df = clean_dataframe(df)

    # Write to Parquet
    output_path = write_parquet(df, args.year)

    # Summary
    print(f"\n{'='*60}")
    print(f"  DONE — {len(df):,} rows written to {output_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
