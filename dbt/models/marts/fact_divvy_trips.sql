-- ============================================================
-- fact_divvy_trips — Divvy trip fact table (Phase 4.4)
-- ============================================================
-- One row per Divvy trip, with date_key FK to dim_date.
-- Partitioned by started_at (timestamp) for partition pruning.
-- Clustered by start_station_id for station-level query efficiency.
--
-- Source: {{ ref('stg_divvy_trips') }}
-- Output: mart.fact_divvy_trips (table, partitioned + clustered)
--
-- BigQuery partitioning requires materialized='table' (not view).
-- Partition by started_at (daily partitions), cluster by start_station_id
-- — the most common filter/group-by column for ridership analysis.
-- ============================================================

{{
    config(
        materialized='table',
        partition_by={"field": "started_at", "data_type": "timestamp"},
        cluster_by=["start_station_id"]
    )
}}

SELECT
    ride_id,
    started_at,
    ended_at,
    DATE(started_at) AS date_key,
    rideable_type,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    source_month,
    -- Trip duration in seconds (useful for ridership analysis)
    TIMESTAMP_DIFF(ended_at, started_at, SECOND) AS trip_duration_seconds
FROM {{ ref('stg_divvy_trips') }}
