-- ============================================================
-- stg_divvy_trips — staging layer for Divvy trip history
-- ============================================================
-- Phase 4.4: Divvy trip history ingested from S3 CSVs via dlt
-- into raw.divvy_trips. This staging view cleans types and filters
-- invalid rows.
--
-- Source: {{ source('raw', 'divvy_trips') }}
-- Output: staging.stg_divvy_trips (view)
--
-- Schema (from dlt load):
--   ride_id, rideable_type, started_at, ended_at,
--   start_station_name, start_station_id,
--   end_station_name, end_station_id,
--   start_lat, start_lng, end_lat, end_lng,
--   member_casual, source_month
--
-- Cleaning:
--   - Cast started_at/ended_at to TIMESTAMP
--   - Filter out rows with null ride_id (data quality)
--   - Filter out rows with null started_at (can't use for analysis)
--   - Filter out rows with coordinates outside Chicago area (1 row in Montreal)
--   - Keep rows with null station info (ebikes may not have stations)
-- ============================================================

SELECT
    CAST(ride_id AS STRING)                  AS ride_id,
    CAST(rideable_type AS STRING)            AS rideable_type,
    {{ try_cast('started_at', 'timestamp') }} AS started_at,
    {{ try_cast('ended_at', 'timestamp') }}   AS ended_at,
    CAST(start_station_name AS STRING)       AS start_station_name,
    CAST(start_station_id AS STRING)         AS start_station_id,
    CAST(end_station_name AS STRING)         AS end_station_name,
    CAST(end_station_id AS STRING)           AS end_station_id,
    SAFE_CAST(start_lat AS FLOAT64)          AS start_lat,
    SAFE_CAST(start_lng AS FLOAT64)          AS start_lng,
    SAFE_CAST(end_lat AS FLOAT64)            AS end_lat,
    SAFE_CAST(end_lng AS FLOAT64)            AS end_lng,
    CAST(member_casual AS STRING)            AS member_casual,
    CAST(source_month AS STRING)             AS source_month
FROM {{ source('raw', 'divvy_trips') }}
WHERE ride_id IS NOT NULL
  AND started_at IS NOT NULL
  -- Filter out coordinates outside Chicago area (1 row had Montreal coords).
  -- Keep nulls — some trips (dockless ebikes) don't have start coordinates.
  AND (start_lat IS NULL
       OR (start_lat BETWEEN 41.0 AND 42.5
           AND start_lng BETWEEN -88.5 AND -87.0))
