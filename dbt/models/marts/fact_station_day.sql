-- ============================================================
-- fact_station_day — THE analytics mart (Phase 4.4)
-- ============================================================
-- One row per station per day: trip_count + crime_count_within_quarter_mile.
-- This is the table that answers the driving question:
--   "Does crime near a Divvy station affect ridership?"
--
-- Geospatial join: crimes within 402 meters (0.25 mile) of a station,
-- occurring on the same day or within the prior 24 hours.
--
-- Sources:
--   {{ ref('dim_stations') }} — station locations
--   {{ ref('fact_divvy_trips') }} — trip counts per station per day
--   {{ ref('fact_crime_events') }} — crime events with lat/long
--
-- Output: mart.fact_station_day (table, partitioned by date_key)
--
-- Partitioning: by date_key (daily) — enables efficient date-range queries.
-- Clustering: by station_id — station-level analysis is the primary access pattern.
-- ============================================================

{{
    config(
        materialized='table',
        partition_by={"field": "date_key", "data_type": "date"},
        cluster_by=["station_id"]
    )
}}

WITH trip_counts AS (
    SELECT
        start_station_id AS station_id,
        DATE(started_at) AS date_key,
        COUNT(*) AS trip_count
    FROM {{ ref('fact_divvy_trips') }}
    WHERE start_station_id IS NOT NULL
    GROUP BY 1, 2
),

-- Crimes per day with coordinates (only those with lat/long for geospatial join)
crime_daily AS (
    SELECT
        date_key,
        latitude,
        longitude,
        ST_GEOGPOINT(longitude, latitude) AS geo_point
    FROM {{ ref('fact_crime_events') }}
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
),

-- For each station-day, count crimes within 0.25 mile (402 meters)
-- on the same day. We join on date equality + spatial proximity.
-- The ST_DISTANCE check is applied after the date partition prune.
crime_counts AS (
    SELECT
        s.station_id,
        t.date_key,
        COUNT(c.date_key) AS crime_count_within_quarter_mile
    FROM trip_counts t
    JOIN {{ ref('dim_stations') }} s
        ON t.station_id = s.station_id
    LEFT JOIN crime_daily c
        ON c.date_key = t.date_key
       AND ST_DISTANCE(s.geo_point, c.geo_point) <= 402  -- 0.25 mile in meters
    GROUP BY 1, 2
)

SELECT
    t.station_id,
    t.date_key,
    t.trip_count,
    COALESCE(c.crime_count_within_quarter_mile, 0) AS crime_count_within_quarter_mile
FROM trip_counts t
LEFT JOIN crime_counts c
    ON t.station_id = c.station_id
   AND t.date_key = c.date_key
