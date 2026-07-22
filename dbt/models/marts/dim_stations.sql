-- ============================================================
-- dim_stations — Divvy station dimension (Phase 4.4)
-- ============================================================
-- Extracted from fact_divvy_trips: distinct stations with their
-- coordinates (from start_station). Some stations may have slightly
-- different coordinates across trips — we take the most common
-- (mode) coordinate per station.
--
-- Source: {{ ref('stg_divvy_trips') }}
-- Output: mart.dim_stations (table)
--
-- Note: station_id is a string (mixed UUID + numeric, per Phase 2.1 finding).
-- Only includes stations with non-null coordinates (needed for geospatial join).
-- ============================================================

{{
    config(
        materialized='table'
    )
}}

WITH station_coords AS (
    SELECT
        start_station_id AS station_id,
        start_station_name AS station_name,
        start_lat AS lat,
        start_lng AS lng
    FROM {{ ref('stg_divvy_trips') }}
    WHERE start_station_id IS NOT NULL
      AND start_lat IS NOT NULL
      AND start_lng IS NOT NULL
),

-- Pick the most frequently occurring coordinate pair per station
-- (some stations report slightly different coords across trips)
station_coord_counts AS (
    SELECT
        station_id,
        station_name,
        lat,
        lng,
        COUNT(*) AS occurrence_count,
        ROW_NUMBER() OVER (
            PARTITION BY station_id
            ORDER BY COUNT(*) DESC, lat, lng
        ) AS coord_rank
    FROM station_coords
    GROUP BY station_id, station_name, lat, lng
)

SELECT
    station_id,
    -- Pick the most common name per station
    ANY_VALUE(station_name HAVING MAX(occurrence_count)) AS station_name,
    lat,
    lng,
    ST_GEOGPOINT(lng, lat) AS geo_point
FROM station_coord_counts
WHERE coord_rank = 1
GROUP BY station_id, lat, lng
