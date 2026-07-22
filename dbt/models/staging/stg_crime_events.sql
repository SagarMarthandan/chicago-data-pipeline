-- ============================================================
-- stg_crime_events — staging layer for crime data
-- ============================================================
-- Phase 4.4: Now reads from bigquery-public-data.chicago_crime.crime
-- (8.6M rows, 2001-present) instead of raw.crime_events (263K Socrata
-- extract). The public dataset is free to query and gives full history.
--
-- Filters to 2018-present to keep marts focused on the period where
-- Divvy trip data overlaps (Divvy S3 data starts 2020-04; we include
-- 2018-2019 for crime trend context).
--
-- Column mapping (public dataset → our schema):
--   unique_key  → crime_id (was `id` in Socrata extract)
--   date        → occurred_at
--   updated_on  → updated_at
--   All other columns match.
--
-- Source: {{ source('chicago_crime_public', 'crime') }}
-- Output: staging.stg_crime_events (view)
-- ============================================================

SELECT
    SAFE_CAST(unique_key AS INT64)                              AS crime_id,
    CAST(case_number AS STRING)                                 AS case_number,
    {{ try_cast('date', 'timestamp') }}                         AS occurred_at,
    CAST(primary_type AS STRING)                                AS primary_type,
    CAST(description AS STRING)                                 AS description,
    CAST(location_description AS STRING)                        AS location_description,
    CAST(arrest AS BOOLEAN)                                     AS arrest,
    CAST(domestic AS BOOLEAN)                                   AS domestic,
    {{ try_cast('community_area', 'int') }}                     AS community_area_id,
    {{ try_cast('district', 'int') }}                           AS district_id,
    {{ try_cast('ward', 'int') }}                               AS ward_id,
    SAFE_CAST(latitude AS FLOAT64)                              AS latitude,
    SAFE_CAST(longitude AS FLOAT64)                             AS longitude,
    SAFE_CAST(year AS INT64)                                    AS year,
    {{ try_cast('updated_on', 'timestamp') }}                   AS updated_at
FROM {{ source('chicago_crime_public', 'crime') }}
WHERE year >= 2018
  -- Filter out bad coordinates (a few rows have Missouri lat/long — data entry
  -- errors in the public dataset). Keep nulls (~0.8% of rows) — they're valid
  -- crimes with unknown location, still useful for non-spatial analysis.
  AND (latitude IS NULL
       OR (latitude BETWEEN 41.64 AND 42.03
           AND longitude BETWEEN -87.95 AND -87.52))
-- BigQuery has no DISTINCT ON (Postgres syntax). Use QUALIFY + ROW_NUMBER()
-- to keep the most recently updated row per unique_key. QUALIFY filters after
-- window functions, like a HAVING for windowed aggregates.
QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY updated_on DESC) = 1
