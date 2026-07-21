-- ============================================================
-- stg_crime_events — staging layer for raw crime data
-- ============================================================
-- 1:1 with raw.crime_events. Renames columns to snake_case,
-- casts types, deduplicates on id.
--
-- Source: {{ source('raw', 'crime_events') }} → raw.crime_events
-- Output: staging.stg_crime_events (view)
-- ============================================================

SELECT
    SAFE_CAST(id AS INT64)                                  AS crime_id,
    CAST(case_number AS STRING)                             AS case_number,
    {{ try_cast('date', 'timestamp') }}                     AS occurred_at,
    CAST(primary_type AS STRING)                            AS primary_type,
    CAST(description AS STRING)                             AS description,
    CAST(location_description AS STRING)                    AS location_description,
    CAST(arrest AS BOOLEAN)                                 AS arrest,
    CAST(domestic AS BOOLEAN)                               AS domestic,
    {{ try_cast('community_area', 'int') }}                 AS community_area_id,
    {{ try_cast('district', 'int') }}                       AS district_id,
    {{ try_cast('ward', 'int') }}                           AS ward_id,
    SAFE_CAST(latitude AS FLOAT64)                          AS latitude,
    SAFE_CAST(longitude AS FLOAT64)                         AS longitude,
    SAFE_CAST(year AS INT64)                                AS year,
    {{ try_cast('updated_on', 'timestamp') }}               AS updated_at
FROM {{ source('raw', 'crime_events') }}
-- BigQuery has no DISTINCT ON (Postgres syntax). Use QUALIFY + ROW_NUMBER()
-- to keep the most recently updated row per id. QUALIFY filters after
-- window functions, like a HAVING for windowed aggregates.
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
