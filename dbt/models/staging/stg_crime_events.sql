-- ============================================================
-- stg_crime_events — staging layer for raw crime data
-- ============================================================
-- 1:1 with raw.crime_events. Renames columns to snake_case,
-- casts types, deduplicates on id.
--
-- Source: {{ source('raw', 'crime_events') }} → raw.crime_events
-- Output: staging.stg_crime_events (view)
-- ============================================================

SELECT DISTINCT ON (id)
    id::bigint                                          AS crime_id,
    case_number::varchar                                AS case_number,
    {{ try_cast('date', 'timestamp') }}                 AS occurred_at,
    primary_type::varchar                               AS primary_type,
    description::varchar                                AS description,
    location_description::varchar                       AS location_description,
    arrest::boolean                                     AS arrest,
    domestic::boolean                                   AS domestic,
    {{ try_cast('community_area', 'int') }}             AS community_area_id,
    {{ try_cast('district', 'int') }}                   AS district_id,
    {{ try_cast('ward', 'int') }}                       AS ward_id,
    latitude::double precision                          AS latitude,
    longitude::double precision                         AS longitude,
    year::int                                           AS year,
    {{ try_cast('updated_on', 'timestamp') }}           AS updated_at
FROM {{ source('raw', 'crime_events') }}
ORDER BY id, updated_at DESC  -- keep the most recently updated row per id
