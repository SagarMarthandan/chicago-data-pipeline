-- ============================================================
-- fact_crime_events — main crime fact table
-- ============================================================
-- One row per crime event, with foreign keys to dimensions.
-- Filters out rows with null crime_id (data quality).
--
-- Source: {{ ref('stg_crime_events') }}
-- Output: mart.fact_crime_events (table)
-- ============================================================

SELECT
    c.crime_id,
    c.occurred_at,
    DATE(c.occurred_at) AS date_key,
    c.community_area_id,
    c.primary_type || '|' || c.description AS crime_type_key,
    c.arrest,
    c.domestic,
    c.latitude,
    c.longitude,
    c.district_id,
    c.ward_id,
    c.year
FROM {{ ref('stg_crime_events') }} c
WHERE c.crime_id IS NOT NULL
