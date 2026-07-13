-- ============================================================
-- dim_crime_type — distinct primary_type + description combinations
-- ============================================================
-- Each row is a unique crime type. Surrogate key is generated
-- from the concatenation of primary_type and description.
--
-- Source: {{ ref('stg_crime_events') }}
-- Output: mart.dim_crime_type (table)
-- ============================================================

SELECT DISTINCT
    primary_type || '|' || description AS crime_type_key,
    primary_type,
    description
FROM {{ ref('stg_crime_events') }}
WHERE primary_type IS NOT NULL
  AND description IS NOT NULL
