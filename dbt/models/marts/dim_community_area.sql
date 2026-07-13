-- ============================================================
-- dim_community_area — Chicago's 77 community areas
-- ============================================================
-- Loaded from seeds/community_areas.csv via `dbt seed`.
-- This is static reference data from the Chicago Data Portal
-- (Boundaries - Community Areas, resource ID: igwz-8jzy).
--
-- Source: {{ ref('community_areas') }} (seed)
-- Output: mart.dim_community_area (table)
-- ============================================================

SELECT
    community_area_id::int   AS community_area_id,
    community_area_name::varchar AS community_area_name
FROM {{ ref('community_areas') }}
