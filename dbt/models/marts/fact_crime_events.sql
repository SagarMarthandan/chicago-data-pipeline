-- ============================================================
-- fact_crime_events — main crime fact table
-- ============================================================
-- One row per crime event, with foreign keys to dimensions.
-- Filters out rows with null crime_id (data quality).
--
-- Phase 4.4: Partitioned by date_key (daily) for partition pruning.
-- Clustered by community_area_id + primary_type — common filter/group-by
-- columns for crime analysis. BigQuery partitioning requires table (not view).
--
-- Source: {{ ref('stg_crime_events') }}
-- Output: mart.fact_crime_events (table, partitioned + clustered)
-- ============================================================

{{
    config(
        materialized='table',
        partition_by={"field": "date_key", "data_type": "date"},
        cluster_by=["community_area_id", "primary_type"]
    )
}}

SELECT
    c.crime_id,
    c.occurred_at,
    DATE(c.occurred_at) AS date_key,
    c.community_area_id,
    c.primary_type,
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
