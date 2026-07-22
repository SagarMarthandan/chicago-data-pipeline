-- ============================================================
-- Singular test: crime events must fall within Chicago's city limits
-- ============================================================
-- A dbt singular test is any SELECT that returns "bad" rows. dbt fails
-- the test if the query returns one or more rows. Here, a bad row is a
-- crime event whose lat/long is populated but falls OUTSIDE Chicago's
-- bounding box — a sign of upstream geocoding corruption in Spark or
-- the Socrata source.
--
-- Bounds (lat 41.64–42.03, lon -87.95–-87.52) match the range checks
-- declared on fact_crime_events.latitude/longitude in marts/schema.yml.
-- This singular test exists alongside those column tests because a
-- combined lat/long check is clearer to read in one place than two
-- independent column-range tests, and it is the example custom test
-- called out in docs/chicago-pipeline-plan.md (Phase 3.2).
--
-- Rows with NULL lat/long are excluded (~0.8% of events) — missing
-- location is already covered by an accepted-null contract, not a
-- bounds violation.
-- ============================================================

SELECT
    crime_id,
    latitude,
    longitude,
    occurred_at
FROM {{ ref('fact_crime_events') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND (latitude NOT BETWEEN 41.64 AND 42.03
       OR longitude NOT BETWEEN -87.95 AND -87.52)
