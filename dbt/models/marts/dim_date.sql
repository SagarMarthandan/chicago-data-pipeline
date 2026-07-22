-- ============================================================
-- dim_date — date dimension (Phase 4.4 — crime + Divvy trip dates)
-- ============================================================
-- Generates a row for every date between the earliest and latest
-- dates across both crime events and Divvy trips.
-- Phase 1-3: crime only. Phase 4.3: crime only (BigQuery migration).
-- Phase 4.4: crime (2018-present) + Divvy trips (2020-present).
--
-- Sources: {{ ref('stg_crime_events') }} + {{ ref('stg_divvy_trips') }}
-- Output: mart.dim_date (table)
--
-- BigQuery notes:
--   - No generate_series() (Postgres). Use GENERATE_DATE_ARRAY() + UNNEST.
--   - EXTRACT works the same. FORMAT_TIMESTAMP replaces TO_CHAR.
-- ============================================================

WITH crime_bounds AS (
    SELECT
        MIN(occurred_at) AS min_ts,
        MAX(occurred_at) AS max_ts
    FROM {{ ref('stg_crime_events') }}
),

divvy_bounds AS (
    SELECT
        MIN(started_at) AS min_ts,
        MAX(started_at) AS max_ts
    FROM {{ ref('stg_divvy_trips') }}
),

date_bounds AS (
    SELECT
        LEAST(c.min_ts, d.min_ts) AS min_ts,
        GREATEST(c.max_ts, d.max_ts) AS max_ts
    FROM crime_bounds c
    CROSS JOIN divvy_bounds d
),

date_series AS (
    SELECT date_key
    FROM date_bounds,
    UNNEST(GENERATE_DATE_ARRAY(
        DATE(min_ts),
        DATE(max_ts),
        INTERVAL 1 DAY
    )) AS date_key
)

SELECT
    date_key,
    EXTRACT(year FROM date_key)       AS year,
    EXTRACT(month FROM date_key)      AS month,
    EXTRACT(day FROM date_key)        AS day,
    EXTRACT(DAYOFWEEK FROM date_key)  AS day_of_week,
    FORMAT_TIMESTAMP('%A', TIMESTAMP(date_key))  AS day_name,
    FORMAT_TIMESTAMP('%B', TIMESTAMP(date_key))  AS month_name,
    DATE_TRUNC(date_key, MONTH)       AS month_start,
    DATE_TRUNC(date_key, QUARTER)     AS quarter_start
FROM date_series
