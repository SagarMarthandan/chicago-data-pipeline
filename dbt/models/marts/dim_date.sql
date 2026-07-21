-- ============================================================
-- dim_date — date dimension (Phase 4.3 — crime dates only)
-- ============================================================
-- Generates a row for every date between the earliest and latest
-- crime dates. Previously (Phase 1-3) this spanned BOTH crime + station
-- sources, but station_status stays on local Postgres in Phase 4.
-- When Divvy trip history is ingested in Phase 4.6, this will span
-- crime + Divvy trip dates.
--
-- Source: {{ ref('stg_crime_events') }}
-- Output: mart.dim_date (table)
--
-- BigQuery notes:
--   - No generate_series() (Postgres). Use GENERATE_DATE_ARRAY() + UNNEST.
--   - EXTRACT works the same. FORMAT_TIMESTAMP replaces TO_CHAR.
-- ============================================================

WITH date_bounds AS (
    SELECT
        MIN(occurred_at) AS min_ts,
        MAX(occurred_at) AS max_ts
    FROM {{ ref('stg_crime_events') }}
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
