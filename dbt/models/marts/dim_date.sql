-- ============================================================
-- dim_date — date dimension spanning all fact tables
-- ============================================================
-- Generates a row for every date between the earliest and latest
-- dates across ALL fact sources (crime events + station reads).
-- This ensures date_key FKs from any fact table resolve here.
--
-- Sources: {{ ref('stg_crime_events') }} + {{ ref('stg_station_status') }}
-- Output: mart.dim_date (table)
-- ============================================================

WITH date_range AS (
    SELECT
        MIN(occurred_at::date) AS min_date,
        MAX(occurred_at::date) AS max_date
    FROM {{ ref('stg_crime_events') }}

    UNION ALL

    SELECT
        MIN(reported_at::date) AS min_date,
        MAX(reported_at::date) AS max_date
    FROM {{ ref('stg_station_status') }}
),

date_bounds AS (
    SELECT MIN(min_date) AS min_date, MAX(max_date) AS max_date
    FROM date_range
),

date_series AS (
    SELECT generate_series(min_date, max_date, interval '1 day')::date AS date_key
    FROM date_bounds
)

SELECT
    date_key,
    EXTRACT(year FROM date_key)::int       AS year,
    EXTRACT(month FROM date_key)::int      AS month,
    EXTRACT(day FROM date_key)::int        AS day,
    EXTRACT(dow FROM date_key)::int        AS day_of_week,
    TO_CHAR(date_key, 'Day')               AS day_name,
    TO_CHAR(date_key, 'Month')             AS month_name,
    DATE_TRUNC('month', date_key)::date    AS month_start,
    DATE_TRUNC('quarter', date_key)::date  AS quarter_start
FROM date_series
