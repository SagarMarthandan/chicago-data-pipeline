-- ============================================================
-- crime_ridership_correlation — answers the driving question (Phase 4.4)
-- ============================================================
-- Computes CORR(trip_count, crime_count_within_quarter_mile) across
-- station-days, grouped by station, month, and overall.
--
-- A negative correlation would suggest crime reduces ridership.
-- A positive correlation might mean both are higher in busy areas.
-- A null/near-zero correlation means no linear relationship.
--
-- Source: {{ ref('fact_station_day') }}
-- Output: mart.crime_ridership_correlation (table)
-- ============================================================

{{
    config(
        materialized='table'
    )
}}

WITH station_day_stats AS (
    SELECT
        station_id,
        date_key,
        trip_count,
        crime_count_within_quarter_mile
    FROM {{ ref('fact_station_day') }}
),

-- Per-station correlation: does crime near THIS station affect its ridership?
per_station AS (
    SELECT
        station_id,
        CORR(trip_count, crime_count_within_quarter_mile) AS correlation_coefficient,
        COUNT(*) AS station_day_count,
        AVG(trip_count) AS avg_trips,
        AVG(crime_count_within_quarter_mile) AS avg_crimes
    FROM station_day_stats
    GROUP BY station_id
    HAVING COUNT(*) >= 30  -- need at least 30 days for meaningful correlation
),

-- Per-month correlation: seasonal effects on the crime-ridership relationship
per_month AS (
    SELECT
        DATE_TRUNC(date_key, MONTH) AS month,
        CORR(trip_count, crime_count_within_quarter_mile) AS correlation_coefficient,
        COUNT(*) AS station_day_count
    FROM station_day_stats
    GROUP BY 1
    HAVING COUNT(*) >= 30
),

-- Overall correlation across all station-days
overall AS (
    SELECT
        CORR(trip_count, crime_count_within_quarter_mile) AS correlation_coefficient,
        COUNT(*) AS station_day_count,
        AVG(trip_count) AS avg_trips,
        AVG(crime_count_within_quarter_mile) AS avg_crimes
    FROM station_day_stats
)

-- Union all three levels with a scope label
SELECT
    'overall' AS scope,
    NULL AS station_id,
    NULL AS month,
    correlation_coefficient,
    station_day_count AS observation_count,
    avg_trips,
    avg_crimes
FROM overall

UNION ALL

SELECT
    'per_station' AS scope,
    station_id,
    NULL AS month,
    correlation_coefficient,
    station_day_count AS observation_count,
    avg_trips,
    avg_crimes
FROM per_station

UNION ALL

SELECT
    'per_month' AS scope,
    NULL AS station_id,
    month,
    correlation_coefficient,
    station_day_count AS observation_count,
    NULL AS avg_trips,
    NULL AS avg_crimes
FROM per_month
