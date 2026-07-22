-- ============================================================
-- crime_ridership_predictions — BQML out-of-sample predictions (Phase 4.8)
-- ============================================================
-- Runs ML.PREDICT on the 2024+ test data. Each row gets a
-- predicted_trip_count alongside the actual trip_count, so we can
-- compare model predictions vs reality.
--
-- date_key is included as a passthrough column (not a feature) so
-- predictions can be joined back to the station-day grain.
--
-- Depends on: crime_ridership_model_training_data (which trains the
-- BQML model via post_hook before this model runs).
--
-- Source: {{ ref('crime_ridership_model_training_data') }} (dependency only)
-- Output: mart.crime_ridership_predictions (table)
-- ============================================================

{{
    config(
        materialized='table'
    )
}}

SELECT
    station_id,
    date_key,
    trip_count,
    predicted_trip_count,
    crime_count_within_quarter_mile
FROM ML.PREDICT(
    MODEL `{{ target.project }}.mart.crime_ridership_model`,
    (
        SELECT
            trip_count,
            crime_count_within_quarter_mile,
            EXTRACT(DAYOFWEEK FROM date_key) AS day_of_week,
            EXTRACT(MONTH FROM date_key) AS month,
            station_id,
            date_key
        FROM {{ ref('fact_station_day') }}
        WHERE date_key >= '2024-01-01'
    )
)
