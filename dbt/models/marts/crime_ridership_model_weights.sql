-- ============================================================
-- crime_ridership_model_weights — BQML feature weights (Phase 4.8)
-- ============================================================
-- Runs ML.WEIGHTS to extract the trained linear regression coefficients.
-- Key row: crime_count_within_quarter_mile — its weight tells us whether
-- crime predicts ridership after controlling for station, day, and month.
--
-- For categorical features (station_id, day_of_week, month), BigQuery ML
-- one-hot encodes them — one row per category value. The weight column
-- is the coefficient; standard_error gives the significance.
--
-- Depends on: crime_ridership_model_training_data (which trains the
-- BQML model via post_hook before this model runs).
--
-- Source: {{ ref('crime_ridership_model_training_data') }} (dependency only)
-- Output: mart.crime_ridership_model_weights (table)
-- ============================================================

{{
    config(
        materialized='table'
    )
}}

SELECT * FROM ML.WEIGHTS(
    MODEL `{{ target.project }}.mart.crime_ridership_model`
)
