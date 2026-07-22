-- ============================================================
-- crime_ridership_model_evaluation — BQML evaluation metrics (Phase 4.8)
-- ============================================================
-- Runs ML.EVALUATE without a data argument — uses the auto-split
-- validation set (20% of training rows held out by BigQuery ML).
-- This is in-sample validation: all stations in the validation set
-- were seen during training, so station fixed effects apply.
--
-- Returns: r2_score, mean_absolute_error, mean_squared_error,
-- mean_squared_log_error, median_absolute_error, explained_variance.
--
-- For out-of-sample predictions on 2024+ data (including stations that
-- didn't exist in the training period), see
-- crime_ridership_predictions.
--
-- Depends on: crime_ridership_model_training_data (which trains the
-- BQML model via post_hook before this model runs).
--
-- Source: {{ ref('crime_ridership_model_training_data') }} (dependency only)
-- Output: mart.crime_ridership_model_evaluation (table)
-- ============================================================

{{
    config(
        materialized='table'
    )
}}

SELECT * FROM ML.EVALUATE(
    MODEL `{{ target.project }}.mart.crime_ridership_model`
)
