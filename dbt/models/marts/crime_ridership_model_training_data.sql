-- ============================================================
-- crime_ridership_model_training_data — BQML training set (Phase 4.8)
-- ============================================================
-- Prepares the feature matrix + label for BigQuery ML linear
-- regression. The post_hook trains the model after this table
-- is built.
--
-- Model question: after controlling for station identity (fixed
-- effect), day of week, and month, does crime count near a station
-- predict ridership?
--
-- Features:
--   crime_count_within_quarter_mile — numeric (the key predictor)
--   day_of_week                     — categorical 1-7 (Sunday=1)
--   month                           — categorical 1-12 (seasonality)
--   station_id                      — categorical (station fixed effect,
--                                      controls for baseline ridership)
-- Label:
--   trip_count                      — numeric (trips starting at station)
--
-- Train/test split: temporal for data selection (2020-04 to 2023-12 trains,
-- 2024+ used for out-of-sample predictions). The BQML model uses
-- data_split_method='auto_split' (default) — BigQuery holds out 20% of
-- training rows for in-sample validation (ML.EVALUATE without arguments).
-- The 2024+ predictions (crime_ridership_predictions) are out-of-sample:
-- many stations opened after 2023, so the model has no learned weights for
-- them — predictions rely on the intercept + temporal features only.
--
-- Source: {{ ref('fact_station_day') }}
-- Output: mart.crime_ridership_model_training_data (table)
--         + mart.crime_ridership_model (BQML model, via post_hook)
-- ============================================================

{{
    config(
        materialized='table',
        post_hook=[
            """
            CREATE OR REPLACE MODEL `{{ target.project }}.mart.crime_ridership_model`
            OPTIONS(
              model_type='linear_reg',
              input_label_cols=['trip_count']
            ) AS
            SELECT
              trip_count,
              crime_count_within_quarter_mile,
              day_of_week,
              month,
              station_id
            FROM `{{ target.project }}.mart.crime_ridership_model_training_data`
            """
        ]
    )
}}

SELECT
    trip_count,
    crime_count_within_quarter_mile,
    EXTRACT(DAYOFWEEK FROM date_key) AS day_of_week,
    EXTRACT(MONTH FROM date_key) AS month,
    station_id
FROM {{ ref('fact_station_day') }}
WHERE date_key < '2024-01-01'
