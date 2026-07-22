# BigQuery ML Reference

## What is BigQuery ML?

BigQuery ML (BQML) lets you train and run machine learning models using SQL — no Python, no separate ML infrastructure. The models live inside BigQuery alongside your data, so you avoid exporting data to a separate ML platform.

For a data engineer, BQML fits the "analytics engineering" story: the same person who builds dbt marts can also train a regression model, because it's all SQL.

## Core Syntax

### CREATE MODEL

```sql
CREATE OR REPLACE MODEL `project.dataset.model_name`
OPTIONS(
  model_type='linear_reg',
  input_label_cols=['target_column'],
  data_split_method='auto_split'  -- default: 80/20 random split
) AS
SELECT
  target_column,
  feature_1,
  feature_2
FROM `project.dataset.source_table`
WHERE date < '2024-01-01';
```

### ML.EVALUATE

```sql
-- Without data argument: evaluates on the auto-split validation set
SELECT * FROM ML.EVALUATE(MODEL `project.dataset.model_name`);

-- With data argument: evaluates on custom test data
SELECT * FROM ML.EVALUATE(
  MODEL `project.dataset.model_name`,
  (SELECT ... FROM test_table)
);
```

Returns: `r2_score`, `mean_absolute_error`, `mean_squared_error`, `mean_squared_log_error`, `median_absolute_error`, `explained_variance`.

### ML.WEIGHTS

```sql
SELECT * FROM ML.WEIGHTS(MODEL `project.dataset.model_name`);
```

Returns one row per feature:
- `processed_input` — feature name
- `weight` — regression coefficient (NULL for categorical features)
- `category_weights` — JSON array of per-category weights (for categorical features only)

### ML.PREDICT

```sql
SELECT * FROM ML.PREDICT(
  MODEL `project.dataset.model_name`,
  (SELECT ... FROM new_data)
);
```

Returns input columns + `predicted_<label_column>`.

## DBT Integration

BQML models can't be created via dbt's standard `CREATE TABLE` materialization. Instead, use a **post_hook** on a training-data model:

```sql
{{
    config(
        materialized='table',
        post_hook=[
            """
            CREATE OR REPLACE MODEL `{{ target.project }}.mart.my_model`
            OPTIONS(
              model_type='linear_reg',
              input_label_cols=['target']
            ) AS
            SELECT target, feature_1, feature_2
            FROM `{{ target.project }}.mart.my_training_data`
            """
        ]
    )
}}

SELECT target, feature_1, feature_2
FROM {{ ref('source_table') }}
WHERE date < '2024-01-01'
```

The post_hook runs after the training data table is built. Subsequent dbt models (evaluation, weights, predictions) reference the model via `ML.EVALUATE(MODEL ...)` etc.

### DBT dependency chain

```
training_data (table + post_hook trains BQML model)
    ├── model_evaluation (ML.EVALUATE)
    ├── model_weights (ML.WEIGHTS)
    └── model_predictions (ML.PREDICT)
```

All three result models `ref()` the training_data model — this ensures dbt builds them after the post_hook trains the BQML model.

## Key Concepts

| Concept | Description |
|---|---|
| `model_type` | Algorithm: `linear_reg`, `logistic_reg`, `kmeans`, `arima_plus`, `boosted_tree_regressor`, `dnn_regressor`, etc. |
| `input_label_cols` | Which column is the prediction target |
| `data_split_method` | How training data is split: `auto_split` (80/20 random), `no_split` (all training), `seq` (temporal split on `data_split_col`) |
| `auto_split` | Default. BigQuery holds out 20% for validation. `ML.EVALUATE` without arguments uses this held-out set |
| `no_split` | All training rows used for fitting. `ML.EVALUATE` without arguments fails (no validation set) |
| Categorical features | BigQuery ML one-hot encodes them automatically. Per-category weights appear in `category_weights` (JSON), not `weight` |
| `__INTERCEPT__` | The bias term in `ML.WEIGHTS` output |

## Gotchas

### 1. `weight` is NULL for categorical features

`ML.WEIGHTS` returns `weight = NULL` for categorical features. The per-category coefficients are in `category_weights` as a JSON array: `[{"category":"123","weight":"-15646.1"}, ...]`. Don't put a `not_null` test on the `weight` column.

### 2. High-cardinality categoricals + unseen test categories = catastrophic predictions

If you use a high-cardinality categorical (like `station_id` with 1,900+ values) as a feature, the model learns a fixed effect per category. When you predict on data with categories NOT seen during training, BigQuery ML falls back to the intercept only — no category weight is subtracted. If the intercept is large (e.g. 16,000 trips) and actual values are small (e.g. 10-50 trips), predictions will be wildly off.

**Symptom:** In-sample R² is reasonable (e.g. 0.43), but out-of-sample R² on new data is catastrophically negative (e.g. -199,000).

**Fix options:**
- Use `data_split_method='auto_split'` and evaluate with `ML.EVALUATE` (no data argument) — uses in-sample validation where all categories are known
- Drop high-cardinality categoricals and use aggregate features instead (e.g. station's historical average ridership)
- Use a model type that handles unseen categories better (boosted trees, DNN)

### 3. `data_split_method='no_split'` + `ML.EVALUATE` without data = error

If you train with `no_split`, there's no held-out validation set. `ML.EVALUATE(MODEL ...)` without a data argument will fail. You must either pass test data to `ML.EVALUATE` or use `auto_split` (default).

### 4. `CREATE MODEL` is not `CREATE TABLE`

BQML models are not tables — they're a separate BigQuery resource type. You can't `SELECT * FROM model_name`. You must use `ML.PREDICT`, `ML.EVALUATE`, `ML.WEIGHTS` etc. to query them.

### 5. `{{ target.project }}` in post_hooks

Use `{{ target.project }}` in post_hook SQL to reference the BigQuery project dynamically. This keeps the model portable across dev/prod projects.

## Our Usage (Phase 4.8)

- **Model:** `mart.crime_ridership_model` (linear_reg)
- **Label:** `trip_count` (trips starting at a station per day)
- **Features:** `crime_count_within_quarter_mile` (numeric), `day_of_week` (categorical), `month` (categorical), `station_id` (categorical — fixed effect)
- **Training data:** 815K rows (2020-04 to 2023-12, 1,915 stations)
- **Test data:** 648K rows (2024-01 to 2026-06, 3,834 stations — 1,919 new)
- **data_split_method:** `auto_split` (default — 20% held out for in-sample validation)
- **In-sample R²:** 0.434 (MAE = 13.4 trips)
- **Seen-station out-of-sample R²:** 0.447 (temporal generalization works for known stations)
- **Full out-of-sample R²:** -199K (50% of test rows are unseen stations — fixed effect has no learned weight)
- **Crime coefficient:** +1.45 (each additional crime predicts 1.45 more trips, even after controlling for station/day/month)

### Key Finding

The positive crime coefficient (+1.45) confirms the Phase 4.4 correlation finding (+0.20): the relationship is positive, not negative. Crime doesn't reduce ridership. Both are higher in busy areas — the confounding variable is urban activity level. The regression controls for station identity (fixed effect), day of week, and month, but the crime coefficient remains positive.

### Files

| File | Purpose |
|---|---|
| `dbt/models/marts/crime_ridership_model_training_data.sql` | Training data table + post_hook that creates the BQML model |
| `dbt/models/marts/crime_ridership_model_evaluation.sql` | `ML.EVALUATE` on auto-split validation set |
| `dbt/models/marts/crime_ridership_model_weights.sql` | `ML.WEIGHTS` — feature coefficients |
| `dbt/models/marts/crime_ridership_predictions.sql` | `ML.PREDICT` on 2024+ out-of-sample data |

---

**← Previous:** [dlt](dlt.md) | **Next:** [architecture](architecture.md) →
