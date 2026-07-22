# DBT Conventions

## Project Structure

```
dbt/
├── dbt_project.yml
├── profiles.yml
├── macros/
│   └── try_cast.sql
├── models/
│   ├── staging/          ← stg_*.sql — 1:1 with raw sources, light cleaning
│   ├── intermediate/     ← int_*.sql — joins/aggregations between staging and marts
│   └── marts/            ← fact_*/dim_* — business-facing tables
├── tests/
│   └── *.sql             ← custom singular tests
└── seeds/
    └── *.csv             ← static reference data loaded via dbt seed
```

## Naming Conventions

| Layer | Prefix | Purpose | Materialized as |
|---|---|---|---|
| Staging | `stg_` | 1:1 with source, rename/cast/dedupe | view |
| Intermediate | `int_` | Multi-step joins, business logic | view |
| Marts — facts | `fact_` | Events/metrics (things that happen) | table |
| Marts — dims | `dim_` | Entities/context (things that exist) | table |
| Seeds | (none) | Static CSV reference data | table |

## The `try_cast` Macro

**Always use `{{ try_cast('column', 'type') }}` instead of `TRY_CAST()` or `SAFE_CAST()` directly.**

```sql
-- ✅ Correct
{{ try_cast('community_area', 'int') }} AS community_area_id

-- ❌ Wrong — TRY_CAST doesn't exist in Postgres
TRY_CAST(community_area AS int) AS community_area_id

-- ❌ Wrong — bypasses the macro, breaks BigQuery portability
community_area::int AS community_area_id  -- only if you KNOW it's clean
```

The macro (`macros/try_cast.sql`) dispatches per-warehouse:
- **Postgres:** plain `::` cast (fails loudly on bad data — that's intentional)
- **BigQuery:** `SAFE_CAST` (returns null on failure — BigQuery doesn't raise)

See `docs/wiki/conventions/dbt.md` design note in the plan for rationale.

## Date/Time Handling

**Never use `EXTRACT(date FROM ...)` — it's invalid Postgres.** `date` is not a valid EXTRACT field.

```sql
-- ✅ Correct — cast to date
occurred_at::date AS date_key

-- ❌ Wrong — will error on Postgres
EXTRACT(date FROM occurred_at) AS date_key
```

For truncation, `DATE_TRUNC` works on both Postgres and BigQuery:
```sql
DATE_TRUNC('month', occurred_at) AS month  -- returns timestamp
DATE_TRUNC('month', occurred_at::date) AS month  -- also works (date → timestamp implicit cast)
```

## Source Definitions

Define sources in `models/staging/schema.yml`:

```yaml
sources:
  - name: raw
    schema: raw
    tables:
      - name: crime_events
      - name: station_status
```

Reference them as `{{ source('raw', 'crime_events') }}` — never hardcode table names.

## Tests

### Standard tests (in `schema.yml`)
Every fact table MUST have:
- `unique` + `not_null` on the primary key
- `not_null` on all FK columns
- `relationships` on FKs to dimension tables

```yaml
models:
  - name: fact_crime_events
    columns:
      - name: crime_id
        tests: [unique, not_null]
      - name: community_area_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_community_area')
              field: community_area_id
```

### Custom singular tests (in `tests/`)
Use for business rules that standard tests can't express:

```sql
-- tests/assert_crime_in_chicago_bounds.sql
SELECT *
FROM {{ ref('fact_crime_events') }}
WHERE latitude IS NOT NULL
  AND (latitude NOT BETWEEN 41.6 AND 42.1
       OR longitude NOT BETWEEN -87.9 AND -87.5)
```

A passing test returns **zero rows**. Any row returned = test failure.

## Materializations

- **Staging:** `view` (cheap, always reflects latest raw data)
- **Intermediate:** `view` (same reasoning)
- **Facts/Dims:** `table` (query performance for dashboards)
- **Large facts (Phase 4+):** `incremental` with `unique_key` + `merge` strategy

## Idempotency

Every model must produce the same result when re-run. This means:
- No `CURRENT_TIMESTAMP` in transformations (use source data timestamps)
- No `ORDER BY` without a deterministic tiebreaker
- Incremental models must use `unique_key` to upsert, not append

## Running DBT

```bash
dbt run --select staging         # just staging
dbt run --select marts           # just marts
dbt run --select fact_crime_events  # one model
dbt run --select staging+        # staging and everything downstream
dbt test                         # all tests
dbt test --select fact_crime_events  # tests for one model
dbt seed                         # load seed files
dbt build                        # run seeds → models → tests (all in DAG order)
```

## Common Mistakes to Expect

1. **`TRY_CAST` error** → you forgot to use the `try_cast` macro (Postgres has no `TRY_CAST`)
2. **`EXTRACT(date ...)` error** → use `::date` cast instead
3. **Test fails on relationships** → FK has values not in the dimension (e.g., `community_area = 0` for unassigned). Handle in the model with a `WHERE` filter or a default dim row.
4. **Model runs but table is empty** → upstream source is empty or your `WHERE` filter is too aggressive
5. **Re-running duplicates rows** → you're using `table` materialization but the source has duplicates. Add deduplication in staging.
