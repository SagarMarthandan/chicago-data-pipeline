# Phase 3.2 — DBT Tests

**Date:** 2026-07-20
**Phase:** 3.2 (DBT Tests)
**Status:** COMPLETE, verified, committed by user

## What Was Done

### DBT Singular Test
- Created `dbt/tests/assert_crime_in_chicago_bounds.sql` — singular test that flags crime events with lat/lon outside Chicago's bounding box (lat 41.64–42.03, lon -87.95–-87.52)
- Complements per-column range tests in `schema.yml` with a combined readable check
- Rows with NULL lat/lon excluded (~0.8% of events) — missing location is an accepted-null contract, not a bounds violation

### DBT Test Results Recorder
- Created `airflow/scripts/record_dbt_results.py` — parses `dbt/target/run_results.json` after `dbt build`, upserts one row per test into `observability.dbt_test_results` (new schema, created idempotently)
- **Key fix:** dbt 1.11 has no `resource_type` field (None for all entries); tests identified by `unique_id.startswith("test.")`, name extracted from `unique_id` (format: `test.chicago_crime.<name>.<hash>`)
- Added `./airflow/scripts:/opt/airflow/scripts` mount to `docker-compose.yml` `x-airflow-common` anchor
- Added `record_dbt_results` BashOperator to both DAGs after `dbt_build`

### Grafana DBT Panel
- Rewired Grafana panel id 8 from static `SELECT 59` to real query against `observability.dbt_test_results`
- Query returns passing/failing/warnings counts for the latest invocation
- Field overrides: Passing=green, Failing=red (≥1), Warnings=neutral
- Retitled "DBT test outcomes (latest run)"

## Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Recorder captured 0 tests despite `dbt build` reporting `TOTAL=60` | Filtered on `r.get("resource_type") == "test"`, but dbt 1.11's `run_results.json` does NOT populate `resource_type` (None for every entry). `name` field also None. | Changed filter to `r.get("unique_id", "").startswith("test.")`. Extracted name from `unique_id` by stripping `test.chicago_crime.` prefix and trailing `.<hash>` suffix. |
| 2 | Grafana dashboard JSON malformed after incremental edits to DBT panel | Multiple `edit` ops dropped `fieldConfig` wrapper + `matcher` opener, leaving defaults/overrides at wrong nesting level. | Re-inserted missing wrappers; validated with `json.load`. Lesson: edit JSON panel objects wholesale, not field-by-field. |

## Verification
- `dbt build` PASS=60 (1 seed + 7 models + 52 tests)
- `record_dbt_results` task succeeded in both DAGs
- `observability.dbt_test_results` populated: 52 tests, all status='pass'
- Singular bounds test `assert_crime_in_chicago_bounds` ran and passed
- Grafana panel query via `/api/ds/query` returns: passing=52, failing=0, warnings=0
- Dashboard loads with updated panel title "DBT test outcomes (latest run)"

## Key Decisions
- **Custom recorder vs dbt-artifacts package:** 40-line script, no new dbt dependency, project keeps `packages.yml` small
- **`observability` schema:** Dedicated schema for pipeline metadata, separate from `raw`/`staging`/`mart`, created idempotently by recorder script
- **dbt 1.11 `run_results.json`:** No `resource_type` field; identify tests by `unique_id` prefix `test.`
