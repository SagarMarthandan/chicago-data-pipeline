# Phase 3.4 — Verification

**Date:** 2026-07-20
**Phase:** 3.4 (Verification)
**Status:** COMPLETE — Phase 3 gate met

## What Was Done

Broke the pipeline in 3 ways and confirmed all observability mechanisms catch the failures. No new permanent code — verification phase only. Pipeline restored to working state after each test.

### Scenario 1: Stream freshness alert
- **Break:** Producer stopped (divvy_stream DAG completed, no new data flowing)
- **Observability:** Grafana "Stream freshness" panel (id 6) — threshold red at 900s (15min)
- **Result:** Freshness = 1195s (19.9min) > 900s → panel RED ✅

### Scenario 2: DBT test failure
- **Break:** Injected bad crime row into `raw.crime_events` (id=99999999, lat=45.0, lon=-100.0 — South Dakota, outside Chicago bounds)
- **Observability:** DBT bounds tests in `staging/schema.yml` (latitude + longitude range checks) + Grafana "DBT test outcomes" panel (id 8)
- **Result:** 2 tests failed (`expect_column_values_to_be_between` for latitude + longitude), recorder captured fail=2, Grafana panel showed passing=30 failing=2 → RED ✅
- **Restore:** Deleted bad row, re-ran `dbt build` (PASS=60), ran recorder → Grafana back to passing=52 failing=0 → GREEN

### Scenario 3: Task failure + retries + callback
- **Break:** Throwaway DAG `verify_failure_handling` with `fail_on_purpose` task (`exit 1`), retries=3, retry_delay=10s, on_failure_callback
- **Observability:** Airflow retries (4 attempts) + on_failure_callback (structured log) + Grafana "Failed tasks" panel (id 11)
- **Result:**
  - Task failed after 4 attempts (try_number=4 = 1 initial + 3 retries) ✅
  - `on_failure_callback` fired on final failure, logged: `dag=verify_failure_handling task=fail_on_purpose run=manual__... try=4 exception=None` ✅
  - Grafana "Failed tasks" panel showed failed_tasks=2 → RED ✅
- **Restore:** Deleted DAG file, ran `airflow dags delete verify_failure_handling` (removed 5 metadata records)

## Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `dbt build` manual run: image `chicago-crime-dbt:latest` not found | Wrong image name. DAGs use `chicago-data-pipeline-dbt:latest`. | Used correct name from `DBT_IMAGE` var. |
| 2 | `dbt build` manual run: `--project-dir /opt/dbt` does not exist | Wrong path. DAGs use `/opt/airflow/dbt`. | Used correct path from `DBT_DIR` var. |
| 3 | Throwaway DAG not found by `airflow dags trigger` | DAG bundle refresh interval is long (~30s+). | Ran `airflow dags reserialize` to force refresh. |
| 4 | `airflow dags delete` failed with `EOFError` | Delete prompts for `y/n`, no TTY in `docker compose exec -T`. | Piped `echo "y"` into the command. |

## Key Decisions
- **Panel thresholds are sufficient alerts for local dev** — Grafana unified alerting (contact points, policies, rules) is overkill for a learning project. Panel turning red IS the alert.
- **Throwaway DAGs are the right way to test failure handling** — temporary `exit 1` DAG tests retries + callbacks without touching production DAGs.
- **Manual `dbt build` preserves test data** — triggering crime_batch DAG would re-run spark_crime_batch and overwrite the bad row. Manual dbt build (same image/paths) preserves it.

## Phase 3 Gate — MET ✅
- Grafana shows live row counts and stream freshness ✅ (3.1)
- Breaking the pipeline (stop producer) shows as Grafana alert within minutes ✅ (Scenario 1)
- DBT tests catch a deliberately introduced data quality issue ✅ (Scenario 2)
- Airflow retries a deliberately failing task and alerts on SLA miss ✅ (Scenario 3 — used execution_timeout + failed-tasks panel since Airflow 3.0 removed SLA)
