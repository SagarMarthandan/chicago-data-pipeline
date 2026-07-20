# Phase 3.3 — Airflow Robustness

**Date:** 2026-07-20
**Phase:** 3.3 (Airflow Robustness)
**Status:** COMPLETE, verified

## What Was Done

### SqlSensor — Race Condition Fix
- Added `SqlSensor` (`wait_for_stream_data`) to `crime_batch_dag.py` — gates `dbt_build` on `raw.station_status` existing
- Uses `SELECT to_regclass('raw.station_status')` — returns OID if table exists, NULL if not
- `mode="reschedule"`, `poke_interval=60s`, `timeout=1hr`
- Fixes the race condition where `dim_date` (spans both crime + station sources) causes `dbt build` to fail if `divvy_stream` hasn't run

### on_failure_callback
- Created `airflow/dags/callbacks.py` — shared `on_failure_callback` that logs structured failure context (dag_id, task_id, run_id, try_number, exception)
- Wired into both DAGs via `default_args["on_failure_callback"]`

### Retries + execution_timeout
- Updated `default_args` in both DAGs: `retries=3`, `retry_delay=timedelta(minutes=5)`, `on_failure_callback`
- Added `execution_timeout=timedelta(minutes=30)` to `dbt_build` in both DAGs
- Set `retries=0` on cleanup tasks (`stop_stream`, `stop_producer`) in `divvy_stream_dag.py`

### Infrastructure
- Added `AIRFLOW_CONN_POSTGRES_DEFAULT` env var to `docker-compose.yml` `x-airflow-common` anchor — SqlSensor needs a Postgres connection
- Added "Failed tasks (last 7 days)" panel (id 11) to `pipeline_health.json` — queries `task_instance` for failed/upstream_failed states

## Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | SqlSensor failed: `'str' object has no attribute 'fetchone'` | Used `success=lambda result: result.fetchone()[0]` — assumed callback receives a cursor. Airflow 3.0's `SqlSensor.poke` passes `records[0]` (a row tuple) to the success callable. | Changed to `success=lambda row: row[0] is not None`. |
| 2 | `sla=` triggers deprecation warning, is a no-op | Airflow 3.0 removed the SLA feature entirely. `sla=` is accepted but does nothing. | Replaced with `execution_timeout=timedelta(minutes=30)`. Changed Grafana panel from "SLA misses" to "Failed tasks". |
| 3 | Stuck DAG run blocked new runs | Failed sensor task was `up_for_retry` (3 retries × 5min). DAG run stayed `running`, blocking new runs (`max_active_runs=1`). | Manually marked stuck run as `failed` in metadata DB. |

## Verification
- Both DAGs parse successfully
- `postgres_default` connection created via env var
- `divvy_stream` DAG run: all 8 tasks succeeded
- `crime_batch` DAG run: all 6 tasks succeeded (SqlSensor passed immediately — raw.station_status exists)
- Grafana dashboard loads with 11 panels, failed tasks panel returns data

## Key Decisions
- **Race condition fix via SqlSensor (not dbt selectors):** `dim_date` spans both sources, making the dependency real. The sensor makes it EXPLICIT rather than removing it.
- **`execution_timeout` vs `sla=`:** Airflow 3.0 removed SLA — `sla=` is a no-op. `execution_timeout` actually fails the task on timeout.
- **`retries=0` on cleanup tasks:** `stop_stream` and `stop_producer` don't benefit from retries.
- **`mode="reschedule"` on sensor:** Releases worker slot between pokes (may wait up to 1hr).
