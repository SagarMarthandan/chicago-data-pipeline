# Session 05 — Phase 1.6 Verification

**Date:** 2026-07-13
**Session type:** Phase gate verification
**Phase:** 1.6 (Phase 1 gate)

## What Was Done

### 1. Cold Start Verification

Brought down all services with `docker compose down` (volumes preserved), then `docker compose up -d`. All services came up healthy.

### 2. DBT Views Blocking Spark Overwrite

First DAG run failed on `spark_crime_batch` with:
```
ERROR: cannot drop table raw.crime_events because other objects depend on it
```

**Root cause:** Spark's `mode("overwrite")` does `DROP TABLE raw.crime_events`, but DBT's `staging.stg_crime_events` view depends on it. On re-runs with preserved volumes, the views survive and block the drop. The first DAG run (Phase 1.5) worked because no views existed yet.

**Fix:** Added `clear_dbt_schemas` task to the DAG — drops `staging` and `mart` schemas (CASCADE) before Spark runs. DBT rebuilds them all in the next task. Idempotent (no-op on first run when schemas don't exist).

### 3. Airflow 3.0 Separate Dag-Processor

After adding the new task, the scheduler couldn't serialize the updated DAG. The serialized_dag table was empty and the scheduler logged:
```
DAG 'crime_batch' not found in serialized_dag table
```

**Root cause:** Airflow 3.0 split DAG processing into a separate `dag-processor` component. Unlike Airflow 2.x where the scheduler parsed DAGs inline, Airflow 3.0 requires a separate `airflow dag-processor` process. Without it, DAGs are never parsed/serialized.

The previous Phase 1.5 runs worked because the serialized_dag table already had entries (created by... something during initial setup). When I deleted the entry to force a re-parse, there was no dag-processor to recreate it.

**Fix:** Added `airflow-dag-processor` service to docker-compose.yml:
```yaml
airflow-dag-processor:
  <<: *airflow-common
  restart: unless-stopped
  depends_on:
    airflow-init:
      condition: service_completed_successfully
  command: dag-processor
```

### 4. End-to-End DAG Run Success

After full restart with dag-processor, triggered DAG run `manual__2026-07-13T14:11:11...9MkcEDt7`:

| Task | State | Duration |
|---|---|---|
| download_crime | success | 119s |
| clear_dbt_schemas | success | 0.3s |
| spark_crime_batch | success | 34s |
| dbt_build | success | 9s |
| **DagRun** | **success** | **163s** |

### 5. Marts Verified

| Table | Rows |
|---|---|
| dim_date | 365 |
| dim_community_area | 77 |
| dim_crime_type | 323 |
| fact_crime_events | 263,394 |
| raw.crime_events | 263,394 |

Fact matches raw — no data loss.

## Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `cannot drop table raw.crime_events because other objects depend on it` | DBT views from prior run block Spark's DROP TABLE in overwrite mode | Added `clear_dbt_schemas` task: DROP SCHEMA staging/mart CASCADE before Spark |
| 2 | `DAG 'crime_batch' not found in serialized_dag table` | Airflow 3.0 requires separate dag-processor service for DAG serialization | Added `airflow-dag-processor` service to docker-compose.yml |

### Operational Mistakes (AI Assistant)

| # | Mistake | Impact | Lesson |
|---|---|---|---|
| 1 | Deleted working serialized_dag entry to "force" re-parse | Broke DAG for 20+ minutes | Never delete working state. Diagnose first, touch second. |
| 2 | 4+ unnecessary docker compose down/up cycles | ~4 min wasted | Repeated restarts without new info is thrashing. |
| 3 | Manual Python scripts to mutate Airflow internal tables | Failed — wrong API, wrong format | Never manually mutate managed metadata tables. |

## Lessons

- **DBT views block Spark overwrite:** When Spark uses `mode("overwrite")` on a table with dependent views, Postgres blocks the drop. Drop dependent schemas first.
- **Airflow 3.0 dag-processor is mandatory:** Unlike 2.x, the scheduler no longer parses DAGs. A separate `airflow dag-processor` process is required. This is a breaking change — existing docker-compose setups migrating from 2.x need the new service.

## Files Changed

- `airflow/dags/crime_batch_dag.py` — added `clear_dbt_schemas` task (4-task pipeline)
- `docker-compose.yml` — added `airflow-dag-processor` service (7 services total)
- `docs/phases/phase-1.6-verification.md` — created phase doc
- `chat-history/current-state.md` — updated for Phase 1 completion
- `changelog.md` — added 2 new errors + lessons
- `docs/operations-performed.md` — added Phase 1.6 section

## Phase Gate Result

**Phase 1: COMPLETE.** Cold start → DAG run → 4 tasks succeed → marts queryable. Phase 2 (Streaming) unlocked.
