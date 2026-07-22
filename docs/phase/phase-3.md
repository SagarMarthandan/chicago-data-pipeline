# Phase 3 — Observability (Grafana + DBT Tests + Airflow Robustness + Verification)

> **Status:** Complete / Verified on 2026-07-20
> **Phase gate:** Grafana shows live row counts and stream freshness; breaking the pipeline (stop producer) shows as a Grafana alert within minutes; DBT tests catch a deliberately introduced data quality issue; Airflow retries a deliberately failing task and alerts on the failure. Phase 3 COMPLETE — Phase 4 (Cloud) unlocked.

## Summary

Phase 3 wrapped the existing batch + streaming pipeline in a full observability layer and then proved it works by deliberately breaking the pipeline in three ways. The phase was built in four sub-phases over three days (2026-07-18 to 2026-07-20):

- **3.1 — Grafana (2026-07-18):** Added Grafana as the dashboarding layer. Two Postgres datasources (Chicago Analytics + Airflow Metadata) and two dashboards (Pipeline Health — 10 panels, Crime + Divvy Analysis — 6 panels) are provisioned file-based via YAML/JSON. All 16 panel queries verified against live data.
- **3.2 — DBT Tests (2026-07-20):** Added a custom singular DBT test for geographic bounds and a recorder that parses `run_results.json` after every `dbt build` and writes test outcomes into `observability.dbt_test_results`. The Grafana DBT panel was rewired from a static placeholder to live test outcomes.
- **3.3 — Airflow Robustness (2026-07-20):** Added a `SqlSensor` to `crime_batch` that fixes the race condition with `divvy_stream`, plus retries (3x, 5min delay), `on_failure_callback` (structured failure logging), and `execution_timeout` on `dbt_build` (30min). Added a "Failed tasks" panel to Grafana. Airflow 3.0 removed the SLA feature, so `execution_timeout` is used instead of `sla=`.
- **3.4 — Verification (2026-07-20):** Broke the pipeline in 3 ways and confirmed all observability mechanisms catch the failures: (1) stopped producer → Grafana stream freshness panel went red, (2) injected bad crime data → 2 DBT tests failed + Grafana DBT panel went red, (3) failed a task → Airflow retried 3x + on_failure_callback logged + Grafana failed-tasks panel went red. Pipeline restored to working state after each test.

## Files Created/Modified (Phase 3 Overall)

| File | Action | Sub-phase | Purpose |
|---|---|---|---|
| `docker-compose.yml` | Modified | 3.1 | Added `grafana` service (image, env, port 3000, volumes, healthcheck) + `grafana_data` named volume. Added `AIRFLOW_DB_USER`/`AIRFLOW_DB_PASSWORD` to grafana env. |
| `docker-compose.yml` | Modified | 3.2 | Added `./airflow/scripts:/opt/airflow/scripts` mount to `x-airflow-common` anchor. |
| `docker-compose.yml` | Modified | 3.3 | Added `AIRFLOW_CONN_POSTGRES_DEFAULT` env var to `x-airflow-common` for SqlSensor. |
| `.env.example` | Modified | 3.1 | Added `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` section. |
| `.env` | Modified | 3.1 | Appended Grafana admin creds. |
| `grafana/provisioning/datasources/postgres.yml` | Created | 3.1 | Provisions two Postgres datasources: `chicago-analytics` (warehouse) + `airflow-metadata` (Airflow DB). Uses shell-style `$VAR` env interpolation. |
| `grafana/provisioning/dashboards/dashboards.yml` | Created | 3.1 | Dashboard provider — scans `./grafana/dashboards/` every 30s, loads into "Chicago Pipeline" folder. |
| `grafana/dashboards/pipeline_health.json` | Created | 3.1 | 10-panel pipeline health dashboard (row counts, stream freshness, DBT tests, Airflow DAG runs + task instances). |
| `grafana/dashboards/pipeline_health.json` | Modified | 3.2 | Rewired DBT panel (id 8) from static placeholder to real query against `observability.dbt_test_results`. |
| `grafana/dashboards/pipeline_health.json` | Modified | 3.3 | Added "Failed tasks (last 7 days)" panel (id 11). |
| `grafana/dashboards/crime_divvy_analysis.json` | Created | 3.1 | 6-panel analysis dashboard (crime by area, crime types, station availability heatmap, crime-vs-ridership proxy). |
| `docs/wiki/grafana.md` | Created | 3.1 | Grafana reference: provisioning, env var syntax gotcha, dashboard inventory, useful commands. |
| `dbt/tests/assert_crime_in_chicago_bounds.sql` | Created | 3.2 | Singular test — flags crime events with lat/long outside Chicago's bounding box (lat 41.64–42.03, lon -87.95–-87.52). |
| `airflow/scripts/record_dbt_results.py` | Created | 3.2 | Parses `run_results.json`, upserts test outcomes into `observability.dbt_test_results`. Idempotent (keyed on invocation_id + test_name). |
| `airflow/dags/callbacks.py` | Created | 3.3 | Shared `on_failure_callback` — logs dag_id, task_id, run_id, try_number, exception to Airflow task logs. |
| `airflow/dags/crime_batch_dag.py` | Modified | 3.2 | Added `record_dbt_results` task after `dbt_build`. |
| `airflow/dags/crime_batch_dag.py` | Modified | 3.3 | Added SqlSensor, retries, on_failure_callback, execution_timeout. |
| `airflow/dags/divvy_stream_dag.py` | Modified | 3.2 | Added `record_dbt_results` task between `dbt_build` and `stop_stream`. |
| `airflow/dags/divvy_stream_dag.py` | Modified | 3.3 | Added retries, on_failure_callback, execution_timeout, retries=0 on cleanup. |

---

## 3.1 — Grafana (2026-07-18)

### What Was Built

Added Grafana as the observability dashboarding layer. Two Postgres datasources (Chicago Analytics + Airflow Metadata) are provisioned file-based via YAML, and two dashboards (Pipeline Health + Crime + Divvy Analysis) are provisioned via JSON. All 16 panel queries verified against live data — 263,395 crime rows, 2,001 station reads, Airflow DAG run history.

### Files Created/Modified

| File | Action | Purpose |
|---|---|---|
| `docker-compose.yml` | Modified | Added `grafana` service (image `grafana/grafana:12.4.0`, port 3000, `grafana_data` named volume, healthcheck on `/api/health`, anonymous Viewer access, env vars for Postgres + Airflow DB creds). Added `grafana_data` to volumes section. |
| `.env.example` | Modified | Added `GRAFANA_ADMIN_USER=admin` + `GRAFANA_ADMIN_PASSWORD=admin` section. |
| `.env` | Modified | Appended Grafana admin creds. |
| `grafana/provisioning/datasources/postgres.yml` | Created | Provisions two Postgres datasources: `chicago-analytics` (uid: `chicago-analytics`, database: `chicago_analytics`) + `airflow-metadata` (uid: `airflow-metadata`, database: `airflow_metadata`). Uses shell-style `$VAR` env interpolation (NOT Go templates). Includes `database:` inside `jsonData:` block for both datasources (fix for browser rendering). |
| `grafana/provisioning/dashboards/dashboards.yml` | Created | Dashboard provider scanning `/var/lib/grafana/dashboards/` every 30s, loading into "Chicago Pipeline" folder. |
| `grafana/dashboards/pipeline_health.json` | Created | 10-panel pipeline health dashboard (uid: `pipeline-health`): 4 stat panels (row counts for raw.crime_events, mart.fact_crime_events, raw.station_status, mart.fact_station_reads), 1 time series (stream ingestion rate rows/hour), 2 stat panels (stream freshness seconds since last ingest, latest Kafka message timestamp), 1 stat panel (DBT tests passing — static placeholder wired in Phase 3.2), 1 pie chart (Airflow DAG runs last 7 days from airflow-metadata datasource), 1 table (recent Airflow task instances last 2 days from airflow-metadata datasource). |
| `grafana/dashboards/crime_divvy_analysis.json` | Created | 6-panel analysis dashboard (uid: `crime-divvy-analysis`): 1 bar chart (top 15 community areas by crime count), 1 pie chart (top 10 crime types), 1 time series (avg vehicles available per station hourly), 1 heatmap (station availability top 20 stations × hour of day), 1 time series (crime vs Divvy ridership proxy monthly — THE DRIVING QUESTION), 1 heatmap (crime by day of week × hour of day). |
| `docs/wiki/grafana.md` | Created | Comprehensive Grafana reference: core concepts (datasource, dashboard, panel, query, provisioning), our setup, file-based provisioning, env var interpolation gotcha, `jsonData.database` deep dive (browser vs API code paths), two-datasource pattern, dashboard inventory, DAG run order (stream first), useful commands, 10 common mistakes, 8 mermaid diagrams. |

### Architecture — What Was Built

```mermaid
graph TB
    subgraph "Postgres Container"
        CA["chicago_analytics DB<br/>(raw + staging + mart schemas)"]
        AM["airflow_metadata DB<br/>(dag_run, task_instance)"]
    end

    subgraph "Grafana Container (port 3000)"
        DS1["Datasource: chicago-analytics<br/>uid: chicago-analytics"]
        DS2["Datasource: airflow-metadata<br/>uid: airflow-metadata"]
        PH["Dashboard: Pipeline Health<br/>10 panels"]
        CA2["Dashboard: Crime + Divvy Analysis<br/>6 panels"]
        PROV["Provisioning<br/>/etc/grafana/provisioning/"]
    end

    PROV --> DS1
    PROV --> DS2
    PROV --> PH
    PROV --> CA2
    DS1 --> CA
    DS2 --> AM
    PH --> DS1
    PH --> DS2
    CA2 --> DS1
```

Grafana reads from two separate Postgres databases via two provisioned datasources. Dashboards are JSON files bind-mounted into the container; provisioning YAML auto-loads them on startup.

**For detailed architecture diagrams**, see `docs/wiki/architecture.md`.

### Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `Failed to provision data sources: yaml: unmarshal errors: line 25: cannot unmarshal !!map into string` | Used Go template syntax `{{.POSTGRES_USER}}` in datasource YAML. Grafana provisioning uses shell-style `$VAR`, not Go templates. | Changed to `user: $POSTGRES_USER` and `password: $POSTGRES_PASSWORD`. |
| 2 | `FATAL: no PostgreSQL user name specified in startup packet (SQLSTATE 28000)` on Airflow datasource | `AIRFLOW_DB_USER`/`AIRFLOW_DB_PASSWORD` env vars not in Grafana container. `docker compose restart` doesn't re-read env vars. | Added vars to docker-compose grafana env, then `docker compose up -d grafana` (recreates container). |
| 3 | `relation "airflow_metadata.dag_run" does not exist` | Tried to cross-database query (`airflow_metadata.dag_run`) from `chicago_analytics` datasource. Postgres can't query across databases without `postgres_fdw`. | Added second datasource `airflow-metadata` pointing at `airflow_metadata` DB. Updated Airflow panels to use it; dropped `airflow_metadata.` schema prefix from SQL. |
| 4 | Browser console: `You do not currently have a default database configured for this data source` — panels show "No data" despite API queries working | Grafana 12.4's Postgres plugin reads the database name from `jsonData.database`, NOT the top-level `database:` field. The top-level field works for the internal API but the browser plugin uses a different code path that requires `jsonData.database`. | Added `database:` inside the `jsonData:` block of each datasource in `postgres.yml`. Recreated Grafana container. |

### Lessons

- **Grafana env var syntax is `$VAR`, not `{{.VAR}}`** — the cryptic "cannot unmarshal !!map into string" error is the signature of this mistake. Grafana's provisioning parser uses shell-style interpolation, not Go templates.
- **`docker compose restart` ≠ `docker compose up -d` for env changes** — restart reuses the existing container (with old env). `up -d` recreates the container when config changes. This is a common Docker Compose gotcha.
- **Postgres databases are isolated** — unlike schemas (which share a database and can be cross-queried), databases are fully separate. Cross-DB queries need `postgres_fdw` or a second datasource. Our project has two databases (`chicago_analytics` + `airflow_metadata`), so Grafana needs two datasources.
- **Postgres datasource needs `jsonData.database`** — Grafana 12.4's Postgres plugin reads the DB name from `jsonData.database`, not the top-level `database:` field. The top-level field works for the internal API but the browser plugin requires `jsonData.database`. Without it, API queries succeed but browser panels show "No data". Set both for compatibility.
- **Verify in the browser, not just curl** — the internal API (`/api/ds/query` with `datasourceId`) uses the top-level `database:` field and gives false positives. The browser plugin uses `jsonData.database`. Always verify dashboards render in the browser after provisioning changes.

### Decisions Made

| Decision | Choice | Why |
|---|---|---|
| Grafana version | 12.4.0 (not 13.1.0) | 13.1.0 released 2026-06-23 — 25 days old. 12.4.0 is production-hardened. Matches "stable versions only" rule (same logic as Airflow 3.0.0 over 3.3.0). |
| Image | `grafana/grafana` (not `grafana/grafana-oss`) | `grafana-oss` repo deprecated since 12.4.0. `grafana/grafana` includes Enterprise features, free to use. |
| Two datasources | One per Postgres database | Postgres can't cross-query databases without `postgres_fdw`. Simpler to provision two datasources than set up FDW. |
| File-based provisioning | YAML + JSON, no UI config | Version-controlled, reproducible, no manual UI steps. Dashboards rebuild from files on any machine. |
| Anonymous Viewer access | Enabled for local dev | Dashboards viewable without login. Set `GF_AUTH_ANONYMOUS_ENABLED=false` in shared environments. |
| DBT tests panel (static) | Placeholder `SELECT 59` | Real DBT test results need artifact parsing — wired in Phase 3.2. Panel exists now so the dashboard is complete. |
| Crime-vs-ridership is a proxy | System-wide avg bikes, not per-area | GBFS doesn't include `community_area_id` on stations. Real join needs geospatial matching (station lat/long → community area polygon), deferred to Phase 4+. |

### Verification

```bash
# Start Grafana + Postgres
$ docker compose up -d postgres grafana
Container chicago-data-pipeline-postgres-1 Healthy
Container chicago-data-pipeline-grafana-1 Started

# Grafana health
$ curl -s -u admin:admin http://localhost:3000/api/health
{"database": "ok", "version": "12.4.0"}

# Datasources provisioned
$ curl -s -u admin:admin http://localhost:3000/api/datasources | python3 -m json.tool
[{"name": "Chicago Analytics", "uid": "chicago-analytics", "type": "postgres"},
 {"name": "Airflow Metadata", "uid": "airflow-metadata", "type": "postgres"}]

# Dashboards loaded
$ curl -s -u admin:admin http://localhost:3000/api/search
[{"title": "Chicago Pipeline", "type": "dash-folder"},
 {"title": "Crime + Divvy Analysis", "type": "dash-db", "uid": "crime-divvy-analysis"},
 {"title": "Pipeline Health", "type": "dash-db", "uid": "pipeline-health"}]

# Sample query — crime rows
$ curl -s -u admin:admin -X POST http://localhost:3000/api/ds/query ...
{"results":{"A":{"status":200,"frames":[{"data":{"values":[[263395]]}}]}}}

# Sample query — Airflow DAG runs
$ curl -s -u admin:admin -X POST http://localhost:3000/api/ds/query ... (airflow-metadata)
{"results":{"A":{"status":200,"frames":[{"data":{"values":[["failed","success"],[2,1]]}}]}}}
```

- **Grafana healthy:** version 12.4.0, database ok
- **Both datasources provisioned:** Chicago Analytics + Airflow Metadata
- **Both dashboards loaded:** Pipeline Health (10 panels) + Crime + Divvy Analysis (6 panels)
- **All 16 panel queries verified against live data:** 8 Chicago Analytics queries + 6 analysis queries + 2 Airflow queries all return status 200
- **Live data confirmed:** 263,395 crime rows, 2,001 station reads, Airflow DAG runs (2 failed + 1 success in last 7 days)
- **Browser rendering verified (not just API):** panels display live data after `jsonData.database` fix
- **Top community area by crime:** Austin (12,700) — correct, Austin is historically Chicago's highest-crime area

---

## 3.2 — DBT Tests (2026-07-20)

### What Was Built

Added a custom singular DBT test (`assert_crime_in_chicago_bounds`) for geographic bounds checking, and built a recorder that parses dbt's `run_results.json` after every `dbt build` and writes test outcomes into `observability.dbt_test_results` in Postgres. The Grafana "DBT tests" panel (previously a static `SELECT 59` placeholder) now queries that table and shows live passing/failing/warnings counts for the latest dbt invocation. Both DAGs (`crime_batch`, `divvy_stream`) gained a `record_dbt_results` task that runs the recorder after `dbt_build`.

### Files Created/Modified

| File | Action | Purpose |
|---|---|---|
| `dbt/tests/assert_crime_in_chicago_bounds.sql` | Created | Singular test — flags crime events with populated lat/long outside Chicago's bounding box (lat 41.64–42.03, lon -87.95–-87.52). Complements the per-column range tests on `fact_crime_events.latitude/longitude` with a single readable combined check. |
| `airflow/scripts/record_dbt_results.py` | Created | Parses `dbt/target/run_results.json` after `dbt build`, upserts one row per test into `observability.dbt_test_results`. Idempotent (keyed on invocation_id + test_name). Identifies tests by `unique_id` prefix `test.` (dbt 1.11 has no `resource_type` field). Runs in the Airflow container (psycopg2 available via postgres provider). |
| `docker-compose.yml` | Modified | Added `./airflow/scripts:/opt/airflow/scripts` volume mount to `x-airflow-common` anchor (available to all Airflow containers). |
| `airflow/dags/crime_batch_dag.py` | Modified | Added `record_dbt_results` BashOperator after `dbt_build`; updated dependency chain to `... >> dbt_build >> record_dbt_results`. |
| `airflow/dags/divvy_stream_dag.py` | Modified | Added `record_dbt_results` BashOperator between `dbt_build` and `stop_stream`; updated dependency chain to `... >> dbt_build >> record_dbt_results >> stop_stream >> stop_producer`. |
| `grafana/dashboards/pipeline_health.json` | Modified | Rewired panel id 8 ("DBT tests") from static `SELECT 59 AS dbt_tests_passing` to real query against `observability.dbt_test_results` returning passing/failing/warnings counts for the latest invocation. Added field overrides (Passing=green, Failing=red at ≥1, Warnings=neutral). Retitled "DBT test outcomes (latest run)". |

### New Database Object

| Object | Purpose |
|---|---|
| `observability` schema | Dedicated schema for pipeline observability metadata (separate from `raw`/`staging`/`mart`). Created idempotently by `record_dbt_results.py`. |
| `observability.dbt_test_results` table | One row per test per dbt invocation. Columns: `invocation_id`, `generated_at`, `test_name`, `status`, `failures`, `execution_time`, `recorded_at`. PK: `(invocation_id, test_name)`. Queried by the Grafana panel. |

### Architecture — What Was Built

```mermaid
graph LR
    A["dbt build<br/>(DAG task)"] --> B["run_results.json<br/>(target/)"]
    B --> C["record_dbt_results<br/>(Airflow task)"]
    C --> D["observability.dbt_test_results<br/>(Postgres)"]
    D --> E["Grafana panel<br/>'DBT test outcomes'"]
    F["assert_crime_in_chicago_bounds.sql<br/>(singular test)"] --> A
```

dbt build runs all tests (including the new singular bounds test) and writes `run_results.json`. The `record_dbt_results` Airflow task parses that file and upserts one row per test into `observability.dbt_test_results`. Grafana queries the latest invocation's rows and renders passing/failing/warnings counts.

**For detailed architecture diagrams** (how files connect to containers, how images are built, how services depend on each other), see `docs/wiki/architecture.md`. That file is the permanent reference; this doc is the phase snapshot. Don't duplicate those diagrams here.

### Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Recorder captured 0 tests despite `dbt build` reporting `TOTAL=60` | Filtered on `resource_type == "test"`, but dbt 1.11's `run_results.json` does not populate `resource_type` (None for every entry). `name` is also None. | Changed filter to `unique_id.startswith("test.")`. Extracted readable name from `unique_id` by stripping `test.chicago_crime.` prefix and trailing `.<hash>`. |
| 2 | Grafana dashboard JSON malformed after incremental panel edits | Multiple `edit` ops dropped the `"fieldConfig": {` wrapper and `"matcher": {` opener, leaving `defaults`/`overrides` at wrong nesting level. | Re-inserted missing wrappers; validated with `python3 -c "import json; json.load(open(...))"`. |

### Lessons

- **dbt 1.11 `run_results.json` has no `resource_type` field** — every entry has `resource_type: null`. Identify tests by `unique_id` prefix (`test.`), models by `model.`, seeds by `seed.`. The `name` field is also null; the readable name lives inside `unique_id` as `test.chicago_crime.<name>.<hash>`.
- **dbt's `TOTAL=N` counts all resources, not just tests** — `TOTAL=60` = 1 seed + 7 models + 52 tests. The recorder correctly captured 52 tests; the "missing 8" were non-test resources. Don't confuse dbt's resource total with the test count.
- **Edit JSON panel objects wholesale, not field-by-field** — the edit tool's line-range semantics make it easy to drop a closing brace or object opener when patching nested Grafana JSON. For panel rewrites, replace the entire panel object in one op and validate with `json.load` before moving on.
- **Observability metadata gets its own schema** — `observability.dbt_test_results` lives in a dedicated schema (not `mart` or `raw`), created idempotently by the recorder. Keeps pipeline metadata out of the analytics mart and the raw landing zone.

### Decisions Made

| Decision | Choice | Why |
|---|---|---|
| Custom recorder vs. dbt-artifacts package | Custom 40-line script | No new dbt dependency; project keeps `packages.yml` small. The artifact we need (test outcomes) is tiny — a script is clearer than pulling a package that writes 10+ tables. |
| Observability schema | Dedicated `observability` schema (not `mart` or `raw`) | Pipeline metadata ≠ analytics data. Keeps the mart clean for BI queries and the raw zone clean for ingested data. Created idempotently by the recorder (no init.sql change or volume wipe needed). |
| Test identification | `unique_id.startswith("test.")` | dbt 1.11's `run_results.json` does not populate `resource_type` (it is `None` for every entry). The `unique_id` prefix is the reliable identifier. |
| Test name extraction | Strip `test.chicago_crime.` prefix and trailing `.<hash>` from `unique_id` | The `name` field is `None` in dbt 1.11's run_results. The readable name lives inside `unique_id`. |
| Recorder runs in Airflow container | Not in the dbt container | Airflow already has psycopg2 (via postgres provider). The dbt container is `python:3.11-slim` with only dbt installed. The recorder reads `run_results.json` from the shared `./dbt` mount. |
| Panel shows latest invocation only | `WHERE generated_at = (SELECT max(generated_at) ...)` | The panel answers "is the latest dbt build healthy?" not "what's the history?". History is queryable directly from the table. |

### Verification

```bash
# dbt build summary (divvy_stream DAG run)
$ grep "Done." .../task_id=dbt_build/attempt=1.log
Done. PASS=60 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=60

# Recorder output
$ python /opt/airflow/scripts/record_dbt_results.py
Recorded 52 dbt test results for invocation d76f8967-... (pass=52) into observability.dbt_test_results

# Grafana panel query via API
$ curl -s -u admin:admin -X POST http://localhost:3000/api/ds/query ...
passing = [52], failing = [0], warnings = [0]

# Both DAGs — all tasks succeeded including record_dbt_results
$ airflow tasks states-for-dag-run divvy_stream ...
create_topic: success, start_producer: success, start_stream: success,
wait_for_data: success, dbt_build: success, record_dbt_results: success,
stop_stream: success, stop_producer: success
```

- **`dbt build` (via divvy_stream DAG):** PASS=60 WARN=0 ERROR=0 SKIP=0 TOTAL=60 (1 seed + 7 models + 52 tests)
- **Singular bounds test:** `assert_crime_in_chicago_bounds` ran and passed (status='pass' in `observability.dbt_test_results`)
- **52 tests recorded:** all status='pass' for the latest invocation
- **Grafana panel:** query returns passing=52, failing=0, warnings=0; dashboard loads with updated title "DBT test outcomes (latest run)"
- **Both DAGs:** `record_dbt_results` task succeeded in `crime_batch` (5 tasks) and `divvy_stream` (8 tasks)
- **3 dbt invocations recorded** total (manual + divvy_stream DAG + crime_batch DAG)

---

## 3.3 — Airflow Robustness (2026-07-20)

### What Was Built

Added a `SqlSensor` to `crime_batch` that fixes the race condition with `divvy_stream` — `dbt_build` now waits for `raw.station_status` to exist before running, since `dim_date` spans both sources. Updated both DAGs with retries (3x, 5min delay), `on_failure_callback` (structured failure logging), and `execution_timeout` on `dbt_build` (30min). Added a "Failed tasks" panel to Grafana. Airflow 3.0 removed the SLA feature, so `execution_timeout` is used instead of `sla=`.

### Files Created/Modified

| File | Action | Purpose |
|---|---|---|
| `airflow/dags/callbacks.py` | Created | Shared `on_failure_callback` — logs dag_id, task_id, run_id, try_number, exception to Airflow task logs. Imported by both DAGs. |
| `docker-compose.yml` | Modified | Added `AIRFLOW_CONN_POSTGRES_DEFAULT` env var to `x-airflow-common` anchor. SqlSensor uses this connection to query the analytics warehouse. Format: `postgresql://user:pass@postgres:5432/db`. |
| `airflow/dags/crime_batch_dag.py` | Modified | Added `SqlSensor` (`wait_for_stream_data`) between `spark_crime_batch` and `dbt_build`. Uses `to_regclass('raw.station_status')` with `mode="reschedule"`, 60s poke interval, 1hr timeout. Updated `default_args` (retries=3, retry_delay=5min, on_failure_callback). Added `execution_timeout=30min` on `dbt_build`. Updated dependency chain. |
| `airflow/dags/divvy_stream_dag.py` | Modified | Updated `default_args` (retries=3, retry_delay=5min, on_failure_callback). Added `execution_timeout=30min` on `dbt_build`. Set `retries=0` on `stop_stream` + `stop_producer`. |
| `grafana/dashboards/pipeline_health.json` | Modified | Added panel id 11 "Failed tasks (last 7 days)" — queries `task_instance` for failed/upstream_failed states. Moved task instances table (id 10) down to y=30. |

### Architecture — What Was Built

```mermaid
graph TB
    subgraph "crime_batch DAG"
        A["download_crime"] --> B["clear_dbt_schemas"]
        B --> C["spark_crime_batch"]
        C --> D["wait_for_stream_data<br/>SqlSensor"]
        D --> E["dbt_build<br/>execution_timeout=30min"]
        E --> F["record_dbt_results"]
    end

    subgraph "divvy_stream DAG"
        G["create_topic"] --> H["start_producer"]
        H --> I["start_stream"]
        I --> J["wait_for_data"]
        J --> K["dbt_build<br/>execution_timeout=30min"]
        K --> L["record_dbt_results"]
        L --> M["stop_stream<br/>retries=0"]
        L --> N["stop_producer<br/>retries=0"]
    end

    D -.->|"waits for raw.station_status"| J

    subgraph "Shared"
        O["callbacks.py<br/>on_failure_callback"]
    end

    O -.->|"default_args"| E
    O -.->|"default_args"| K
```

The SqlSensor (`wait_for_stream_data`) creates an explicit dependency: `crime_batch` waits for `raw.station_status` to exist (created by `divvy_stream`) before running `dbt_build`. Both DAGs share the `on_failure_callback` and use `execution_timeout` on `dbt_build`.

**For detailed architecture diagrams**, see `docs/wiki/architecture.md`.

### Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | SqlSensor failed: `'str' object has no attribute 'fetchone'` | Used `success=lambda result: result.fetchone()[0]` — assumed callback receives a cursor. Airflow 3.0's `SqlSensor.poke` passes `records[0]` (a row tuple) to the success callable. | Changed to `success=lambda row: row[0] is not None`. The row is a 1-tuple like `('raw.station_status',)` or `(None,)`. |
| 2 | `sla=` triggers deprecation warning, is a no-op | Airflow 3.0 removed the SLA feature entirely. `sla=` is accepted but does nothing. No SLA misses recorded in `dag_warning`. | Replaced with `execution_timeout=timedelta(minutes=30)`. Changed Grafana panel from "SLA misses" to "Failed tasks". |
| 3 | Stuck DAG run blocked new runs | Failed sensor task was `up_for_retry` (3 retries × 5min = 15min). DAG run stayed `running`, blocking new runs (`max_active_runs=1`). | Manually marked stuck run as `failed` in metadata DB. New run then started. |

### Lessons

- **Airflow 3.0 removed the SLA feature** — `sla=` is a no-op with a deprecation warning. No SLA misses are recorded in `dag_warning`. Use `execution_timeout=` instead — it actually fails the task if it exceeds the limit. SLA is planned to return in Airflow 3.1+.
- **Airflow 3.0 SqlSensor success callback receives a row, not a cursor** — `SqlSensor.poke` calls `hook.get_records(sql)` → list of rows, then passes `records[0]` to the success callable. The callable receives a single row (tuple), not a cursor object. Don't call `.fetchone()` on it.
- **Sensors + `max_active_runs=1` can block new runs** — a sensor in `up_for_retry` state keeps the DAG run in `running` state, which blocks new runs if `max_active_runs=1`. For sensor tasks, consider fewer retries or shorter retry delays to avoid long blocking periods.
- **Make implicit cross-DAG dependencies explicit with sensors** — `dim_date` spans both crime + station sources, creating an implicit dependency between `crime_batch` and `divvy_stream`. The SqlSensor makes this explicit: `crime_batch` waits for `raw.station_status` to exist before building marts. This is cleaner than splitting dbt models (batch-only vs stream-only) because `dim_date` legitimately needs both sources.
- **`AIRFLOW_CONN_<CONN_ID>` env var pattern** — Airflow auto-creates connections from env vars. `AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://user:pass@host:port/db` creates a connection with `conn_id="postgres_default"`. No need to use the Airflow UI or CLI.

### Decisions Made

| Decision | Choice | Why |
|---|---|---|
| Race condition fix: sensor vs. split dbt models | SqlSensor | `dim_date` legitimately spans both sources (crime + station). Splitting dbt models would break `dim_date`'s UNION ALL. The sensor makes the implicit dependency explicit without splitting models. |
| `execution_timeout` vs `sla=` | `execution_timeout` | Airflow 3.0 removed SLA. `sla=` is a no-op. `execution_timeout` actually fails the task on timeout. |
| Grafana panel: failed tasks vs SLA misses | Failed tasks | Airflow 3.0 doesn't record SLA misses in `dag_warning` (SLA feature removed). Failed tasks (including timeout failures) are queryable from `task_instance`. |
| `on_failure_callback` vs email | Callback (logging) | No email server in local dev. The callback logs structured failure context to Airflow logs — visible in the UI. In production, this is where Slack/email/PagerDuty would go. |
| `retries=0` on cleanup | Don't retry cleanup | `stop_stream`/`stop_producer` are best-effort cleanup with `trigger_rule=ALL_DONE`. If `kill` fails, the process is already gone. Retrying adds delay without value. |
| SqlSensor `mode="reschedule"` | Not `mode="poke"` | The sensor may wait up to 1hr for `divvy_stream` to run. `reschedule` releases the worker slot between pokes; `poke` holds the slot for the entire wait. |

### Verification

```bash
# Both DAGs parse
$ airflow dags list | grep -E "crime_batch|divvy_stream"
crime_batch  | /opt/airflow/dags/crime_batch_dag.py  | chicago-pipeline | False
divvy_stream | /opt/airflow/dags/divvy_stream_dag.py | chicago-pipeline | False

# Connection created via env var
$ airflow connections get postgres_default
host=postgres, schema=chicago_analytics, login=chicago, port=5432

# crime_batch tasks (note wait_for_stream_data sensor)
$ airflow tasks list crime_batch
clear_dbt_schemas, dbt_build, download_crime, record_dbt_results, spark_crime_batch, wait_for_stream_data

# divvy_stream DAG run — all 8 tasks succeeded
$ airflow tasks states-for-dag-run divvy_stream ...
create_topic: success, start_producer: success, start_stream: success,
wait_for_data: success, dbt_build: success, record_dbt_results: success,
stop_stream: success, stop_producer: success

# crime_batch DAG run — all 6 tasks succeeded (sensor passed immediately)
$ airflow tasks states-for-dag-run crime_batch ...
download_crime: success, clear_dbt_schemas: success, spark_crime_batch: success,
wait_for_stream_data: success, dbt_build: success, record_dbt_results: success

# Grafana — 11 panels, failed tasks panel returns data
$ curl ... /api/dashboards/uid/pipeline-health
Total panels: 11 (was 10 — added "Failed tasks (last 7 days)")
failed_tasks = [1]
```

- **Both DAGs parse successfully** (Airflow `dags list` shows crime_batch + divvy_stream)
- **`postgres_default` connection created via env var** (verified with `airflow connections get`)
- **`callbacks.py` imports successfully** from both DAGs
- **SqlSensor:** passed immediately (raw.station_status exists from prior divvy_stream run)
- **Both DAGs:** all tasks succeeded (crime_batch 6/6, divvy_stream 8/8)
- **Grafana:** 11 panels loaded, "Failed tasks" panel returns failed_tasks=1 (from the earlier failed SqlSensor attempt before the fix)
- **on_failure_callback:** wired via default_args, fires when a task exhausts all 3 retries

---

## 3.4 — Verification (2026-07-20)

### What Was Built

Broke the pipeline in 3 ways and confirmed all observability mechanisms catch the failures: (1) stopped producer → Grafana stream freshness panel went red, (2) injected bad crime data → 2 DBT tests failed + Grafana DBT panel went red, (3) failed a task → Airflow retried 3x + on_failure_callback logged + Grafana failed-tasks panel went red. Pipeline restored to working state after each test.

**Verification phase — no new permanent code.** Created + deleted a throwaway DAG (`verify_failure_dag.py`) to test task failure handling without touching production DAGs.

### Files Created (temporary)

| File | Purpose | Status |
|---|---|---|
| `airflow/dags/verify_failure_dag.py` | Throwaway DAG with `exit 1` task, retries=3, on_failure_callback | **Deleted** after verification |

### Verification Scenarios

#### Scenario 1: Stream freshness alert

| Aspect | Detail |
|---|---|
| **Break** | Producer stopped (divvy_stream DAG completed, no new data flowing) |
| **Observability** | Grafana "Stream freshness" panel (id 6) — threshold red at 900s (15min) |
| **Result** | Freshness = 1195s (19.9min) > 900s → panel RED ✅ |
| **Query** | `SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ingest_timestamp))) AS seconds_since_last_ingest FROM raw.station_status;` |

#### Scenario 2: DBT test failure

| Aspect | Detail |
|---|---|
| **Break** | Injected bad crime row into `raw.crime_events` (id=99999999, lat=45.0, lon=-100.0 — South Dakota, outside Chicago bounds 41.64–42.03 / -87.95–-87.52) |
| **Observability** | DBT bounds tests in `staging/schema.yml` (latitude + longitude range checks) + Grafana "DBT test outcomes" panel (id 8) |
| **Result** | 2 tests failed (`expect_column_values_to_be_between` for latitude + longitude), recorder captured fail=2, Grafana panel showed passing=30 failing=2 → RED ✅ |
| **Restore** | Deleted bad row, re-ran `dbt build` (PASS=60), ran recorder → Grafana panel back to passing=52 failing=0 → GREEN |

#### Scenario 3: Task failure + retries + callback

| Aspect | Detail |
|---|---|
| **Break** | Throwaway DAG `verify_failure_handling` with `fail_on_purpose` task (`exit 1`), retries=3, retry_delay=10s, on_failure_callback |
| **Observability** | Airflow retries (4 attempts) + on_failure_callback (structured log) + Grafana "Failed tasks" panel (id 11) |
| **Result** | Task failed after 4 attempts (try_number=4 = 1 initial + 3 retries), callback logged `dag=verify_failure_handling task=fail_on_purpose run=manual__... try=4 exception=None`, Grafana "Failed tasks" panel showed failed_tasks=2 → RED ✅ |
| **Restore** | Deleted DAG file, ran `airflow dags delete verify_failure_handling` (removed 5 metadata records) |

### Architecture — How Observability Catches Failures

```mermaid
graph TB
    subgraph "Scenario 1: Stream Freshness"
        A["Producer stops"] --> B["No new rows in raw.station_status"]
        B --> C["MAX(ingest_timestamp) ages"]
        C --> D["Grafana panel id 6<br/>seconds_since_last_ingest > 900"]
        D --> E["Panel turns RED"]
    end

    subgraph "Scenario 2: DBT Test Failure"
        F["Bad row injected<br/>lat=45, lon=-100"] --> G["dbt build runs"]
        G --> H["Bounds tests fail<br/>latitude + longitude range"]
        H --> I["Recorder captures fail=2"]
        I --> J["Grafana panel id 8<br/>failing > 0"]
        J --> K["Panel turns RED"]
    end

    subgraph "Scenario 3: Task Failure"
        L["Task exits 1"] --> M["Airflow retries 3x"]
        M --> N["All 4 attempts fail"]
        N --> O["on_failure_callback fires<br/>logs structured context"]
        N --> P["task_instance.state = failed"]
        P --> Q["Grafana panel id 11<br/>failed_tasks > 0"]
        Q --> R["Panel turns RED"]
    end
```

### Errors Hit

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `dbt build` manual run: image `chicago-crime-dbt:latest` not found | Wrong image name. DAGs use `chicago-data-pipeline-dbt:latest`. | Used correct name from `DBT_IMAGE` var. |
| 2 | `dbt build` manual run: `--project-dir /opt/dbt` does not exist | Wrong path. DAGs use `/opt/airflow/dbt` + `/opt/airflow/dbt_profiles`. | Used correct path from `DBT_DIR` + `DBT_PROFILES_DIR` vars. |
| 3 | Throwaway DAG not found by `airflow dags trigger` | DAG bundle refresh interval is long (~30s+). New DAGs aren't immediately available. | Ran `airflow dags reserialize` to force bundle refresh, then triggered. |
| 4 | `airflow dags delete` failed with `EOFError: EOF when reading a line` | Delete command prompts for confirmation (`y/n`), but `docker compose exec -T` has no TTY. | Piped `echo "y"` into the command. |

### Lessons

- **Panel thresholds are sufficient alerts for local dev** — Grafana's unified alerting system (contact points, notification policies, alert rules) is overkill for a learning project. The panel turning red at a threshold IS the alert. Full alerting would be a bonus feature, not a phase gate requirement.
- **DBT singular tests catch what column tests catch, but more readably** — the injected bad row (lat=45, lon=-100) failed BOTH the column-range tests in `staging/schema.yml` AND the singular bounds test `assert_crime_in_chicago_bounds.sql`. The column tests fired first (they run on `stg_crime_events`, the singular test runs on `fact_crime_events`). Both are valuable — column tests are granular, the singular test is a readable combined check.
- **Airflow 3.0 `try_number` starts at 1, not 0** — a task with `retries=3` has try_number values 1, 2, 3, 4 (1 initial + 3 retries). The final failed attempt has `try_number=4`, not 3.
- **`on_failure_callback` fires only after all retries are exhausted** — the callback logged `try=4` (the final attempt), not on each individual retry. This is the correct behavior — you want to alert once after retries are exhausted, not on every transient failure.
- **Throwaway DAGs are the right way to test failure handling** — creating a temporary DAG with `exit 1` tests retries + callbacks without risking production DAGs or needing to modify them. Delete the DAG file + run `airflow dags delete` to clean up metadata.
- **Manual `dbt build` is correct when you need to preserve test data** — triggering the crime_batch DAG would re-run `spark_crime_batch` and overwrite the injected bad row. Running `dbt build` manually (with the same image/paths from the DAG) preserves the bad row through the dbt build step.

---

## Phase 3 Gate — MET

| Criterion | Status | How |
|---|---|---|
| Grafana shows live row counts and stream freshness | ✅ | Phase 3.1 — 11 panels on Pipeline Health dashboard (10 original + 1 failed tasks panel from 3.3) |
| Breaking the pipeline (stop producer) shows as Grafana alert within minutes | ✅ | Scenario 1 — freshness panel red at 900s threshold (freshness = 1195s) |
| DBT tests catch a deliberately introduced data quality issue | ✅ | Scenario 2 — 2 tests failed on bad lat/lon (lat=45.0, lon=-100.0 — South Dakota) |
| Airflow retries a deliberately failing task and alerts on SLA miss | ✅ | Scenario 3 — 4 attempts (1 initial + 3 retries), callback fired, Grafana failed-tasks panel red. (Airflow 3.0 removed SLA; used `execution_timeout` + failed-tasks panel instead.) |

**Phase 3 COMPLETE. Phase 4 (Cloud) unlocked.**

## Dates

| Sub-phase | Date | Duration |
|---|---|---|
| 3.1 — Grafana | 2026-07-18 | 1 day |
| 3.2 — DBT Tests | 2026-07-20 | 1 day |
| 3.3 — Airflow Robustness | 2026-07-20 | 1 day |
| 3.4 — Verification | 2026-07-20 | 1 day |

## Future Recommendations

- **Grafana unified alerting** — the panel-threshold approach (panel turns red) is sufficient for local dev. For production, wire Grafana's unified alerting (contact points, notification policies, alert rules) to send Slack/email/PagerDuty notifications when panels go red.
- **Geospatial crime-vs-ridership join** — the current crime-vs-ridership panel is a proxy (system-wide avg bikes, not per-area). GBFS doesn't include `community_area_id` on stations. A real join needs geospatial matching (station lat/long → community area polygon), deferred to Phase 4+ with full historical data.
- **Airflow SLA when it returns** — Airflow 3.0 removed the SLA feature; `execution_timeout` is the current substitute. When SLA returns in Airflow 3.1+, consider adding `sla=` back for tasks where exceeding the SLA should alert without necessarily failing the task.
- **Reduce sensor retry blocking** — sensors with `max_active_runs=1` can block new runs while in `up_for_retry`. Consider fewer retries or shorter retry delays for sensor tasks, or increase `max_active_runs` if the DAG supports concurrent runs.
- **DBT test history panel** — the current DBT panel shows only the latest invocation. A historical view (test outcomes over time, trend of pass/fail counts) would be valuable for spotting data quality regressions. The data already exists in `observability.dbt_test_results`.
- **Recorder for model freshness** — the recorder currently captures test outcomes only. Extending it to capture model build times, row counts, and freshness would give a fuller observability picture in Grafana.

---

**← Previous:** [Phase 2 — Live Stream](phase-2.md) | **Next:** [Phase 4 — Cloud Migration](phase-4.md)
