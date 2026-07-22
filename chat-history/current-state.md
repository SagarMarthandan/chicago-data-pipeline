# Current State — Handoff Document

> **Read this first in a new session.** This file is the handoff: current state, active decisions, and next steps. Last updated: 2026-07-22 (Phase 4.8 COMPLETE — BQML stretch goal done, regression model trained).

---

## Project

Chicago Crime + Divvy Bike-Share data engineering pipeline. A learning project that answers: *Does crime near a Divvy station affect ridership?*

- **Repo:** `~/chicago-data-pipeline/` (WSL, Ubuntu on Windows 10)
- **Git:** initialized on `main`, no commits yet (user commits manually)
- **Phase:** 1 COMPLETE. Phase 2 COMPLETE (2.1–2.6). Phase 3 COMPLETE (3.1–3.4). Phase 4 COMPLETE: 4.1 GCP setup ✅, 4.2 Terraform ✅, 4.3 Architecture change ✅, 4.4 Divvy trip history + correlation analysis ✅, 4.8 BigQuery ML (stretch) ✅. **The driving question is answered.** Phase 5 NEXT (CI/CD).
- **AI mode:** AI-writes-code (user said "you write it" — explicit mode switch from Socratic)

## Tech Stack

| Layer | Tool | Phase |
|---|---|---|
| Warehouse | Postgres (local) → BigQuery (cloud) | 1 → 4 |
| Batch | Spark DataFrames | 1 ✅ |
| Streaming | Kafka + Spark Structured Streaming | 2 ✅ |
| Transformation | DBT | 1+ ✅ |
| Orchestration | Airflow | 1+ ✅ |
| Observability | Grafana | 3 ✅ (3.1 Grafana, 3.2 DBT tests, 3.3 Airflow robustness, 3.4 Verification) |
| Cloud | Terraform + Airbyte | 4 (locked) |
| CI/CD | GitHub Actions + GHCR | 5 (locked) |

## Current Infrastructure

### Docker Compose — 11 services (7 Phase 1 + 2 Phase 2 + 1 Phase 3 + 1 build-only)

| Service | Image | Status |
|---|---|---|
| postgres | `postgres:16-alpine` | **healthy** — 3 schemas (raw, staging, mart) |
| spark-master | `apache/spark:3.5.1` + JDBC + Kafka connector | **healthy** — UI on port 8180, has `KAFKA_BOOTSTRAP_SERVERS` env + checkpoint volume |
| spark-worker | same as master | **running** — UI on port 8081, has `KAFKA_BOOTSTRAP_SERVERS` env |
| airflow-init | `apache/airflow:3.0.0-python3.11` | **exited (0)** — migrations complete |
| airflow-webserver | same | **healthy** — UI on port 8080 (admin/admin) |
| airflow-scheduler | same | **running** — heartbeat active |
| airflow-dag-processor | same | **running** — parses + serializes DAGs |
| zookeeper | `confluentinc/cp-zookeeper:7.6.0` | **healthy** — port 2181 (internal) |
| kafka | `confluentinc/cp-kafka:7.6.0` | **healthy** — ports 9092 (internal) + 29092 (host) |
| dbt-build | `python:3.11-slim` + dbt | build-only (never runs, exists for `docker compose build`) |
| grafana | `grafana/grafana:12.4.0` | **healthy** — UI on port 3000 (admin/admin), 2 datasources (chicago-analytics + airflow-metadata), 2 dashboards (Pipeline Health + Crime + Divvy Analysis) |

**Note:** At end of session, all services running. Postgres has only `observability.dbt_test_results` (streaming tables not populated — see data inventory below). BigQuery has all analytics data (crime 2.08M rows, Divvy 34.8M rows, fact_station_day 1.46M rows). Start all services with `docker compose up -d`.

### Data Inventory (verified 2026-07-22)

**BigQuery — Analytics Pipeline (ALL PRESENT ✅)**

| Table | Years | Rows | Notes |
|---|---|---|---|
| `staging.stg_crime_events` | 2018–2026 | 2,073,670 | From `bigquery-public-data.chicago_crime` (filtered `year >= 2018`) |
| `mart.fact_crime_events` | 2018–2026 | 2,073,670 | Partitioned by date_key, clustered by community_area + primary_type |
| `raw.divvy_trips` | 2020–2026 | 34,751,413 | dlt from S3 (75 monthly ZIPs, 2020-04 to 2026-06) |
| `mart.fact_divvy_trips` | 2020–2026 | 34,751,412 | Partitioned by started_at, clustered by start_station_id |
| `mart.fact_station_day` | 2020–2026 | 1,463,049 | THE analytics mart — geospatial join (ST_DISTANCE ≤ 402m) |
| `mart.crime_ridership_correlation` | — | ~3,200 | CORR() at overall/per-station/per-month scope |
| `mart.crime_ridership_model_training_data` | 2020–2023 | 815,472 | BQML training set + post_hook trains the model |
| `mart.crime_ridership_model_evaluation` | — | 1 | ML.EVALUATE (R²=0.434, MAE=13.4) |
| `mart.crime_ridership_model_weights` | — | 5 | ML.WEIGHTS (crime coefficient = +1.45) |
| `mart.crime_ridership_predictions` | 2024–2026 | 647,577 | ML.PREDICT on out-of-sample test data |

**Postgres — Streaming + Observability (PARTIAL ⚠️)**

| Table | Status | Notes |
|---|---|---|
| `observability.dbt_test_results` | ✅ Present | Test results from latest dbt build |
| `raw.station_status` | ❌ Missing | Phase 2 streaming table. Run `divvy_stream` DAG to repopulate. Not a dependency of BigQuery analytics. |
| `raw.crime_events` | ❌ Missing | Phase 1 Socrata extract. Analytics marts use BigQuery public dataset instead. Socrata pipeline exists as code fallback. |

**Analytics overlap window:** 2020-04 to 2026-06 (6+ years, 1.46M station-day observations). BQML trains on 2020-2023, tests on 2024-2026.

**See `docs/knowledge/data-sources.md` → "Current Data Inventory" for full details.**

### URLs
- **Airflow UI:** http://localhost:8080 (admin / admin)
- **Spark Master UI:** http://localhost:8180
- **Spark Worker UI:** http://localhost:8081
- **Postgres:** localhost:5432 (user: chicago, db: chicago_analytics)
- **Kafka (host):** localhost:29092
- **Kafka (Docker network):** kafka:9092
- **Grafana UI:** http://localhost:3000 (admin / admin) — anonymous Viewer access enabled
- **dbt docs:** http://localhost:8090 — auto-generated model documentation (15 models, 89 tests, 4 sources). Run via `docker run -p 8090:8090 ... dbt docs serve`. Port 8090 (8080 conflicts with Airflow).

### Key Architecture Decisions (Phase 1 + Phase 2 + Phase 3)
- **3 Postgres schemas:** `raw`, `staging`, `mart` (no `intermediate`)
- **Two databases in one Postgres:** `chicago_analytics` (warehouse) + `airflow_metadata` (Airflow internal)
- **Airflow 3.0.0** (upgraded from 2.8.4 — 2.x is EOL since April 2026)
- **SimpleAuthManager** (Airflow 3.0 default auth — users via env vars + passwords.json)
- **LocalExecutor** (parallelism without Redis/RabbitMQ)
- **JDBC driver baked into Spark image** (not `--packages` at runtime)
- **Spark UI on port 8180** (8080 conflicts with Airflow)
- **uv init (project mode)** for host Python — `pyproject.toml` + `uv.lock`
- **`pip install` as airflow user** in Airflow Dockerfile (not `uv pip install --system` — uv fails on kafka-python; apache/airflow refuses pip as root)
- **Socrata resource ID is `ijzp-q8t2`** (NOT `ijzp-q4t2` — the plan had a typo)
- **Confluent Platform 7.6.0** for Kafka (not Bitnami — no longer free; not `latest` — pinned)
- **Zookeeper mode** (not KRaft — more educational, traditional setup)
- **Two Kafka listeners:** `kafka:9092` (Docker network) + `localhost:29092` (host testing)
- **3 partitions for `divvy_station_status`** — station_id as key → same station → same partition
- **Single-broker overrides:** replication factor 1 for all internal topics
- **Explicit topic creation** (not auto-create) for custom partition counts — `KAFKA_NUM_PARTITIONS` env var doesn't work with Confluent images
- **4 Kafka connector JARs baked into Spark image** (spark-sql-kafka, spark-token-provider, kafka-clients, commons-pool2)
- **foreachBatch bridges streaming→JDBC** — JDBC has no native streaming sink; foreachBatch gives each micro-batch as a static DataFrame
- **Checkpoint via named volume** `spark_checkpoints` — persists Kafka offsets across container restarts
- **Stale station filter at 1 hour** — 888/2016 stations (44%) had stale `last_reported`; filtered in Spark, not DBT
- **is_* fields cast int→boolean in Spark** — GBFS returns 0/1 integers, not booleans
- **Kafka metadata columns** (partition, offset, timestamp) stored in `raw.station_status` for traceability
- **DBT dedup on Kafka coordinates** — `stg_station_status` uses `DISTINCT ON (kafka_partition, kafka_offset)` (streaming equivalent of crime's `DISTINCT ON (id)`)
- **dim_date spans all fact sources** — UNION ALL of min/max from `stg_crime_events` + `stg_station_status`; 1,292 rows covering 2023 + 2026
- **fact_station_reads grain = one row per station poll** — station_id is NOT unique (repeats across polls); no unique test on it
- **Observability schema (`observability`)** — dedicated schema for pipeline metadata (`dbt_test_results`), separate from `raw`/`staging`/`mart`. Created idempotently by `record_dbt_results.py`.
- **Grafana: two datasources** — `chicago-analytics` (warehouse) + `airflow-metadata` (Airflow DB). Postgres databases are isolated; can't cross-query without `postgres_fdw`.
- **Grafana: panel thresholds as alerts** — Phase 3.4 verified that panel turning red IS sufficient alerting for local dev. Full Grafana unified alerting (contact points, policies, rules) is optional, not a phase gate requirement.
- **SqlSensor for cross-DAG dependency** — `crime_batch` has `wait_for_stream_data` sensor that gates `dbt_build` on `raw.station_status` existing. Makes the implicit `dim_date` dependency on both sources explicit. Chosen over splitting dbt models because `dim_date` legitimately spans both sources.
- **`execution_timeout` not `sla=`** — Airflow 3.0 removed the SLA feature (`sla=` is a no-op). Use `execution_timeout=timedelta(...)` which actually fails the task on timeout.
- **`retries=0` on cleanup tasks** — `stop_stream`/`stop_producer` are best-effort cleanup with `trigger_rule=ALL_DONE`. Retrying adds delay without value.
- **`AIRFLOW_CONN_POSTGRES_DEFAULT` env var** — Airflow auto-creates connections from env vars. No need for UI/CLI configuration.
- **Custom dbt results recorder vs dbt-artifacts package** — 40-line `record_dbt_results.py` script, no new dbt dependency. Identifies tests by `unique_id` prefix `test.` (dbt 1.11 has no `resource_type` field).

## Phase 1 — COMPLETE (1.1–1.6)

All Phase 1 sub-phases verified end-to-end. Cold start → DAG run → 4 tasks succeed → marts queryable (263,394 fact rows). See `docs/phases/phase-1.*.md` for details.

### Phase 1 Key Files
- `ingestion/download_crime.py` — Socrata API → Parquet (263K rows)
- `spark/jobs/crime_batch.py` — Parquet → clean → Postgres `raw.crime_events`
- `dbt/` — staging + marts (dim_date, dim_community_area, dim_crime_type, fact_crime_events)
- `airflow/dags/crime_batch_dag.py` — 4 tasks: download → clear_dbt_schemas → spark → dbt_build
- `dbt/Dockerfile` — separate dbt image (protobuf conflict with Airflow)

## Phase 2 — COMPLETE (2.1–2.6)

### Phase 2.1 — Divvy GBFS Data Source (COMPLETE)
- Explored live GBFS feeds: `station_status.json` (2,016 stations, 12 mandatory + 2 optional fields) + `station_information.json` (static-ish dimension data)
- **4 design-changing findings:**
  1. `station_id` is mixed format (667 UUIDs + 1,349 numeric strings) → must stay as string (plan's `station_id::bigint` will fail)
  2. `is_renting`/`is_returning`/`is_installed` are integers 0/1, NOT booleans → need explicit cast
  3. `num_scooters_available`/`num_scooters_unavailable` are optional → Spark schema must tolerate absence
  4. One dead station had `last_reported: 86400` (Jan 2, 1970) → filter stale stations
- Full schema documented in `docs/knowledge/data-sources.md`
- No code written — exploration only

### Phase 2.2 — Kafka + Zookeeper Docker Services (COMPLETE)
- Added `zookeeper` (`confluentinc/cp-zookeeper:7.6.0`) + `kafka` (`confluentinc/cp-kafka:7.6.0`) to `docker-compose.yml`
- 3 named volumes: `kafka_data`, `zookeeper_data`, `zookeeper_log`
- `KAFKA_BOOTSTRAP_SERVERS: kafka:9092` added to spark-master + spark-worker
- Verified: topic creation, message produce/consume round-trip on both listeners
- `docs/knowledge/kafka.md` — comprehensive reference with 8 mermaid diagrams (cluster, topic, partition, offset, producer, consumer, broker, Zookeeper, message flow)

### Phase 2.3 — Kafka Producer (COMPLETE)
- `kafka/producers/divvy_producer.py` — polls GBFS every 60s, publishes each station as JSON to `divvy_station_status` topic
  - Key = `station_id` (same station → same partition → ordered processing)
  - Value = full station status JSON
  - Graceful SIGINT/SIGTERM shutdown with message flush
  - `--once` (single poll test), `--interval N` (custom cadence), `--bootstrap` (Kafka address)
- `kafka-python` 3.0.8 added to host venv + `airflow/requirements.txt`
- `./kafka:/opt/airflow/kafka_scripts` volume mount (renamed from `/opt/airflow/kafka` in Phase 2.6 to avoid shadowing kafka-python package)
- Verified: 2,016 messages/poll, 3 partitions (720/661/635 distribution), real Divvy data confirmed, continuous mode + graceful shutdown
- **2 errors hit:**
  1. `NoBrokersAvailable` removed in kafka-python 3.0.x → catch `KafkaError` instead
  2. Auto-created topic had 1 partition → `KAFKA_NUM_PARTITIONS` env var doesn't work with Confluent image → explicit `kafka-topics --create --partitions 3`
- Topic `divvy_station_status` was recreated at start of Phase 2.4 (3 partitions, replication factor 1)

### Phase 2.4 — Spark Structured Streaming (COMPLETE)
- `spark/jobs/divvy_stream.py` — Structured Streaming consumer: `readStream.format("kafka")` → `from_json()` → cast types → filter stale → `foreachBatch` → Postgres `raw.station_status`
  - 4 Kafka connector JARs baked into Spark Dockerfile (spark-sql-kafka, spark-token-provider, kafka-clients, commons-pool2)
  - `spark_checkpoints` named volume for checkpoint persistence (Kafka offsets)
  - 60s trigger matches producer poll interval; `--once` mode for testing
  - Stale station filter: `last_reported > now() - 1 hour` (drops 888/2016 = 44% stale stations)
  - is_* fields cast int→boolean; station_id stays string; optional scooter fields nullable
  - Kafka metadata columns (partition, offset, timestamp) stored for traceability
- `raw.station_status` table created in Postgres (18 columns)
- Verified: `--once` mode → 1,128 rows; continuous mode → 5,640 rows over 5 micro-batches (1,128/batch)
- **2 errors hit:**
  1. Checkpoint mkdir failed — named volume mounted as root, Spark runs as `spark` user → `chown` + Dockerfile fix
  2. AQE warning for streaming — not an error, Spark silently disables AQE for streaming queries

### Phase 2.5 — DBT Stream Models (COMPLETE)
- `dbt/models/staging/stg_station_status.sql` — staging view on `raw.station_status`: renames `last_reported`→`reported_at`, `ingest_timestamp`→`ingested_at`, deduplicates on Kafka coordinates (partition + offset)
- `dbt/models/marts/fact_station_reads.sql` — mart table: one row per station poll, with `date_key` FK to `dim_date`, derived `total_vehicles_available` (bikes + ebikes + COALESCE(scooters, 0)), Kafka traceability columns
- `dbt/models/marts/dim_date.sql` — modified to span both crime (2023) + station (2026) dates via UNION ALL; 1,292 rows
- Updated `dbt/models/staging/schema.yml` — added `station_status` source + `stg_station_status` model with tests
- Updated `dbt/models/marts/schema.yml` — updated `dim_date` description + year bounds (2023–2026), added `fact_station_reads` model with tests
- Verified: `dbt build` → 59/59 tests pass (PASS=59 WARN=0 ERROR=0 SKIP=0); `fact_station_reads` has 5,640 rows, 1,128 unique stations; analytics query ("avg bikes available per station") returns correct results
- **0 errors hit** — all tests passed on first run

### Phase 2.6 — Airflow DAG for Stream (COMPLETE)
- `airflow/dags/divvy_stream_dag.py` — 7-task DAG orchestrating the full streaming lifecycle:
  - `create_topic` → `start_producer` (--once mode) → `start_stream` (background) → `wait_for_data` (poll Postgres) → `dbt_build` → `stop_stream` → `stop_producer`
  - Producer uses `--once` mode (single poll, ~2,016 messages) — Airflow BashOperator kills background processes, so continuous mode doesn't work
  - Spark stream started as background process via `docker exec spark-master nohup spark-submit ... &`
  - `wait_for_data` captures INITIAL count, polls until CURRENT > INITIAL (delta logic)
  - Cleanup tasks use `trigger_rule=ALL_DONE` — no orphaned processes even on failure
- Infrastructure changes:
  - `airflow/Dockerfile` — switched from `uv pip install --system` to `pip install` as airflow user (uv couldn't create `kafka` dir; apache/airflow refuses pip as root)
  - `spark/Dockerfile` + `spark/entrypoint.sh` — entrypoint chowns checkpoint volume before dropping to spark via gosu
  - `docker-compose.yml` — renamed kafka mount to `/opt/airflow/kafka_scripts` (old path shadowed kafka-python package)
- Verified: all 7 tasks succeed, `raw.station_status` 2,001 rows, `fact_station_reads` 2,001 rows, 1,125 unique stations, avg 5.55 bikes/read
- **9 errors hit** — see changelog + phase doc for full list. Key lessons: volume mount path shadowing, apache/airflow pip guard, uv silent failures, BashOperator kills background processes, named volumes mount as root

### Phase 2 Gate — MET
Full end-to-end: `docker compose up` → Kafka → producer → Spark streaming → Postgres → DBT → queryable marts. Analytics query "avg bikes available per station" returns correct results.

## Phase 3 — COMPLETE (3.1, 3.2, 3.3, 3.4)

### Phase 3.1 — Grafana (COMPLETE)
- Added `grafana` service to `docker-compose.yml` (`grafana/grafana:12.4.0`, port 3000, `grafana_data` volume, anonymous Viewer access)
- Two Postgres datasources provisioned via `grafana/provisioning/datasources/postgres.yml`:
  - `chicago-analytics` (uid: `chicago-analytics`) → `chicago_analytics` database (raw + mart schemas)
  - `airflow-metadata` (uid: `airflow-metadata`) → `airflow_metadata` database (dag_run, task_instance)
  - **Why two:** Postgres databases are isolated — can't cross-query without `postgres_fdw`. One datasource per DB.
- Two dashboards provisioned via `grafana/provisioning/dashboards/dashboards.yml`:
  - `pipeline_health.json` (11 panels): row counts, stream ingestion rate, stream freshness, latest Kafka msg, DBT test outcomes (LIVE — wired in 3.2), failed tasks (Phase 3.3), Airflow DAG runs + task instances
  - `crime_divvy_analysis.json` (6 panels): top community areas by crime, crime types, avg vehicles per station, station availability heatmap, crime-vs-ridership proxy (THE DRIVING QUESTION), crime heatmap
- **4 errors hit:** (1) Go-template `{{.VAR}}` syntax in datasource YAML → Grafana uses `$VAR`; (2) env vars not in container after `restart` → need `up -d` to recreate; (3) cross-database query failed → added second datasource; (4) `jsonData.database` missing → browser panels showed "No data" despite API queries working (Grafana 12.4's Postgres plugin reads DB name from `jsonData.database`, not top-level `database:` field)
- **DAG race condition FIXED in Phase 3.3** — `crime_batch` now has a `SqlSensor` (`wait_for_stream_data`) that gates `dbt_build` on `raw.station_status` existing. The sensor makes the previously implicit cross-DAG dependency explicit. No longer need to manually run `divvy_stream` first.
- Verified: Grafana healthy (v12.4.0), both datasources + dashboards loaded, all 16 panel queries return status 200 against live data (263,401 crime rows, 1,130 station reads, Airflow DAG runs). Browser rendering verified (not just API).

### Phase 3.2 — DBT Tests (COMPLETE)
- Created `dbt/tests/assert_crime_in_chicago_bounds.sql` — singular test: flags crime events with lat/long outside Chicago's bounding box (lat 41.64–42.03, lon -87.95–-87.52). Complements per-column range tests with a combined readable check.
- Created `airflow/scripts/record_dbt_results.py` — parses `dbt/target/run_results.json` after `dbt build`, upserts one row per test into `observability.dbt_test_results` (new schema, created idempotently). Identifies tests by `unique_id` prefix `test.` (dbt 1.11 has no `resource_type` field; `name` is also null — readable name extracted from `unique_id`).
- Added `record_dbt_results` BashOperator task to both DAGs (after `dbt_build`). Mounted `./airflow/scripts:/opt/airflow/scripts` in `docker-compose.yml` `x-airflow-common` anchor.
- Rewired Grafana "DBT tests" panel (id 8) from static `SELECT 59 AS dbt_tests_passing` to real query against `observability.dbt_test_results` returning passing/failing/warnings counts for the latest invocation. Field overrides: Passing=green, Failing=red (≥1), Warnings=neutral. Retitled "DBT test outcomes (latest run)".
- Stream `not_null` tests on `stg_station_status` + `fact_station_reads` were already present from Phase 2.5 — no new tests needed there.
- New DB object: `observability` schema + `observability.dbt_test_results` table (PK: invocation_id + test_name). Created by the recorder, not by init.sql.
- **2 errors hit:** (1) Recorder captured 0 tests — filtered on `resource_type == "test"` but dbt 1.11 doesn't populate that field (None for every entry); fixed by filtering on `unique_id.startswith("test.")` and extracting name from `unique_id`. (2) Grafana dashboard JSON malformed after incremental panel edits — dropped `fieldConfig` wrapper + `matcher` opener; fixed by re-inserting wrappers + `json.load` validation. Lesson: edit JSON panel objects wholesale, not field-by-field.
- Verified: `dbt build` PASS=60 (1 seed + 7 models + 52 tests), `record_dbt_results` task succeeded in both DAGs, 52 tests recorded all status='pass', singular bounds test passed, Grafana panel query returns passing=52/failing=0/warnings=0, dashboard loads with updated panel.

### Phase 3.3 — Airflow Robustness (COMPLETE)
- Created `airflow/dags/callbacks.py` — shared `on_failure_callback` that logs structured failure context (dag_id, task_id, run_id, try_number, exception) to Airflow task logs. Wired into both DAGs via `default_args["on_failure_callback"]`.
- Added `SqlSensor` (`wait_for_stream_data`) to `crime_batch_dag.py` — gates `dbt_build` on `raw.station_status` existing via `to_regclass('raw.station_status')`. Fixes the race condition where `dim_date` (spans both crime + station sources) causes `dbt build` to fail if `divvy_stream` hasn't run. `mode="reschedule"`, 60s poke, 1hr timeout.
- Updated `default_args` in both DAGs: `retries=3`, `retry_delay=timedelta(minutes=5)`, `on_failure_callback=on_failure_callback`.
- Added `execution_timeout=timedelta(minutes=30)` to `dbt_build` in both DAGs. (Originally tried `sla=` but Airflow 3.0 removed the SLA feature — `sla=` is a no-op with deprecation warning.)
- Set `retries=0` on cleanup tasks (`stop_stream`, `stop_producer`) in `divvy_stream_dag.py` — don't retry cleanup.
- Added `AIRFLOW_CONN_POSTGRES_DEFAULT` env var to `docker-compose.yml` `x-airflow-common` anchor — SqlSensor needs a Postgres connection. Format: `postgresql://user:pass@postgres:5432/db`.
- Added "Failed tasks (last 7 days)" panel (id 11) to `pipeline_health.json` — queries `task_instance` for failed/upstream_failed states. Originally planned as SLA misses panel but Airflow 3.0 removed SLA tracking.
- **3 errors hit:** (1) SqlSensor `success` callback failed with `'str' object has no attribute 'fetchone'` — Airflow 3.0's `SqlSensor.poke` passes `records[0]` (a row tuple) to the callable, not a cursor; fixed to `lambda row: row[0] is not None`. (2) `sla=` is a no-op in Airflow 3.0 — SLA feature removed, replaced with `execution_timeout`. (3) Stuck DAG run (sensor `up_for_retry`) blocked new runs due to `max_active_runs=1` — manually marked stuck run as failed.
- Verified: both DAGs parse, `postgres_default` connection created, `divvy_stream` 8/8 tasks succeeded, `crime_batch` 6/6 tasks succeeded (SqlSensor passed immediately), Grafana 11 panels loaded, failed tasks panel returns data.

### Phase 3.4 — Verification (COMPLETE)
- **Verification phase — no new permanent code.** Broke the pipeline in 3 ways and confirmed all observability mechanisms catch the failures.
- **Scenario 1 (stream freshness):** Producer stopped → Grafana "Stream freshness" panel (id 6) turned red at 1195s > 900s threshold. ✅
- **Scenario 2 (DBT test failure):** Injected bad crime row (lat=45, lon=-100 — South Dakota) → 2 DBT bounds tests failed (latitude + longitude range) → recorder captured fail=2 → Grafana "DBT test outcomes" panel (id 8) showed passing=30 failing=2 → RED. Restored: deleted bad row, re-ran dbt build (PASS=60), Grafana back to passing=52 failing=0. ✅
- **Scenario 3 (task failure):** Throwaway DAG `verify_failure_handling` with `exit 1`, retries=3, on_failure_callback → task failed after 4 attempts (try_number=4) → callback logged `dag=verify_failure_handling task=fail_on_purpose try=4` → Grafana "Failed tasks" panel (id 11) showed failed_tasks=2 → RED. Deleted throwaway DAG + metadata. ✅
- **4 errors hit:** (1) Wrong dbt image name in manual run (`chicago-crime-dbt` vs `chicago-data-pipeline-dbt`). (2) Wrong dbt path (`/opt/dbt` vs `/opt/airflow/dbt`). (3) Throwaway DAG not found by `airflow dags trigger` — bundle refresh interval long, fixed with `airflow dags reserialize`. (4) `airflow dags delete` failed with EOFError — no TTY for confirmation prompt, fixed with `echo "y" |`.
- **Decision: panel thresholds are sufficient alerts for local dev** — Grafana unified alerting (contact points, notification policies, alert rules) is overkill for a learning project. Panel turning red IS the alert. Full alerting would be a bonus feature, not a phase gate requirement.

### Phase 3 Gate — MET
- Grafana shows live row counts and stream freshness ✅ (3.1)
- Breaking the pipeline (stop producer) shows as Grafana alert within minutes ✅ (Scenario 1)
- DBT tests catch a deliberately introduced data quality issue ✅ (Scenario 2)
- Airflow retries a deliberately failing task and alerts on SLA miss ✅ (Scenario 3 — used execution_timeout + failed-tasks panel since Airflow 3.0 removed SLA)

## Phase 4 — COMPLETE (4.1, 4.2, 4.3, 4.4 ALL DONE)

### Phase 4.1 — GCP Project Setup (COMPLETE)
- Chose BigQuery over Snowflake/Redshift: free tier, serverless, DBT first-class.
- Created GCP project `chicago-divvy-pipeline` (ID `480666653891`), linked billing, enabled APIs (BigQuery, Storage, Resource Manager).
- Created service account `terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com` with 4 scoped roles (bigquery.dataOwner, bigquery.jobUser, storage.admin, iam.serviceAccountTokenCreator — NOT owner).
- Downloaded key to `~/chicago-divvy-pipeline-credentials.json` (chmod 600→644 for container access, gitignored).
- See `docs/knowledge/gcp.md` for full reference.

### Phase 4.2 — Terraform Provisioning (COMPLETE)
- Created `terraform/` with `providers.tf` (Google provider v7.40.0), `variables.tf`, `main.tf` (3 resources: `google_bigquery_dataset.raw`, `google_bigquery_dataset.mart`, `google_storage_bucket.data_lake`).
- `terraform init`/`plan`/`apply` successful. Verified: `bq ls` → raw+mart, `gsutil ls` → bucket.
- See `docs/knowledge/terraform.md` for full reference.

### Phase 4.3 — Architecture Change: Postgres → GCS/BigQuery (COMPLETE)
- **Spark**: GCS connector JAR added. `crime_batch.py` now writes Parquet to `gs://chicago-divvy-pipeline-data-lake/raw/crime/` (was Postgres JDBC).
- **Airflow**: gcloud SDK + bq CLI installed. New `bq_load_crime` task (GCS Parquet → BigQuery `raw.crime_events`). Removed `clear_dbt_schemas` + `wait_for_stream_data` sensor. `dbt_build` now `--exclude stg_station_status fact_station_reads` + passes GCP env vars.
- **DBT**: Switched to `dbt-bigquery==1.12.0`. Both `profiles.yml` files rewritten for BigQuery. SQL dialect fixes: `DISTINCT ON`→`QUALIFY`, `generate_series`→`GENERATE_DATE_ARRAY`, `::type`→`SAFE_CAST`, `TO_CHAR`→`FORMAT_TIMESTAMP`. `dim_date` now crime-only (dropped station_status UNION). `try_cast` macro has BigQuery type mapping.
- **Streaming stays on Postgres**: `stg_station_status` + `fact_station_reads` excluded from BigQuery DBT build via `--exclude`. Source kept in `schema.yml` for parsing.
- **Verification**: All 4 tasks tested individually (download_crime failed on network timeout — pre-existing, not 4.3 related). spark_crime_batch: 263,402 rows→GCS. bq_load_crime: 263,403 rows→BigQuery. dbt_build: 38/38 tests pass. record_dbt_results: 32 results→Postgres. BigQuery marts verified: dim_date(365), fact_crime_events(263,403), dim_community_area(77), dim_crime_type(323).
- **6 errors hit**: Docker credential helper, stale Airflow image, bq CLI auth, credentials file permissions, DBT `--exclude` parsing, Socrata timeout. See changelog for details.

### Phase 4.4 — Divvy Trip History + Correlation Analysis (COMPLETE)
- **dlt ingestion:** Installed `dlt[bigquery]` 1.29.0. Created `ingestion/load_divvy_trips.py` — S3 ZIP → CSV → BigQuery `raw.divvy_trips` (append mode). Ingested 34,751,413 rows across 75 months (2020-04 to 2026-06).
- **Crime source switch:** `stg_crime_events` now reads from `bigquery-public-data.chicago_crime.crime` (8.6M rows, 2001-present) instead of 263K-row Socrata extract. Filtered to `year >= 2018` for Divvy overlap.
- **New DBT models:** `stg_divvy_trips`, `dim_stations` (ST_GEOGPOINT), `fact_divvy_trips` (partitioned by `started_at`), `fact_station_day` (THE analytics mart — geospatial join ST_DISTANCE ≤ 402m, 1.5M rows), `crime_ridership_correlation` (CORR() at 3 scopes, 3.2K rows).
- **Partitioning:** `fact_crime_events` (date_key + community_area_id/primary_type), `fact_divvy_trips` (started_at + start_station_id), `fact_station_day` (date_key + station_id). Verified pruning: 2.17% of full scan (97.8% savings).
- **Airflow DAGs:** Created `divvy_trip_history_dag.py` (3 tasks). Simplified `crime_batch_dag.py` to 2 tasks (removed download/spark/bq_load).
- **Grafana:** BigQuery datasource + scatter plot (panel 7) + correlation gauge (panel 8).
- **DBT build:** 67/67 tests pass. All models materialized as tables (required for BigQuery partitioning).
- **Key finding:** Overall Pearson correlation = **+0.20** (weak positive). Both crime and ridership are higher in busy areas — confounding variable is urban activity level, not causation. Per-month range: 0.08 (Apr 2020 COVID) to 0.31 (Sep 2024).
- **7 errors hit:** stale Airflow image, coordinate test failures (Missouri + Montreal rows), missing `primary_type` in SELECT for cluster_by, missing FROM clause (edit accident), column name mismatch in correlation CTE, stale dim_date/dim_crime_type. See changelog for details.
- See `docs/phases/phase-4.4-divvy-trip-history.md` for full details.

### Phase 4.8 — BigQuery ML (stretch goal, COMPLETE)
- **BQML linear regression:** Trained `mart.crime_ridership_model` (linear_reg) via dbt post_hook. Features: crime_count_within_quarter_mile, day_of_week, month, station_id (fixed effect). Label: trip_count.
- **4 new DBT models:** `crime_ridership_model_training_data` (815K rows, 2020-2023 + post_hook), `crime_ridership_model_evaluation` (ML.EVALUATE, R²=0.434), `crime_ridership_model_weights` (ML.WEIGHTS, crime coefficient +1.45), `crime_ridership_predictions` (ML.PREDICT, 648K rows 2024+).
- **Airflow DAGs updated:** `divvy_trip_history_dag.py` `--select` includes BQML models; `crime_batch_dag.py` `--exclude` excludes them.
- **DBT build:** 17/17 tests pass (BQML models only).
- **Key finding:** Crime coefficient = +1.45 (positive) even after controlling for station/day/month. Confirms Phase 4.4 correlation: crime doesn't reduce ridership. In-sample R²=0.434; seen-station out-of-sample R²=0.447; full out-of-sample R²=-199K (50% unseen stations — high-cardinality fixed effect breaks on new stations).
- **2 errors hit:** not_null test on `weight` (NULL for categoricals), catastrophically negative out-of-sample R² (no_split + unseen stations). See changelog for details.
- See `docs/phases/phase-4.8-bigquery-ml.md` for full details.

## Files Created (full repo structure)

```
~/chicago-data-pipeline/
├── .env.example
├── .gitignore
├── .vscode/settings.json
├── AGENTS.md
├── README.md
├── docker-compose.yml        ← 12 services (Phase 1+2+3+4), Grafana BigQuery plugin, GCP credentials mounts, YAML anchors
├── terraform/                ← Phase 4.2 — BigQuery datasets + GCS bucket
├── airflow/
│   ├── Dockerfile            ← includes gcloud SDK, dlt[bigquery]
│   ├── passwords.json
│   ├── requirements.txt      ← kafka-python, dlt[bigquery], google-cloud-bigquery
│   ├── scripts/
│   │   └── record_dbt_results.py
│   ├── dags/
│   │   ├── crime_batch_dag.py    ← Phase 4.4 — simplified to 2 tasks (dbt_build → record), crime from public dataset
│   │   ├── divvy_stream_dag.py   ← Phase 2.6 + 3.2 + 3.3
│   │   ├── divvy_trip_history_dag.py ← Phase 4.4 — 3 tasks (load → dbt_build → record)
│   │   └── callbacks.py
│   └── dbt_profiles/profiles.yml
├── spark/
│   ├── Dockerfile            ← apache/spark:3.5.1 + JDBC + Kafka + GCS connector
│   ├── entrypoint.sh
│   └── jobs/
│       ├── crime_batch.py    ← Phase 4.3 — writes to GCS
│       └── divvy_stream.py
├── ingestion/
│   ├── download_crime.py
│   └── load_divvy_trips.py   ← Phase 4.4 — dlt S3→BigQuery ingestion
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_crime_events.sql  ← Phase 4.4 — reads from bigquery-public-data
│   │   │   ├── stg_divvy_trips.sql   ← Phase 4.4 — staging for Divvy trips
│   │   │   ├── stg_station_status.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── dim_date.sql             ← Phase 4.4 — spans crime + Divvy dates (2018-2026)
│   │       ├── dim_community_area.sql
│   │       ├── dim_crime_type.sql
│   │       ├── dim_stations.sql         ← Phase 4.4 — station dimension with ST_GEOGPOINT
│   │       ├── fact_crime_events.sql    ← Phase 4.4 — partitioned + clustered
│   │       ├── fact_divvy_trips.sql     ← Phase 4.4 — partitioned + clustered
│   │       ├── fact_station_day.sql     ← Phase 4.4 — THE analytics mart (geospatial join)
│   │       ├── fact_station_reads.sql
│   │       ├── crime_ridership_correlation.sql ← Phase 4.4 — CORR() at 3 scopes
│   │       └── schema.yml
│   ├── packages.yml
│   ├── tests/
│   │   └── assert_crime_in_chicago_bounds.sql ← Phase 3.2 — singular geographic bounds test
└── docs/
    ├── knowledge/
    │   ├── data-sources.md   ← Phase 2.1 + 4.4 (GBFS schema + Divvy S3 + dlt)
    │   ├── grafana.md        ← Phase 3.1–3.4
    │   ├── dlt.md            ← Phase 4.4 — dlt reference
    │   ├── gcp.md            ← Phase 4.1
    │   ├── terraform.md      ← Phase 4.2
    │   ├── wsl.md, uv.md, docker-compose.md, postgres.md, dbt.md, spark.md
    │   ├── architecture.md   ← 10 sections
    │   ├── kafka.md, airflow.md, git.md, mermaid-syntax.md
    ├── learning-protocol.md
    ├── operations-performed.md ← TOC + entries through Phase 4.4
    ├── phases/
    │   ├── phase-1.1-docker.md through phase-1.6-verification.md
    │   ├── phase-2.1-gbfs-data-source.md through phase-2.6-airflow-stream-dag.md
    │   ├── phase-3.1-grafana.md through phase-3.4-verification.md
    │   └── phase-4.4-divvy-trip-history.md  ← Phase 4.4
    └── conventions/
        ├── airflow.md, dbt.md, docker.md, spark.md
```

## Next Steps

1. **Phase 5: CI/CD** — GitHub Actions + GHCR
   - Requires: Phase 4 complete ✅ met
   - New: Branch protection (dev/prod), PR checks (ruff + dbt parse + compose validate), versioned releases (semantic versioning), image push to GHCR
   - Plan in `chicago-pipeline-plan.md` (sections 5.1–5.6)
   - **Future task (after Phase 5):** Generate 50–100 interview questions covering the full pipeline — architecture decisions, error debugging, tool tradeoffs, production readiness. User must be able to answer all from memory.
   - **Future task (after Phase 5):** Comprehensive documentation restructuring — reorganize all docs for portfolio readability, consolidate redundant content, ensure consistent formatting across changelog/operations/phases/knowledge. Discuss approach when we get there.

- **Phase gates:** Phase 1 COMPLETE. Phase 2 COMPLETE (2.1–2.6). Phase 3 COMPLETE (3.1–3.4). Phase 4 COMPLETE (4.1–4.4 + stretch 4.8 BQML — driving question answered). Phase 5 NEXT (CI/CD). Do NOT skip ahead.
- **Learning protocol:** Socratic by default. User must say "write the code" to get code. Currently in AI-writes-code mode.
- **Three-doc system:** `changelog.md` (errors), `docs/knowledge/` (reference, one file per topic), `docs/operations-performed.md` (audit trail). Update all three after every change.
- **Phase-completion docs:** After each sub-phase is verified, create `docs/phases/phase-X.Y-<name>.md` from `TEMPLATE.md`. Include one high-level mermaid diagram + pointer to `docs/knowledge/architecture.md` for details.
- **Chat-history system:** Update `chat-history/` when context approaches 75%. Update `current-state.md` at the end of a session.
- **Doc maintainability (AGENTS.md rule 14):** When a `.md` file exceeds ~500 lines or ~20KB, split into a folder with one file per section + `index.md`. Append-only logs stay single but get a TOC with anchor links. (changelog.md and operations-performed.md exceed limits but are append-only logs with TOCs — exempt per rule 14.)
- **Mermaid quoting:** All node labels containing `:`, `/`, `$`, `{`, `}` must be wrapped in double quotes. See `docs/knowledge/mermaid-syntax.md` for rules + scanner script.
- **Stable versions only:** User wants non-experimental, production-hardened versions.
- **Treat user as entry-level DE engineer** for explanations, despite actual experience.

## User Preferences

- Wants to understand the *why* behind every choice, not just the *what*
- Treat as entry-level for explanations
- User does git commits manually
- User runs Docker commands manually
- Devin IDE doesn't watch for external file changes — must close/reopen to see OMP edits
- `.venv/` exists (Python 3.13.13), activate with `source .venv/bin/activate`

## Open Questions / Risks

- **Spark Kafka connector:** RESOLVED — `apache/spark:3.5.1` does NOT include Kafka connector. Added 4 JARs to `spark/Dockerfile` (spark-sql-kafka, spark-token-provider, kafka-clients, commons-pool2).
- **`station_id` must be string:** RESOLVED — StringType throughout Spark + Postgres. Works for both UUID and numeric IDs.
- **`is_renting`/`is_returning`/`is_installed` are integers 0/1:** RESOLVED — `CAST(col AS BOOLEAN)` in Spark. Postgres receives proper boolean.
- **Optional scooter fields:** RESOLVED — nullable in Spark schema. `from_json` returns null for missing fields. 1099/1128 non-null.
- **Dead station filtering:** RESOLVED — filter `last_reported > now() - 1 hour` in Spark. Drops 888/2016 (44%) stale stations.
- **Kafka topic:** RESOLVED — `divvy_station_status` recreated at start of Phase 2.4 (3 partitions, replication factor 1).
- **Airflow 3.0 DockerOperator:** RESOLVED — not used in Phase 1 or 2. BashOperator with `docker exec`/`docker run` is simpler and works for both batch and streaming. DockerOperator adds complexity (separate containers, network config, mount management) without benefit for this project.
- **Bitnami images no longer free** — resolved for Spark (`apache/spark:3.5.1`) and Kafka (`confluentinc/cp-kafka:7.6.0`).
- **`docker compose down` (without `-v`) preserves data** — named volumes persist. Use `-v` only to wipe everything. Kafka data volume (`kafka_data`) also persists.
- **WSL2 memory limit:** 8GB via `.wslconfig`. 10 Docker services may need more — monitor.
- **apache/spark PATH:** `spark-submit` not on PATH. Always use `/opt/spark/bin/spark-submit`.
- **DBT profiles.yml has hardcoded password:** In `.gitignore`. For Phase 4, use env vars or secrets manager.
- **DBT run location:** Must run from inside `dbt/` directory.
- **kafka-python 3.0.x API change:** `NoBrokersAvailable` removed. Use `KafkaError` (base class) for catch-all.
- **`KAFKA_NUM_PARTITIONS` env var doesn't work** with Confluent images. Create topics explicitly for custom partition counts.
- **Container name is `chicago-data-pipeline-postgres-1`** not `postgres` — use `docker compose exec postgres` or the full container name.
- **Volume mount path shadowing:** RESOLVED — `./kafka:/opt/airflow/kafka` shadowed the `kafka-python` package. Renamed to `./kafka:/opt/airflow/kafka_scripts`.
- **apache/airflow pip guard:** RESOLVED — image refuses pip as root. Use `pip install` as `USER airflow` (venv at `/home/airflow/.local`).
- **uv pip install --system silent failure:** RESOLVED — uv can't create `kafka` directory in site-packages. Switched to `pip install`.
- **Named volumes mount as root:** RESOLVED — `spark/entrypoint.sh` chowns checkpoint dir before dropping to spark via gosu.
- **Airflow BashOperator kills background processes:** RESOLVED — producer uses `--once` mode (foreground, single poll). For 24/7 streaming, run as separate Docker service (Phase 3).
- **DAG ordering: crime_batch before divvy_stream:** RESOLVED in Phase 3.3 — `crime_batch` now has a `SqlSensor` (`wait_for_stream_data`) that gates `dbt_build` on `raw.station_status` existing. Either DAG can be triggered first; the sensor waits (up to 1hr) for the stream table if needed.

## Chat History Chunks

| File | Topic |
|---|---|
| `2026-07-08/01-project-setup-and-migration.md` | Windows→WSL migration, folder flattening |
| `2026-07-09/01-docker-setup-env-and-init.md` | .env, init.sql, docker-compose.yml creation |
| `2026-07-09/02-docker-compose-and-dockerfiles.md` | Airflow + Spark Dockerfiles |
| `2026-07-09/03-uv-init.md` | uv project mode setup |
| `2026-07-09/04-airflow-upgrade.md` | Airflow 2.8.4 → 3.0.0 upgrade |
| `2026-07-09/05-chat-history-system.md` | Chat-history folder creation |
| `2026-07-09/06-bitnami-to-apache-spark.md` | Bitnami → apache/spark migration |
| `2026-07-09/07-airflow-3-runtime-fixes.md` | 6 runtime fixes to get all services healthy |
| `2026-07-13/01-phase-1.3-spark-batch.md` | Spark batch job: Parquet → clean → Postgres |
| `2026-07-13/02-phase-1.4-dbt-models.md` | DBT project scaffold, staging + marts, dbt-expectations |
| `2026-07-13/03-phase-1.5-airflow-dag.md` | Airflow DAG, dbt Docker image, protobuf conflict |
| `2026-07-13/04-gid-portability-and-socrata-credentials.md` | DOCKER_GID build arg fix, Socrata credentials |
| `2026-07-13/05-phase-1.6-verification.md` | Phase 1 gate: cold start, DAG run, marts verified |
| `2026-07-15/01-phase-2.1-gbfs-data-source.md` | GBFS API exploration, schema analysis, 4 design-changing findings |
| `2026-07-15/02-phase-2.2-kafka.md` | Kafka + Zookeeper Docker services, Confluent images, single-broker overrides |
| `2026-07-15/03-phase-2.3-producer-and-docs.md` | Divvy producer implementation, kafka.md conceptual rewrite with mermaid diagrams |
| `2026-07-15/04-phase-2.4-spark-streaming.md` | Spark Structured Streaming: Kafka connector JARs, divvy_stream.py, foreachBatch→JDBC, checkpoint volume |
| `2026-07-16/01-phase-2.5-dbt-stream-models.md` | DBT stream models: stg_station_status, fact_station_reads, dim_date expansion, 59/59 tests pass |
| `2026-07-16/02-phase-2.6-airflow-stream-dag.md` | Airflow stream DAG: 7-task lifecycle, 9 errors (kafka-python install, volume shadowing, checkpoint perms, BashOperator bg process kill), Phase 2 gate met |
| `2026-07-20/01-phase-3.2-dbt-tests.md` | DBT singular bounds test, record_dbt_results.py recorder, observability schema, Grafana DBT panel wired to live data |
| `2026-07-20/02-phase-3.3-airflow-robustness.md` | SqlSensor race condition fix, on_failure_callback, retries, execution_timeout (Airflow 3.0 removed SLA), Grafana failed-tasks panel |
| `2026-07-20/03-phase-3.4-verification.md` | Broke pipeline 3 ways: stopped producer, injected bad data, failed a task — all caught by observability. Phase 3 gate met. |

---

## Next Session — Phase 5 (CI/CD)

**Goal:** Set up GitHub Actions CI/CD — branch protection, PR checks (ruff + dbt parse + compose validate), versioned releases, image push to GHCR. See `chicago-pipeline-plan.md` sections 5.1–5.6.

### Before starting Phase 5
1. **Commit current work** — Phase 4.4 + 4.8 (BQML) + all doc updates (README, dlt knowledge doc, bigquery-ml knowledge doc, phase-4.8 doc). User commits manually.
2. **Read `chicago-pipeline-plan.md` sections 5.1–5.6** — CI/CD plan.
3. **Prerequisites to confirm:**
   - GitHub repo created and code pushed
   - GitHub Actions enabled
   - GHCR (GitHub Container Registry) access

### Phase 4 is COMPLETE — driving question answered
- Overall Pearson correlation = +0.20 (weak positive — both crime and ridership are higher in busy areas)
- 34.8M Divvy trips ingested, 8.6M crime rows from public dataset, 1.5M station-day rows in analytics mart
- 67/67 DBT tests pass, partition pruning verified (97.8% bytes saved)
- Grafana scatter plot + correlation gauge live

### Stretch goals
- ~~BigQuery ML: `CREATE MODEL mart.crime_ridership_model OPTIONS(model_type='linear_reg')` — predict ridership from crime count + temporal features~~ **DONE (Phase 4.8)** — crime coefficient +1.45, in-sample R²=0.434, confirms positive crime-ridership relationship
- Control for confounding variables (population density, day of week, seasonality) in correlation analysis
