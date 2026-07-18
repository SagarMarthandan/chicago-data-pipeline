# Current State — Handoff Document

> **Read this first in a new session.** This file is the handoff: current state, active decisions, and next steps. Last updated: 2026-07-18 (end of session — Phase 3.1 Grafana COMPLETE, Phase 3.2 DBT tests NEXT).

---

## Project

Chicago Crime + Divvy Bike-Share data engineering pipeline. A learning project that answers: *Does crime near a Divvy station affect ridership?*

- **Repo:** `~/chicago-data-pipeline/` (WSL, Ubuntu on Windows 10)
- **Git:** initialized on `main`, no commits yet (user commits manually)
- **Phase:** 1 COMPLETE. Phase 2 COMPLETE (2.1–2.6). Phase 3 STARTED (3.1 Grafana ✅, 3.2 DBT tests NEXT, 3.3 Airflow SLAs LOCKED, 3.4 Verification LOCKED). Phase 4 locked. Phase 5 locked (plan written in chicago-pipeline-plan.md).
- **AI mode:** AI-writes-code (user said "you write it" — explicit mode switch from Socratic)

## Tech Stack

| Layer | Tool | Phase |
|---|---|---|
| Warehouse | Postgres (local) → BigQuery (cloud) | 1 → 4 |
| Batch | Spark DataFrames | 1 ✅ |
| Streaming | Kafka + Spark Structured Streaming | 2 ✅ |
| Transformation | DBT | 1+ ✅ |
| Orchestration | Airflow | 1+ ✅ |
| Observability | Grafana | 3.1 ✅ (3.2–3.4 in progress) |
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

**Note:** At end of session, all services running. `raw.crime_events` has 263,395 rows. `raw.station_status` has 2,001 rows (from divvy_stream DAG run). `mart.fact_station_reads` has 2,001 rows, 1,125 unique stations. `mart.fact_crime_events` has 263,395 rows. Start all services with `docker compose up -d`.

### URLs
- **Airflow UI:** http://localhost:8080 (admin / admin)
- **Spark Master UI:** http://localhost:8180
- **Spark Worker UI:** http://localhost:8081
- **Postgres:** localhost:5432 (user: chicago, db: chicago_analytics)
- **Kafka (host):** localhost:29092
- **Kafka (Docker network):** kafka:9092
- **Grafana UI:** http://localhost:3000 (admin / admin) — anonymous Viewer access enabled

### Key Architecture Decisions (Phase 1 + Phase 2)
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

## Phase 3 — IN PROGRESS (3.1 done, 3.2 next)

### Phase 3.1 — Grafana (COMPLETE)
- Added `grafana` service to `docker-compose.yml` (`grafana/grafana:12.4.0`, port 3000, `grafana_data` volume, anonymous Viewer access)
- Two Postgres datasources provisioned via `grafana/provisioning/datasources/postgres.yml`:
  - `chicago-analytics` (uid: `chicago-analytics`) → `chicago_analytics` database (raw + mart schemas)
  - `airflow-metadata` (uid: `airflow-metadata`) → `airflow_metadata` database (dag_run, task_instance)
  - **Why two:** Postgres databases are isolated — can't cross-query without `postgres_fdw`. One datasource per DB.
- Two dashboards provisioned via `grafana/provisioning/dashboards/dashboards.yml`:
  - `pipeline_health.json` (10 panels): row counts, stream ingestion rate, stream freshness, latest Kafka msg, DBT tests (static placeholder), Airflow DAG runs + task instances
  - `crime_divvy_analysis.json` (6 panels): top community areas by crime, crime types, avg vehicles per station, station availability heatmap, crime-vs-ridership proxy (THE DRIVING QUESTION), crime heatmap
- **4 errors hit:** (1) Go-template `{{.VAR}}` syntax in datasource YAML → Grafana uses `$VAR`; (2) env vars not in container after `restart` → need `up -d` to recreate; (3) cross-database query failed → added second datasource; (4) `jsonData.database` missing → browser panels showed "No data" despite API queries working (Grafana 12.4's Postgres plugin reads DB name from `jsonData.database`, not top-level `database:` field)
- **DAG run order: stream first, then crime batch** — `crime_batch`'s `dbt_build` builds ALL models including `stg_station_status` (depends on `raw.station_status` from `divvy_stream`). Run `divvy_stream` first to eliminate the race condition. Pre-existing design issue to address in Phase 3.3.
- Verified: Grafana healthy (v12.4.0), both datasources + dashboards loaded, all 16 panel queries return status 200 against live data (263,401 crime rows, 1,130 station reads, Airflow DAG runs). Browser rendering verified (not just API).

## Files Created (full repo structure)

```
~/chicago-data-pipeline/
├── .env.example
├── .gitignore
├── .vscode/settings.json
├── AGENTS.md
├── README.md
├── docker-compose.yml        ← 11 services + spark_checkpoints + grafana_data volumes (Phase 1 + 2 + 3.1), YAML anchors
├── airflow/
│   ├── Dockerfile
│   ├── passwords.json
│   ├── requirements.txt      ← now includes kafka-python
│   ├── dags/
│   │   ├── crime_batch_dag.py
│   │   └── divvy_stream_dag.py ← Phase 2.6 — streaming lifecycle DAG
│   └── dbt_profiles/profiles.yml
├── spark/
│   ├── Dockerfile            ← apache/spark:3.5.1 + JDBC + Kafka connector + entrypoint (Phase 2.6)
│   ├── entrypoint.sh         ← Phase 2.6 — chowns checkpoint volume, drops to spark via gosu
│   └── jobs/
│       ├── crime_batch.py
│       └── divvy_stream.py   ← Phase 2.4 — Structured Streaming consumer
├── ingestion/
│   └── download_crime.py
├── grafana/                   ← Phase 3.1
│   ├── provisioning/
│   │   ├── datasources/postgres.yml  ← 2 datasources (chicago-analytics + airflow-metadata)
│   │   └── dashboards/dashboards.yml ← dashboard provider
│   └── dashboards/
│       ├── pipeline_health.json      ← 10-panel pipeline health dashboard
│       └── crime_divvy_analysis.json ← 6-panel analysis dashboard
├── kafka/                    ← Phase 2.3
│   └── producers/
│       └── divvy_producer.py ← GBFS → Kafka producer
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_crime_events.sql
│   │   │   ├── stg_station_status.sql  ← NEW (Phase 2.5)
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── dim_date.sql             ← MODIFIED (Phase 2.5 — spans both sources)
│   │       ├── dim_community_area.sql
│   │       ├── dim_crime_type.sql
│   │       ├── fact_crime_events.sql
│   │       ├── fact_station_reads.sql   ← NEW (Phase 2.5)
│   │       └── schema.yml
│   ├── packages.yml
│   └── seeds/community_areas.csv
├── data/
│   └── raw/crime/crime_2023.parquet
├── chat-history/
│   └── current-state.md      ← THIS FILE
└── docs/
    ├── knowledge/
    │   ├── data-sources.md   ← expanded with full GBFS schema (Phase 2.1)
    │   ├── grafana.md        ← Phase 3.1 — comprehensive reference: concepts, provisioning, env var gotchas, jsonData.database deep dive, DAG run order, 10 common mistakes, 8 mermaid diagrams
    │   ├── wsl.md, uv.md, docker-compose.md, postgres.md, dbt.md, spark.md
    │   ├── architecture.md   ← 10 sections (now includes Kafka + Zookeeper + Spark Streaming)
    │   ├── kafka.md          ← full concepts + 8 mermaid diagrams + Spark consumer + checkpointing
    │   ├── airflow.md
    │   ├── git.md
    │   ├── data-sources.md   ← expanded with full GBFS schema (Phase 2.1)
    │   └── mermaid-syntax.md
    ├── learning-protocol.md
    ├── operations-performed.md ← TOC + entries through Phase 2.6
    ├── phases/
    │   └── phase-2.6-airflow-stream-dag.md  ← NEW (Phase 2.6)
    │   └── phase-3.1-grafana.md             ← NEW (Phase 3.1)
    │   ├── phase-1.1-docker.md through phase-1.6-verification.md
    │   ├── phase-2.1-gbfs-data-source.md
    │   ├── phase-2.2-kafka.md
    │   ├── phase-2.3-divvy-producer.md
    │   ├── phase-2.4-spark-streaming.md
    │   ├── phase-2.5-dbt-stream-models.md
    │   └── phase-2.6-airflow-stream-dag.md  ← NEW (Phase 2.6)
    └── conventions/
        ├── airflow.md, dbt.md, docker.md, spark.md
```

## Next Steps

1. **Phase 3: Observability** — Grafana dashboards + DBT tests + Airflow SLAs
   - **3.1 Grafana: COMPLETE** ✅ — grafana service + 2 datasources + 2 dashboards (Pipeline Health + Crime + Divvy Analysis). All 16 panel queries verified against live data.
   - **3.2 DBT tests: NEXT** — Add custom singular test `assert_crime_in_chicago_bounds.sql` + stream not_null tests. Wire the DBT tests Grafana panel to actual test results (currently static placeholder).
   - **3.3 Airflow robustness: LOCKED** — Add retries, SLAs, freshness sensor, on_failure_callback.
   - **3.4 Verification: LOCKED** — Break the pipeline and confirm observability catches it.
   - Requires: Phase 2 complete (streaming pipeline works end-to-end) ✅ met
2. **Phase 4: Cloud** — Terraform → BigQuery + Airbyte
   - Requires: Phase 3 complete (observability in place before migrating to cloud)
   - New: Terraform infrastructure, BigQuery warehouse, Airbyte ingestion
3. **Phase 5: CI/CD** — GitHub Actions + GHCR
   - Requires: Phase 4 complete
   - New: Branch protection (dev/prod), PR checks (ruff + dbt parse + compose validate), versioned releases (semantic versioning), image push to GHCR
   - **Future task (after Phase 5):** Generate 50–100 interview questions covering the full pipeline — architecture decisions, error debugging, tool tradeoffs, production readiness. User must be able to answer all from memory.
   - **Future task (after Phase 5):** Comprehensive documentation restructuring — reorganize all docs for portfolio readability, consolidate redundant content, ensure consistent formatting across changelog/operations/phases/knowledge. Discuss approach when we get there.
   - Plan added to `chicago-pipeline-plan.md` (sections 5.1–5.6)

- **Phase gates:** Phase 1 COMPLETE. Phase 2 COMPLETE (2.1–2.6 done). Phase 3 IN PROGRESS (3.1 Grafana done, 3.2 DBT tests next). Phase 4 locked. Phase 5 locked. Do NOT skip ahead.
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
- **DAG ordering: crime_batch before divvy_stream:** `dim_date.sql` UNION ALLs min/max dates from both `stg_crime_events` and `stg_station_status`. Both DAGs run `dbt build` (all models), so each needs both raw tables. On cold start: crime_batch's `dbt_build` fails on `stg_station_status` (table doesn't exist yet) — expected, non-blocking, all crime models build fine. Then divvy_stream's `dbt_build` succeeds (both raw tables exist). Fix for Phase 3: split `dbt build` by selector per DAG, or add a separate `dim_date` finalize DAG.

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
