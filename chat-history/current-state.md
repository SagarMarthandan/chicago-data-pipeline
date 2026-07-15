# Current State — Handoff Document

> **Read this first in a new session.** This file is the handoff: current state, active decisions, and next steps. Last updated: 2026-07-15 (end of session — Phase 2.1–2.3 complete, Phase 2.4 next).

---

## Project

Chicago Crime + Divvy Bike-Share data engineering pipeline. A learning project that answers: *Does crime near a Divvy station affect ridership?*

- **Repo:** `~/chicago-data-pipeline/` (WSL, Ubuntu on Windows 10)
- **Git:** initialized on `main`, no commits yet (user commits manually)
- **Phase:** 1 COMPLETE. Phase 2 IN PROGRESS (2.1 GBFS exploration ✅, 2.2 Kafka+Zookeeper ✅, 2.3 Producer ✅, 2.4 Spark Streaming NEXT). Phase 3, 4 locked.
- **AI mode:** AI-writes-code (user said "you write it" — explicit mode switch from Socratic)

## Tech Stack

| Layer | Tool | Phase |
|---|---|---|
| Warehouse | Postgres (local) → BigQuery (cloud) | 1 → 4 |
| Batch | Spark DataFrames | 1 ✅ |
| Streaming | Kafka + Spark Structured Streaming | 2 (in progress) |
| Transformation | DBT | 1+ ✅ |
| Orchestration | Airflow | 1+ ✅ |
| Observability | Grafana | 3 (locked) |
| Cloud | Terraform + Airbyte | 4 (locked) |

## Current Infrastructure

### Docker Compose — 10 services (7 Phase 1 + 2 Phase 2 + 1 build-only)

| Service | Image | Status |
|---|---|---|
| postgres | `postgres:16-alpine` | **healthy** — 3 schemas (raw, staging, mart) |
| spark-master | `apache/spark:3.5.1` + JDBC | **healthy** — UI on port 8180, now has `KAFKA_BOOTSTRAP_SERVERS` env |
| spark-worker | same as master | **running** — UI on port 8081, now has `KAFKA_BOOTSTRAP_SERVERS` env |
| airflow-init | `apache/airflow:3.0.0-python3.11` | **exited (0)** — migrations complete |
| airflow-webserver | same | **healthy** — UI on port 8080 (admin/admin) |
| airflow-scheduler | same | **running** — heartbeat active |
| airflow-dag-processor | same | **running** — parses + serializes DAGs |
| zookeeper | `confluentinc/cp-zookeeper:7.6.0` | **healthy** — port 2181 (internal) |
| kafka | `confluentinc/cp-kafka:7.6.0` | **healthy** — ports 9092 (internal) + 29092 (host) |
| dbt-build | `python:3.11-slim` + dbt | build-only (never runs, exists for `docker compose build`) |

**Note:** At end of session, only zookeeper + kafka are running (Phase 1 services were stopped). Start all with `docker compose up -d`.

### URLs
- **Airflow UI:** http://localhost:8080 (admin / admin)
- **Spark Master UI:** http://localhost:8180
- **Spark Worker UI:** http://localhost:8081
- **Postgres:** localhost:5432 (user: chicago, db: chicago_analytics)
- **Kafka (host):** localhost:29092
- **Kafka (Docker network):** kafka:9092

### Key Architecture Decisions (Phase 1 + Phase 2)
- **3 Postgres schemas:** `raw`, `staging`, `mart` (no `intermediate`)
- **Two databases in one Postgres:** `chicago_analytics` (warehouse) + `airflow_metadata` (Airflow internal)
- **Airflow 3.0.0** (upgraded from 2.8.4 — 2.x is EOL since April 2026)
- **SimpleAuthManager** (Airflow 3.0 default auth — users via env vars + passwords.json)
- **LocalExecutor** (parallelism without Redis/RabbitMQ)
- **JDBC driver baked into Spark image** (not `--packages` at runtime)
- **Spark UI on port 8180** (8080 conflicts with Airflow)
- **uv init (project mode)** for host Python — `pyproject.toml` + `uv.lock`
- **`uv pip install --system`** in Docker containers (not `uv sync`)
- **Socrata resource ID is `ijzp-q8t2`** (NOT `ijzp-q4t2` — the plan had a typo)
- **Confluent Platform 7.6.0** for Kafka (not Bitnami — no longer free; not `latest` — pinned)
- **Zookeeper mode** (not KRaft — more educational, traditional setup)
- **Two Kafka listeners:** `kafka:9092` (Docker network) + `localhost:29092` (host testing)
- **3 partitions for `divvy_station_status`** — station_id as key → same station → same partition
- **Single-broker overrides:** replication factor 1 for all internal topics
- **Explicit topic creation** (not auto-create) for custom partition counts — `KAFKA_NUM_PARTITIONS` env var doesn't work with Confluent images

## Phase 1 — COMPLETE (1.1–1.6)

All Phase 1 sub-phases verified end-to-end. Cold start → DAG run → 4 tasks succeed → marts queryable (263,394 fact rows). See `docs/phases/phase-1.*.md` for details.

### Phase 1 Key Files
- `ingestion/download_crime.py` — Socrata API → Parquet (263K rows)
- `spark/jobs/crime_batch.py` — Parquet → clean → Postgres `raw.crime_events`
- `dbt/` — staging + marts (dim_date, dim_community_area, dim_crime_type, fact_crime_events)
- `airflow/dags/crime_batch_dag.py` — 4 tasks: download → clear_dbt_schemas → spark → dbt_build
- `dbt/Dockerfile` — separate dbt image (protobuf conflict with Airflow)

## Phase 2 — IN PROGRESS

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
- `./kafka:/opt/airflow/kafka` volume mount added to Airflow (for Phase 2.6)
- Verified: 2,016 messages/poll, 3 partitions (720/661/635 distribution), real Divvy data confirmed, continuous mode + graceful shutdown
- **2 errors hit:**
  1. `NoBrokersAvailable` removed in kafka-python 3.0.x → catch `KafkaError` instead
  2. Auto-created topic had 1 partition → `KAFKA_NUM_PARTITIONS` env var doesn't work with Confluent image → explicit `kafka-topics --create --partitions 3`
- Topic `divvy_station_status` was deleted at end of testing — needs to be recreated before Phase 2.4

### Phase 2 Remaining Sub-phases

| Sub-phase | Status | What to build |
|---|---|---|
| 2.4 Spark Structured Streaming | **NEXT** | `spark/jobs/divvy_stream.py` — `readStream` from Kafka → parse JSON → `foreachBatch` writes to `raw.station_status` in Postgres |
| 2.5 DBT models for stream | Not started | `stg_station_status` (staging view) + `fact_station_reads` (one row per station poll) |
| 2.6 Airflow DAG for stream | Not started | `divvy_stream_dag.py` — starts/monitors producer + streaming job |

### Phase 2.4 — What's Needed Next
- **Before starting:** Recreate Kafka topic: `docker compose exec kafka kafka-topics --create --topic divvy_station_status --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092`
- **Spark streaming job** (`spark/jobs/divvy_stream.py`):
  - `readStream.format("kafka")` → subscribe to `divvy_station_status`
  - `from_json()` to parse the JSON value using a schema (must handle: station_id as string, is_* as int→boolean cast, optional scooter fields as nullable)
  - `foreachBatch` to write to Postgres `raw.station_status` via JDBC (no native streaming JDBC sink)
  - Checkpoint location for fault tolerance
  - 60s trigger (matches producer poll interval)
- **Key lessons from plan:** `foreachBatch` is the bridge between Structured Streaming and JDBC. Learn why.
- **Spark needs Kafka connector JAR** — check if `apache/spark:3.5.1` includes it or if we need to add it to the Spark Dockerfile
- **Run command:** `docker compose exec spark-master /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/divvy_stream.py`
- **Verification:** `SELECT COUNT(*) FROM raw.station_status` grows over time

## Files Created (full repo structure)

```
~/chicago-data-pipeline/
├── .env.example
├── .gitignore
├── .vscode/settings.json
├── AGENTS.md
├── README.md
├── docker-compose.yml        ← 10 services (Phase 1 + Phase 2), YAML anchors
├── chicago-pipeline-plan.md
├── init.sql
├── pyproject.toml            ← now includes kafka-python
├── uv.lock
├── airflow/
│   ├── Dockerfile
│   ├── passwords.json
│   ├── requirements.txt      ← now includes kafka-python
│   ├── dags/
│   │   └── crime_batch_dag.py
│   └── dbt_profiles/profiles.yml
├── spark/
│   ├── Dockerfile            ← apache/spark:3.5.1 + PostgreSQL JDBC
│   └── jobs/
│       └── crime_batch.py
├── ingestion/
│   └── download_crime.py
├── kafka/                    ← NEW (Phase 2.3)
│   └── producers/
│       └── divvy_producer.py ← GBFS → Kafka producer
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   ├── models/
│   ├── packages.yml
│   └── seeds/community_areas.csv
├── data/
│   └── raw/crime/crime_2023.parquet
├── chat-history/
│   └── current-state.md      ← THIS FILE
└── docs/
    ├── knowledge/
    │   ├── index.md
    │   ├── wsl.md, uv.md, docker-compose.md, postgres.md, dbt.md, spark.md
    │   ├── architecture.md   ← 9 sections (now includes Kafka + Zookeeper)
    │   ├── kafka.md          ← REWRITTEN: full concepts + 8 mermaid diagrams
    │   ├── airflow.md
    │   ├── git.md
    │   ├── data-sources.md   ← expanded with full GBFS schema (Phase 2.1)
    │   └── mermaid-syntax.md
    ├── learning-protocol.md
    ├── operations-performed.md ← TOC + entries through Phase 2.3
    ├── phases/
    │   ├── README.md         ← index updated through Phase 2.3
    │   ├── phase-1.1-docker.md through phase-1.6-verification.md
    │   ├── phase-2.1-gbfs-data-source.md
    │   ├── phase-2.2-kafka.md
    │   └── phase-2.3-divvy-producer.md
    └── conventions/
        ├── airflow.md, dbt.md, docker.md, spark.md
```

## Next Steps

1. **Phase 2.4: Spark Structured Streaming** — `spark/jobs/divvy_stream.py`
   - Recreate Kafka topic first (deleted at end of Phase 2.3 testing)
   - Check if Spark image needs Kafka connector JAR
   - Build streaming job: Kafka → parse JSON → `foreachBatch` → Postgres `raw.station_status`
   - Verification: row count in `raw.station_status` grows over time
2. **Phase 2.5: DBT models** — `stg_station_status` + `fact_station_reads`
3. **Phase 2.6: Airflow DAG** — `divvy_stream_dag.py` (start/monitor producer + streaming)
4. **Phase 2 gate:** Done when `docker compose up` includes Kafka, producer running, Spark streaming writes to Postgres, DBT builds `fact_station_reads`, can query "avg bikes available at station X over last hour"

## Active Constraints

- **Phase gates:** Phase 1 COMPLETE. Phase 2 in progress (2.1–2.3 done, 2.4 next). Phase 3 locked until Phase 2 works. Do NOT skip ahead.
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

- **Spark Kafka connector:** `apache/spark:3.5.1` may not include the Kafka connector JAR (`spark-sql-kafka-0-10_2.12`). Need to check and potentially add to `spark/Dockerfile` (like we did for PostgreSQL JDBC).
- **`station_id` must be string:** The plan's DBT model had `station_id::bigint` — will fail on UUID-format IDs. Must use string throughout.
- **`is_renting`/`is_returning`/`is_installed` are integers 0/1:** Need `CAST(col AS BOOLEAN)` in Spark/DBT.
- **Optional scooter fields:** `num_scooters_available`/`num_scooters_unavailable` not in all stations. Spark schema must use nullable fields.
- **Dead station filtering:** One station had `last_reported: 86400` (1970). Filter in Spark or DBT.
- **Kafka topic deleted:** `divvy_station_status` was deleted at end of Phase 2.3 testing. Must recreate before Phase 2.4: `docker compose exec kafka kafka-topics --create --topic divvy_station_status --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092`
- **Airflow 3.0 DockerOperator:** Not used in Phase 1 — BashOperator with `docker exec`/`docker run` was simpler. May revisit in Phase 2.6.
- **Bitnami images no longer free** — resolved for Spark (`apache/spark:3.5.1`) and Kafka (`confluentinc/cp-kafka:7.6.0`).
- **`docker compose down` (without `-v`) preserves data** — named volumes persist. Use `-v` only to wipe everything. Kafka data volume (`kafka_data`) also persists.
- **WSL2 memory limit:** 8GB via `.wslconfig`. 10 Docker services may need more — monitor.
- **apache/spark PATH:** `spark-submit` not on PATH. Always use `/opt/spark/bin/spark-submit`.
- **DBT profiles.yml has hardcoded password:** In `.gitignore`. For Phase 4, use env vars or secrets manager.
- **DBT run location:** Must run from inside `dbt/` directory.
- **kafka-python 3.0.x API change:** `NoBrokersAvailable` removed. Use `KafkaError` (base class) for catch-all.
- **`KAFKA_NUM_PARTITIONS` env var doesn't work** with Confluent images. Create topics explicitly for custom partition counts.

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


