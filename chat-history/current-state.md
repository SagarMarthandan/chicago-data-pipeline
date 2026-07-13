# Current State — Handoff Document

> **Read this first in a new session.** This file is the handoff: current state, active decisions, and next steps. Last updated: 2026-07-13 (end of session — Phase 1 complete).

---

## Project

Chicago Crime + Divvy Bike-Share data engineering pipeline. A learning project that answers: *Does crime near a Divvy station affect ridership?*

- **Repo:** `~/chicago-data-pipeline/` (WSL, Ubuntu on Windows 10)
- **Git:** initialized on `main`, no commits yet (user commits manually)
- **Phase:** 1 (Batch Foundation) — COMPLETE (1.1 Docker + 1.2 Ingestion + 1.3 Spark batch + 1.4 DBT models + 1.5 Airflow DAG + 1.6 Verification). **Phase 2 unlocked.**
- **AI mode:** AI-writes-code (user said "you write it" — explicit mode switch from Socratic)

## Tech Stack

| Layer | Tool | Phase |
|---|---|---|
| Warehouse | Postgres (local) → BigQuery (cloud) | 1 → 4 |
| Batch | Spark DataFrames | 1 |
| Streaming | Kafka + Spark Structured Streaming | 2 (locked) |
| Transformation | DBT | 1+ |
| Orchestration | Airflow | 1+ |
| Observability | Grafana | 3 (locked) |
| Cloud | Terraform + Airbyte | 4 (locked) |

## Current Infrastructure (ALL RUNNING AND VERIFIED)

### Docker Compose — 7 services
| Service | Image | Status |
|---|---|---|
| postgres | `postgres:16-alpine` | **healthy** — 3 schemas (raw, staging, mart) confirmed |
| spark-master | `apache/spark:3.5.1` + JDBC driver | **healthy** — UI on port 8180 |
| spark-worker | same as master | **running** — UI on port 8081 |
| airflow-init | `apache/airflow:3.0.0-python3.11` | **exited (0)** — migrations complete |
| airflow-webserver | same | **healthy** — UI on port 8080 (admin/admin) |
| airflow-scheduler | same | **running** — heartbeat active |
| airflow-dag-processor | same | **running** — parses + serializes DAGs (Airflow 3.0 separates this from scheduler) |

### URLs
- **Airflow UI:** http://localhost:8080 (admin / admin)
- **Spark Master UI:** http://localhost:8180
- **Spark Worker UI:** http://localhost:8081
- **Postgres:** localhost:5432 (user: chicago, db: chicago_analytics)

### Key Architecture Decisions
- **3 Postgres schemas:** `raw`, `staging`, `mart` (no `intermediate`)
- **Two databases in one Postgres:** `chicago_analytics` (warehouse) + `airflow_metadata` (Airflow internal)
- **Airflow 3.0.0** (upgraded from 2.8.4 — 2.x is EOL since April 2026)
- **SimpleAuthManager** (Airflow 3.0 default auth — users via env vars + passwords.json, NOT `airflow users create`)
- **LocalExecutor** (parallelism without Redis/RabbitMQ)
- **JDBC driver baked into Spark image** (not `--packages` at runtime)
- **Spark UI on port 8180** (8080 conflicts with Airflow)
- **uv init (project mode)** for host Python — `pyproject.toml` + `uv.lock`
- **`uv pip install --system`** in Docker containers (not `uv sync`)
- **Socrata resource ID is `ijzp-q8t2`** (NOT `ijzp-q4t2` — the plan had a typo)

### Phase 1.2 — Ingestion (COMPLETE)
- `ingestion/download_crime.py` — downloads Chicago crime data from Socrata API, paginates, cleans API quirks, writes Parquet
- `data/raw/crime/crime_2023.parquet` — 263,393 rows, 21 columns, 11.5 MB
- Spark can read the Parquet from inside containers (`./data:/opt/spark/data` mount added)
- Socrata app token configured — all 4 credentials in `.env`, passed to Airflow container via docker-compose. Rate limit 10K req/hr.

### Phase 1.3 — Spark Batch Job (COMPLETE)
- `spark/jobs/crime_batch.py` — reads Parquet → cleans → writes to Postgres `raw.crime_events` via JDBC
- Cleaning: cast `id` to long, parse `date`/`updated_on` to timestamp, uppercase `primary_type`, cast `community_area` to int, dedup on `id`, drop null ids
- `raw.crime_events` table: 263,393 rows, 21 columns — verified in Postgres
- Idempotent: `mode("overwrite")` — safe to re-run, replaces whole table
- `docker-compose.yml` updated: Postgres env vars added to both `spark-master` and `spark-worker`
- Run command: `docker compose exec spark-master /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/crime_batch.py`
- **Note:** `spark-submit` is NOT on PATH in apache/spark image — use full path `/opt/spark/bin/spark-submit`

### Phase 1.4 — DBT Models (COMPLETE)
- `dbt/` project: `dbt_project.yml`, `profiles.yml`, macros, models, seeds
- `macros/try_cast.sql` — warehouse-portable cast (Postgres `::` vs BigQuery `SAFE_CAST`)
- `macros/generate_schema_name.sql` — overrides DBT schema concatenation (models go to `staging`/`mart`, not `staging_staging`/`staging_mart`)
- `models/staging/stg_crime_events.sql` — view: rename, cast, dedup on id via `DISTINCT ON`
- `models/marts/` — `dim_date` (365 rows), `dim_community_area` (77 rows), `dim_crime_type` (323 rows), `fact_crime_events` (263,393 rows)
- `seeds/community_areas.csv` — 77 community areas from Chicago Data Portal (`igwz-8jzy`)
- `models/staging/schema.yml` + `models/marts/schema.yml` — 31 tests total: 20 standard (unique, not_null, relationships) + 11 dbt-expectations (range bounds, value sets)
- `packages.yml` — `metaplane/dbt_expectations` 0.10.10 (Great Expectations macros for dbt)
- `.vscode/settings.json` — `dbt.allowListFolders: ["dbt"]`, `dbt.dbtPythonPathOverride: .venv/bin/python` for dbt Power User extension
- `.gitignore` updated — `!dbt/seeds/*.csv` (seed must be committable), `!.vscode/settings.json` (extension config shared), `dbt/profiles.yml` ignored (has password)
- `~/.dbt/profiles.yml` — copy of dbt/profiles.yml for dbt Power User extension (default location)
- DBT installed: dbt-core 1.11.12 + dbt-postgres 1.10.2 (via `uv sync`)
- Run: `cd dbt && dbt build --profiles-dir .` (37/37 PASS)
- **Key lessons:** (1) DBT's default `generate_schema_name` concatenates — override it. (2) `expect_column_values_to_be_in_set` fails on Postgres BOOLEAN — use `not_null`. (3) dbt Power User needs `dbt.allowListFolders` for subdirectory projects.

- `airflow/dags/crime_batch_dag.py` — 4 tasks: download_crime → clear_dbt_schemas → spark_crime_batch → dbt_build
  - download_crime: BashOperator runs `python /opt/airflow/ingestion/download_crime.py --year 2023`
  - clear_dbt_schemas: BashOperator drops staging + mart schemas (CASCADE) so Spark can overwrite raw.crime_events
  - spark_crime_batch: BashOperator runs `docker exec <spark-master> /opt/spark/bin/spark-submit ...`
  - dbt_build: BashOperator runs `docker run --rm --volumes-from $HOSTNAME chicago-data-pipeline-dbt:latest dbt build ...`
  - `schedule=None` (manual trigger), `max_active_runs=1`, `retries=1`
- `dbt/Dockerfile` — separate dbt image (dbt-core 1.11 + dbt-postgres 1.10 on python:3.11-slim). Needed because dbt-core 1.11 requires protobuf >=6.0 which conflicts with Airflow 3.0's protobuf 4.x.
- `airflow/dbt_profiles/profiles.yml` — dbt profiles for Airflow container (`host: postgres`, credentials via `env_var()`)
- `docker-compose.yml` updated: ingestion/dbt/dbt_profiles mounts, Postgres env vars for Airflow, execution API URL, shared secrets, dbt-build service
- `airflow/Dockerfile` updated: docker group (GID configurable via DOCKER_GID build arg) for docker.sock, ingestion deps (pandas, pyarrow, requests, python-dotenv)
- `.env` / `.env.example` updated: `AIRFLOW__CORE__INTERNAL_API_SECRET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__WEBSERVER__SECRET_KEY`
- `ingestion/download_crime.py` — fixed docstring resource ID typo, increased API timeout 60s → 120s
- Verified: DAG run succeeded (download 117s + spark 32s + dbt 11s = 137s total), marts queryable (263,394 fact rows)
- **Key lessons:** (1) Airflow 3.0 `@manual` is invalid — use `schedule=None`. (2) Scheduler needs `EXECUTION_API_SERVER_URL` pointing to webserver service name. (3) JWT + webserver secrets must be shared between containers. (4) dbt + Airflow can't share an image (protobuf conflict). (5) docker.sock GID must match host. (6) `--volumes-from` doesn't pass env vars — use `-e`.

### Phase 1.6 — Verification (COMPLETE)
- Cold-started all 7 services from `docker compose down` + `up`
- DAG run `manual__2026-07-13T14:11:11...9MkcEDt7` — all 4 tasks succeeded (163s total)
- Marts verified: dim_date=365, dim_community_area=77, dim_crime_type=323, fact_crime_events=263,394 (matches raw)
- Added `clear_dbt_schemas` task to DAG — drops staging + mart schemas before Spark runs (fixes `cannot drop table raw.crime_events because other objects depend on it`)
- Added `airflow-dag-processor` service to docker-compose — Airflow 3.0 separates DAG processing from scheduler
- **Key lessons:** (1) DBT views block Spark's overwrite mode — drop derived schemas first. (2) Airflow 3.0 requires a separate dag-processor service for DAG serialization.

### Files Created
```
~/chicago-data-pipeline/
├── .env.example              ← env var template (Airflow 3.0 SimpleAuthManager config)
├── .gitignore
├── .vscode/
│   └── settings.json         ← dbt Power User config (allowListFolders, Python path)
├── AGENTS.md                 ← AI assistant rules (14 rules, read first)
├── README.md                 ← 3 Mermaid diagrams + progress table
├── docker-compose.yml        ← 7 services, YAML anchors, Airflow 3.0, data mount, Spark env vars, dag-processor
├── chicago-pipeline-plan.md  ← full phased plan
├── init.sql                  ← 3 schemas + airflow user + airflow_metadata DB
├── pyproject.toml            ← uv project mode
├── uv.lock                   ← reproducible installs
├── airflow/
│   ├── Dockerfile            ← Airflow 3.0.0 + docker CLI + docker group (DOCKER_GID build arg) + ingestion deps
│   ├── passwords.json        ← SimpleAuthManager: {"admin": "admin"} (chmod 666)
│   ├── requirements.txt      ← postgres + docker providers + ingestion deps (pandas, pyarrow, requests, python-dotenv)
│   │   └── crime_batch_dag.py ← Phase 1.5+1.6 DAG: download → clear_dbt_schemas → spark → dbt_build
│   └── dbt_profiles/
│       └── profiles.yml      ← dbt profiles for Airflow container (host: postgres, env_var credentials)
├── spark/
│   ├── Dockerfile            ← apache/spark:3.5.1 + PostgreSQL JDBC
│   └── jobs/
│       └── crime_batch.py    ← Spark batch ETL: Parquet → clean → Postgres (Phase 1.3)
├── ingestion/
│   └── download_crime.py     ← Socrata API → Parquet (Phase 1.2)
├── dbt/                      ← DBT transformation project (Phase 1.4)
│   ├── Dockerfile             ← dbt container image (separate from Airflow — protobuf conflict)
│   ├── dbt_project.yml       ← model config, materialization, schema mapping
│   ├── profiles.yml          ← Postgres connection (NOT committed to git)
│   ├── macros/
│   │   ├── try_cast.sql      ← warehouse-portable cast macro
│   │   └── generate_schema_name.sql ← override schema concatenation
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_crime_events.sql ← view: rename, cast, dedup
│   │   │   └── schema.yml    ← source definition + staging tests (unique, not_null, dbt-expectations)
│   │   └── marts/
│   │       ├── dim_date.sql
│   │       ├── dim_community_area.sql
│   │       ├── dim_crime_type.sql
│   │       ├── fact_crime_events.sql
│   │       └── schema.yml    ← 31 data tests (20 standard + 11 dbt-expectations)
│   ├── packages.yml          ← dbt-expectations 0.10.10 (Great Expectations macros)
│   ├── package-lock.yml      ← auto-generated lock file
│   └── seeds/
│       └── community_areas.csv ← 77 community areas from Chicago Data Portal
├── data/                     ← Parquet output (gitignored)
│   └── raw/crime/crime_2023.parquet ← 263K rows, 11.5 MB
├── chat-history/             ← conversation reference (read current-state.md first)
└── docs/
    ├── knowledge/            ← reference (one file per topic, see index.md)
    │   ├── index.md          ← navigation table + relationship to other docs
    │   ├── wsl.md
    │   ├── uv.md
    │   ├── docker-compose.md
    │   ├── architecture.md   ← How Everything Connects (9 mermaid diagrams)
    │   ├── postgres.md
    │   ├── dbt.md
    │   ├── spark.md
    │   ├── kafka.md
    │   ├── airflow.md        ← Airflow 2.x vs 3.x comparison (9 subsections)
    │   ├── git.md
    │   ├── data-sources.md   ← Socrata + Divvy API reference
    │   └── mermaid-syntax.md ← quoting rules + scanner script
    ├── learning-protocol.md  ← Socratic mode rules
    ├── operations-performed.md ← audit trail (with TOC)
    ├── phases/               ← phase-completion docs (one per sub-phase)
    │   ├── README.md         ← explains the system
    │   ├── phase-1.1-docker.md ← Phase 1.1 snapshot (complete)
    │   ├── phase-1.2-ingestion.md ← Phase 1.2 snapshot (complete)
    │   ├── phase-1.3-spark-batch.md ← Phase 1.3 snapshot (complete)
    │   ├── phase-1.4-dbt-models.md ← Phase 1.4 snapshot (complete)
    │   ├── phase-1.5-airflow-dag.md ← Phase 1.5 snapshot (complete)
    │   └── phase-1.6-verification.md ← Phase 1.6 snapshot (complete)
    └── conventions/
        ├── airflow.md
        ├── dbt.md
        ├── docker.md
        └── spark.md
```

## Next Steps

Phase 1 is **COMPLETE** (1.1 Docker + 1.2 Ingestion + 1.3 Spark batch + 1.4 DBT models + 1.5 Airflow DAG + 1.6 Verification). All verified end-to-end: cold start → DAG run → 4 tasks succeed → marts queryable (263,394 fact rows). **Phase 2 is unlocked.**

1. **Phase 2: Streaming** — Kafka + Spark Structured Streaming to pipe Divvy live data into Postgres
   - Requires: Phase 1 batch pipeline working end-to-end (verified ✅)
   - New: Kafka broker, Spark Structured Streaming job, Divvy station status API, real-time ingestion DAG
   - Phase 2 plan: see `chicago-pipeline-plan.md`

## Active Constraints

- **Phase gates:** Phase 1 COMPLETE and verified. Phase 2 unlocked. Phase 3 locked until Phase 2 works. Do NOT skip ahead.
- **Learning protocol:** Socratic by default. User must say "write the code" to get code. Currently in AI-writes-code mode.
- **Three-doc system:** `changelog.md` (errors), `docs/knowledge/` (reference, one file per topic), `docs/operations-performed.md` (audit trail). Update all three after every change.
- **Phase-completion docs:** After each sub-phase is verified, create `docs/phases/phase-X.Y-<name>.md` from `TEMPLATE.md`. Include one high-level mermaid diagram + pointer to `docs/knowledge/architecture.md` for details. See `docs/phases/README.md`.
- **Chat-history system:** Update `chat-history/` when context approaches 75%. Update `current-state.md` at the end of a session.
- **Doc maintainability (AGENTS.md rule 14):** When a `.md` file exceeds ~500 lines or ~20KB, split into a folder with one file per section + `index.md`. Append-only logs stay single but get a TOC with anchor links.
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

- **Airflow 3.0 DockerOperator:** Not used in Phase 1.5 — BashOperator with `docker exec` (Spark) and `docker run --rm` (dbt) was simpler and avoided DockerOperator's mount/network complexity. DockerOperator may be revisited in Phase 2 if needed.
- **Socrata app token configured:** All four credentials stored in `.env` (App Token, API Key ID, API Key Secret, Secret Token). App token passed to Airflow container via docker-compose. Rate limit increased from 1K to 10K req/hr.
- **Bitnami images no longer free** — resolved for Spark by switching to `apache/spark:3.5.1`. If other Bitnami images were planned (Kafka, etc.), need alternatives. Kafka isn't needed until Phase 2.
- **`docker compose down` (without `-v`) preserves data** — named volumes `postgres_data` and `airflow_logs` persist. Use `-v` only to wipe everything.
- **WSL2 memory limit:** Increased from 4GB to 8GB via `C:\Users\sagar\.wslconfig` (user did manually). 4GB was bottlenecking with 7 Docker services. Apply with `wsl --shutdown` then reopen terminal.
- **apache/spark PATH:** `spark-submit` is not on PATH in the apache/spark container. Always use `/opt/spark/bin/spark-submit` when exec'ing into spark-master.
- **Spark-written tables not persisted by init.sql:** `raw.crime_events` is created by the Spark job at runtime, not by `init.sql`. If the Postgres volume is wiped, re-run the batch job (idempotent via `overwrite` mode).
- **DBT profiles.yml has hardcoded password:** `dbt/profiles.yml` contains `chicago1234` in plaintext. It's in `.gitignore` (not committed), but for Phase 4 (cloud) this should use environment variables or a secrets manager.
- **dbt-expectations on Postgres BOOLEAN:** `expect_column_values_to_be_in_set` fails with `boolean = text` operator error. Use `not_null` instead — BOOLEAN can't hold values outside {true, false, null}.
- **dbt Power User extension:** Configured via `.vscode/settings.json` (`dbt.allowListFolders`, `dbt.dbtPythonPathOverride`) and `~/.dbt/profiles.yml`. User may need to reload IDE window if lineage doesn't render.
- **DBT run location:** dbt commands must be run from inside the `dbt/` directory (where `dbt_project.yml` lives), not from repo root or `.venv/bin/`.

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
| `2026-07-13/01-phase-1.3-spark-batch.md` | Spark batch job: Parquet → clean → Postgres, docker-compose env vars, spark-submit PATH fix |
| `2026-07-13/02-phase-1.4-dbt-models.md` | DBT project scaffold, staging + marts, dbt-expectations, generate_schema_name override, dbt Power User extension fix |
| `2026-07-13/03-phase-1.5-airflow-dag.md` | Airflow DAG, dbt Docker image, protobuf conflict, docker.sock GID, execution API URL, shared JWT secrets |
| `2026-07-13/04-gid-portability-and-socrata-credentials.md` | DOCKER_GID build arg fix, all 4 Socrata credentials stored |
| `2026-07-13/05-phase-1.6-verification.md` | Phase 1 gate: cold start, DAG run with clear_dbt_schemas, dag-processor service, marts verified |
