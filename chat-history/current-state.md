# Current State — Handoff Document

> **Read this first in a new session.** This file is the handoff: current state, active decisions, and next steps. Last updated: 2026-07-13 (end of session).

---

## Project

Chicago Crime + Divvy Bike-Share data engineering pipeline. A learning project that answers: *Does crime near a Divvy station affect ridership?*

- **Repo:** `~/chicago-data-pipeline/` (WSL, Ubuntu on Windows 10)
- **Git:** initialized on `main`, no commits yet (user commits manually)
- **Phase:** 1 (Batch Foundation) — Phase 1.1 (Docker) + 1.2 (Ingestion) + 1.3 (Spark batch) + 1.4 (DBT models) COMPLETE. Next: Phase 1.5 (Airflow DAG)
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

### Docker Compose — 6 services
| Service | Image | Status |
|---|---|---|
| postgres | `postgres:16-alpine` | **healthy** — 3 schemas (raw, staging, mart) confirmed |
| spark-master | `apache/spark:3.5.1` + JDBC driver | **healthy** — UI on port 8180 |
| spark-worker | same as master | **running** — UI on port 8081 |
| airflow-init | `apache/airflow:3.0.0-python3.11` | **exited (0)** — migrations complete |
| airflow-webserver | same | **healthy** — UI on port 8080 (admin/admin) |
| airflow-scheduler | same | **running** — heartbeat active |

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
- Socrata app token is OPTIONAL — script works without it (1K req/hr anonymous, 10K with token)

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

### Files Created
```
~/chicago-data-pipeline/
├── .env.example              ← env var template (Airflow 3.0 SimpleAuthManager config)
├── .gitignore
├── .vscode/
│   └── settings.json         ← dbt Power User config (allowListFolders, Python path)
├── AGENTS.md                 ← AI assistant rules (14 rules, read first)
├── README.md                 ← 3 Mermaid diagrams + progress table
├── changelog.md              ← errors/fixes/lessons log (with TOC)
├── chicago-pipeline-plan.md  ← full phased plan
├── docker-compose.yml        ← 6 services, YAML anchors, Airflow 3.0, data mount, Spark env vars
├── init.sql                  ← 3 schemas + airflow user + airflow_metadata DB
├── pyproject.toml            ← uv project mode
├── uv.lock                   ← reproducible installs
├── airflow/
│   ├── Dockerfile            ← Airflow 3.0.0 + docker CLI + uv pip install
│   ├── passwords.json        ← SimpleAuthManager: {"admin": "admin"} (chmod 666)
│   ├── requirements.txt      ← postgres + docker providers
│   └── dags/.gitkeep
├── spark/
│   ├── Dockerfile            ← apache/spark:3.5.1 + PostgreSQL JDBC
│   └── jobs/
│       └── crime_batch.py    ← Spark batch ETL: Parquet → clean → Postgres (Phase 1.3)
├── ingestion/
│   └── download_crime.py     ← Socrata API → Parquet (Phase 1.2)
├── dbt/                      ← DBT transformation project (Phase 1.4)
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
    │   └── phase-1.4-dbt-models.md ← Phase 1.4 snapshot (complete)
    └── conventions/
        ├── airflow.md
        ├── dbt.md
        ├── docker.md
        └── spark.md
```

## Next Steps

Phase 1.1 (Docker), 1.2 (Ingestion), 1.3 (Spark batch), and 1.4 (DBT models) are **complete and verified**. Next:

1. **Phase 1.5: Airflow DAG** (`airflow/dags/crime_batch_dag.py`)
   - Orchestrate: download_crime → spark_crime_batch → dbt_run → dbt_test
   - Use Airflow's `DockerOperator` or `BashOperator` to run the Spark job, `BashOperator` for DBT
   - Schedule: `@daily` (but start with `@manual` while debugging)
   - Requires: working Spark job (done) + DBT models (done)
   - New: Airflow DAG file, task dependencies, retry logic, XCom for task status
2. **Phase 1.6: Phase 1 deliverable & verification** — end-to-end pipeline test
   - `docker compose up` → trigger DAG → all 4 steps run → DBT marts queryable
   - This is the Phase 1 gate: Phase 2 unlocks when this works

## Active Constraints

- **Phase gates:** Phase 2 locked until Phase 1 works end-to-end and is verified. Do NOT skip ahead.
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

- **Airflow 3.0 DockerOperator:** The `apache-airflow-providers-docker` package is installed and the image built successfully (133 packages, no conflicts). Will know if it works when first DAG uses DockerOperator.
- **Socrata app token not set:** `SOCRATA_APP_TOKEN` is empty in `.env`. Script works without it (anonymous rate limit 1K req/hr is sufficient for 263K rows = 6 requests). Add token later for larger pulls.
- **Bitnami images no longer free** — resolved for Spark by switching to `apache/spark:3.5.1`. If other Bitnami images were planned (Kafka, etc.), need alternatives. Kafka isn't needed until Phase 2.
- **`docker compose down` (without `-v`) preserves data** — named volumes `postgres_data` and `airflow_logs` persist. Use `-v` only to wipe everything.
- **WSL2 memory limit:** Increased from 4GB to 8GB via `C:\Users\sagar\.wslconfig` (user did manually). 4GB was bottlenecking with 6 Docker services. Apply with `wsl --shutdown` then reopen terminal.
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
