# Current State — Handoff Document

> **Read this first in a new session.** This file is the handoff: current state, active decisions, and next steps. Last updated: 2026-07-11 (end of session).

---

## Project

Chicago Crime + Divvy Bike-Share data engineering pipeline. A learning project that answers: *Does crime near a Divvy station affect ridership?*

- **Repo:** `~/chicago-data-pipeline/` (WSL, Ubuntu on Windows 10)
- **Git:** initialized on `main`, no commits yet (user commits manually)
- **Phase:** 1 (Batch Foundation) — Phase 1.1 (Docker) + Phase 1.2 (Ingestion) COMPLETE. Next: Phase 1.3 (Spark batch job)
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

### Airflow 3.0 Runtime Breaking Changes (ALL FIXED)
These were discovered during `docker compose up` and are now resolved:

| Issue | Fix Applied |
|---|---|
| `airflow webserver` command removed | `command: api-server` in docker-compose.yml |
| Scheduler has no default CMD | `command: scheduler` added explicitly |
| Health endpoint moved | `/api/v2/monitor/health` (not `/health`) |
| Port config section moved | `AIRFLOW__API__PORT` (not `AIRFLOW__WEBSERVER__WEB_SERVER_PORT`) |
| passwords.json PermissionError | `chmod 666 airflow/passwords.json` on host |
| Spark master healthcheck on wrong port | Check Web UI port 8080 (not RPC port 7077) |

### Files Created
```
~/chicago-data-pipeline/
├── .env.example              ← env var template (Airflow 3.0 SimpleAuthManager config)
├── .gitignore
├── AGENTS.md                 ← AI assistant rules (read changelog, knowledge, operations-performed)
├── README.md                 ← 3 Mermaid diagrams
├── changelog.md              ← errors/fixes/lessons log
├── chicago-pipeline-plan.md  ← full phased plan
├── docker-compose.yml        ← 6 services, YAML anchors, Airflow 3.0
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
│   └── jobs/.gitkeep
├── ingestion/
│   └── download_crime.py     ← Socrata API → Parquet (Phase 1.2)
├── data/                     ← Parquet output (gitignored)
│   └── raw/crime/crime_2023.parquet ← 263K rows, 11.5 MB
├── chat-history/             ← THIS FOLDER — conversation reference
└── docs/
    ├── knowledge.md          ← reference commands, syntax, concepts, architecture
    ├── learning-protocol.md  ← Socratic mode rules
    ├── operations-performed.md ← audit trail of what was built
    ├── phases/               ← phase-completion docs (one per sub-phase)
    │   ├── README.md         ← explains the system
    │   ├── TEMPLATE.md       ← copy to start a new phase doc
    │   ├── phase-1.1-docker.md ← Phase 1.1 snapshot (complete)
    │   └── phase-1.2-ingestion.md ← Phase 1.2 snapshot (complete)
    └── conventions/
        ├── airflow.md
        ├── dbt.md
        ├── docker.md
        └── spark.md
```

## Next Steps

Phase 1.1 (Docker) and Phase 1.2 (Ingestion) are **complete and verified**. Next:

1. **Phase 1.3: Spark batch job** (`spark/jobs/crime_batch.py`)
   - Read Parquet from `data/raw/crime/crime_2023.parquet`
   - Clean: parse dates, normalize `primary_type` casing, handle null lat/long, cast `community_area` to int
   - Write to Postgres `raw.crime_events` via JDBC
   - Requires: Parquet file (done), Postgres `raw` schema (done), JDBC driver in Spark image (done)
2. **Phase 1.4: DBT models** — staging + mart transformations
3. **Phase 1.5: Airflow DAG** — orchestrate the full pipeline

## Active Constraints

- **Phase gates:** Phase 2 locked until Phase 1 works end-to-end and is verified. Do NOT skip ahead.
- **Learning protocol:** Socratic by default. User must say "write the code" to get code. Currently in AI-writes-code mode.
- **Three-doc system:** `changelog.md` (errors), `docs/knowledge.md` (reference), `docs/operations-performed.md` (audit trail). Update all three after every change.
- **Phase-completion docs:** After each sub-phase is verified, create `docs/phases/phase-X.Y-<name>.md` from `TEMPLATE.md`. Include section-by-section mermaid diagrams showing how files connect. See `docs/phases/README.md`.
- **Chat-history system:** Update `chat-history/` when context approaches 75%. Update `current-state.md` at the end of each session.
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
- **No pipeline code exists yet** — all files are infrastructure, planning, and documentation. Next step is the ingestion script.
- **Bitnami images no longer free** — resolved for Spark by switching to `apache/spark:3.5.1`. If other Bitnami images were planned (Kafka, etc.), need alternatives. Kafka isn't needed until Phase 2.
- **`docker compose down` (without `-v`) preserves data** — named volumes `postgres_data` and `airflow_logs` persist. Use `-v` only to wipe everything.

## Chat History Chunks (this session)

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
