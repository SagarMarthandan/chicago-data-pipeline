# Operations Performed

A chronological log of operations, files created, and structural changes made to this repo. Explains *what* exists and *why* — not errors (those go in `changelog.md`) or reference material (that goes in `docs/knowledge/`).

> **Format:** `YYYY-MM-DD` — what was done, what was created, and the reasoning.

## Table of Contents

- [2026-07-08 — Project Setup & Migration](#2026-07-08--project-setup--migration)
- [2026-07-09 — Phase 1.1 Docker Setup](#2026-07-09--phase-11-docker-setup-started)
- [2026-07-09 — Airflow 2.8.4 → 3.0.0 Upgrade](#2026-07-09--airflow-284--300-upgrade)
- [2026-07-09 — Chat History System Created](#2026-07-09--chat-history-system-created)
- [2026-07-09 — Bitnami Spark → apache/spark Migration](#2026-07-09--bitnami-spark--apachespark-migration)
- [2026-07-09 — Airflow 3.0 Runtime Fixes + Phase Documentation System](#2026-07-09--airflow-30-runtime-fixes--phase-documentation-system)
- [2026-07-11 — Phase 1.2: Ingestion Script](#2026-07-11--phase-12-ingestion-script)
- [2026-07-11 — Mermaid Diagram Rendering Fixes](#2026-07-11--mermaid-diagram-rendering-fixes)
- [2026-07-13 — Phase 1.3: Spark Batch Job](#2026-07-13--phase-13-spark-batch-job)
- [2026-07-13 — Phase 1.4: DBT Models](#2026-07-13--phase-14-dbt-models)
- [2026-07-13 — Phase 1.5: Airflow DAG](#2026-07-13--phase-15-airflow-dag)
- [2026-07-13 — Phase 1.6: Verification](#2026-07-13--phase-16-verification)
- [2026-07-15 — Phase 2.1: Divvy GBFS Data Source Exploration](#2026-07-15--phase-21-divvy-gbfs-data-source-exploration)
- [2026-07-15 — Phase 2.2: Kafka + Zookeeper Docker Services](#2026-07-15--phase-22-kafka--zookeeper-docker-services)
- [2026-07-15 — Phase 2.3: Kafka Producer](#2026-07-15--phase-23-kafka-producer)
- [2026-07-15 — Phase 2.4: Spark Structured Streaming](#2026-07-15--phase-24-spark-structured-streaming)
- [2026-07-16 — Phase 2.5: DBT Stream Models](#2026-07-16--phase-25-dbt-stream-models)
- [2026-07-16 — Phase 2.6: Airflow Stream DAG](#2026-07-16--phase-26-airflow-stream-dag)
- [2026-07-18 — Phase 3.1: Grafana](#2026-07-18--phase-31-grafana)
- [2026-07-20 — Phase 3.2: DBT Tests](#2026-07-20--phase-32-dbt-tests)
- [2026-07-20 — Phase 3.3: Airflow Robustness](#2026-07-20--phase-33-airflow-robustness)
- [2026-07-20 — Phase 3.4: Verification](#2026-07-20--phase-34-verification)
- [2026-07-21 — Phase 4.1: Warehouse Choice + GCP Project Setup](#2026-07-21--phase-41-warehouse-choice--gcp-project-setup)
- [2026-07-21 — Phase 4.2: Terraform (BigQuery + GCS provisioning)](#2026-07-21--phase-42-terraform-bigquery--gcs-provisioning)
- [2026-07-21 — Phase 4.3: Architecture Change (Postgres → GCS/BigQuery)](#2026-07-21--phase-43-architecture-change-postgres--gcsbigquery)
- [2026-07-22 — Phase 4.4: Divvy Trip History + Correlation Analysis](#2026-07-22--phase-44-divvy-trip-history--correlation-analysis)
---

## 2026-07-08 — Project Setup & Migration

### Planning Phase (Windows / Devin IDE)
- Created project plan `chicago-pipeline-plan.md` (27 KB) — full phased build, repo structure, DBT model SQL, Spark job skeletons, Airflow DAG structure, analytical query
- Created `AGENTS.md` — root agent instructions; AI assistants read this automatically. Defines project context, phase gates, learning mode rules
- Created `docs/learning-protocol.md` — defines Socratic learning mode (AI asks what you've tried, doesn't hand fixes). Explicit mode switches: "write the code", "I give up just fix it", "pair with me"
- Created `docs/conventions/docker.md` — Docker best practices: service naming, networking, volumes, env management, healthchecks, WSL-specific notes
- Created `docs/conventions/dbt.md` — DBT modeling conventions, `try_cast` macro rule, model layer structure
- Created `docs/conventions/spark.md` — Spark job conventions: partitioning, memory, JDBC patterns
- Created `docs/conventions/airflow.md` — Airflow DAG conventions: idempotency, retries, SLAs, operator selection

### Migration to WSL
- Copied project folder from Windows (`C:\Users\sagar\Documents\chicago-data-pipeline\`) to WSL filesystem
- Flattened folder structure — moved `AGENTS.md`, `chicago-pipeline-plan.md`, and `docs/` from nested `devin/` subfolder to repo root
- Renamed repo root from `chicago-divvy-DE-project` → `chicago-data-pipeline` to match `COMPOSE_PROJECT_NAME` and keep Docker network/volume names lowercase and predictable

### Git Initialization
- Ran `git init`, renamed default branch from `master` to `main`
- Created `.gitignore` — excludes `.env`, data files (`*.csv`, `*.parquet`), Python artifacts, DBT target, Airflow logs, Spark metastore, Kafka data, Postgres data, Terraform state, IDE files
- Created `README.md` — project overview, stack table, data sources, Mermaid diagrams (architecture, pipeline flow, roadmap), getting started guide

### Documentation Files
- Created `changelog.md` (repo root) — running log of errors, fixes, and lessons. Pre-populated with 5 planning-phase bugs + 3 documentation/setup bugs (8 total across two entries)
- Created `docs/knowledge.md` — reference lookup organized by tool (WSL, Docker, Postgres, DBT, Spark, Kafka, Airflow, Git, data sources). Commands, syntax, key concepts
- Created `docs/operations-performed.md` — this file; structural audit trail of what exists and why
- Rewrote `README.md` with 3 Mermaid diagrams (architecture, pipeline flow, roadmap) — initial incremental edits didn't persist, rewrote with full file overwrite

### AGENTS.md Updates
- Added rules 9, 10, 11 — AI must read `changelog.md` before starting work, read `docs/knowledge.md` for reference, and update `docs/operations-performed.md` after structural changes
- Updated header note to reference all three docs (`changelog.md`, `docs/knowledge.md`, `docs/operations-performed.md`)
- Updated repo structure block to include `changelog.md`, `docs/knowledge.md`, and `docs/operations-performed.md` with annotations
- Fixed stale path references (`chicago-divvy-DE-project` → `chicago-data-pipeline`)
- Fixed missing opening ` ``` ` code fence in repo structure block (lost during edit, re-added)

### Conventions Updates
- `docs/conventions/docker.md` — added `COMPOSE_PROJECT_NAME=chicago-data-pipeline` to `.env` example and networking section; updated WSL path reference
- `docs/conventions/airflow.md` — updated DockerOperator network/volume names to `chicago-data-pipeline_*`; added note referencing `COMPOSE_PROJECT_NAME`

### Current Repo Structure
```
~/chicago-data-pipeline/
├── .git/
├── .gitignore
├── AGENTS.md
├── README.md
├── changelog.md
├── chicago-pipeline-plan.md
└── docs/
    ├── knowledge.md
    ├── learning-protocol.md
    ├── operations-performed.md
    └── conventions/
        ├── airflow.md
        ├── dbt.md
        ├── docker.md
        └── spark.md
```

**No pipeline code exists yet.** All files are planning, conventions, and documentation. Repo is ready for first git commit. Phase 1 (Batch Foundation) implementation starts next.

---

## 2026-07-09 — Phase 1.1 Docker Setup (started)

### `.env.example` (created)
- Environment variable template committed to git. Copy to `.env` (gitignored) and fill in real values.
- Contains:
  - `COMPOSE_PROJECT_NAME=chicago-data-pipeline` — fixed project name for predictable Docker network/volume names
  - Postgres warehouse credentials (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`) — for `chicago_analytics` database
  - Airflow metadata DB credentials (`AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_DB_NAME`) — separate database within same Postgres instance for Airflow internal state
  - Airflow 3.0 config (`AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__CORE__LOAD_EXAMPLES=False`, webserver port, SimpleAuthManager users + passwords file path)
  - `SOCRATA_APP_TOKEN` — placeholder, empty until ingestion script (Phase 1.2)

### Key Decisions
- **Two databases in one Postgres container** — `chicago_analytics` (warehouse: raw + mart schemas) and `airflow_metadata` (Airflow's internal state). Avoids a second Postgres container. The `init.sql` will create both databases and the `airflow` user.
- **LocalExecutor over CeleryExecutor** — provides parallel task execution without needing Redis/RabbitMQ containers. Switchable to CeleryExecutor later if workloads grow.
- **Image names stay in docker-compose.yml, not .env** — image names (`postgres:16-alpine`, etc.) are not secrets or environment-specific config. `.env` is for secrets and config that changes between environments.

### `init.sql` (created)
- Postgres init script mounted into `/docker-entrypoint-initdb.d/` via docker-compose.yml
- Runs only on first container startup (when data volume is empty)
- Creates:
  - `raw` schema in `chicago_analytics` — landing zone for untransformed data from Spark/Kafka
  - `staging` schema in `chicago_analytics` — DBT staging layer: light cleaning, renaming, type casting (1:1 with source tables)
  - `mart` schema in `chicago_analytics` — DBT final output: facts + dimensions, analytics-ready
  - `airflow` user with password `airflow_pass` — uses `DO $$ ... $$` block because Postgres has no `CREATE USER IF NOT EXISTS`
  - `airflow_metadata` database owned by `airflow` — uses `SELECT ... \gexec` trick because `CREATE DATABASE` can't run inside a transaction
- Grants: `chicago` user gets full privileges on `raw`, `staging`, and `mart` schemas; `airflow` user gets full privileges on `airflow_metadata` database
- Values hardcoded (not env vars) because SQL files can't read `.env`. Values match `.env.example`.
### `docker-compose.yml` (created, updated for Airflow 3.0)
- 6 services: postgres, spark-master, spark-worker, airflow-init, airflow-webserver, airflow-scheduler
- Uses YAML anchor `x-airflow-common` to share env vars + volumes across 3 Airflow services
- All env vars interpolated from `.env` (e.g., `${POSTGRES_USER}`, `${AIRFLOW_DB_PASSWORD}`)
- Airflow 3.0 changes: SimpleAuthManager env vars added, `airflow/passwords.json` mounted, `airflow users create` removed from airflow-init (only `airflow db migrate` now)

### `airflow/Dockerfile` (created, updated to Airflow 3.0, permission fix applied)
- Based on `apache/airflow:3.0.0-python3.11` (upgraded from 2.8.4 — 2.x is EOL since April 2026)
- Installs `docker.io` (Docker CLI) for DockerOperator — official image doesn't include it
- Copies uv binary from `ghcr.io/astral-sh/uv:latest` via multi-stage COPY (no install script needed)
- Installs Airflow providers from `airflow/requirements.txt` using `uv pip install --system` (10-100x faster than pip)
- Uses `--system` flag (installs into container's system Python, no venv needed in containers)
- Uses `uv pip install` not `uv sync` because host and containers need different packages
- Runs `uv pip install` as root (not airflow user) because `--system` writes to `/usr/local/lib/python3.11/site-packages/` which is owned by root. Switches to `USER airflow` at the end for running services.

### `airflow/passwords.json` (created)
- Airflow 3.0 SimpleAuthManager passwords file
- JSON mapping of username → password: `{"admin": "admin"}`
- Mounted into container at `/opt/airflow/config/passwords.json` via docker-compose.yml
- Replaces Airflow 2.x's `airflow users create` CLI command (removed in 3.0)

### `airflow/requirements.txt` (created)
- `apache-airflow-providers-postgres` — PostgresHook, SqlSensor
- `apache-airflow-providers-docker` — DockerOperator

### `spark/Dockerfile` (created, updated to apache/spark)
- Based on `apache/spark:3.5.1` (switched from `bitnami/spark:3.5` — Bitnami moved behind commercial subscription in 2026)
- Downloads PostgreSQL JDBC driver (`postgresql-42.7.3.jar`) into `/opt/spark/jars/`
- Needed for Spark to write to Postgres via JDBC (`df.write.format("jdbc")`)
- Non-root user is `spark` (UID 185), not Bitnami's UID 1001

### Directory placeholders
- `airflow/dags/.gitkeep` — ensures dags/ directory exists in git
- `spark/jobs/.gitkeep` — ensures jobs/ directory exists in git

### uv Virtual Environment (created)
- Initialized with `uv init --bare --name chicago-data-pipeline` — project mode with `pyproject.toml` + `uv.lock`
- Dependencies added via `uv add`: requests, sodapy, dbt-core, dbt-postgres, python-dotenv, psycopg2-binary
- `pyproject.toml` — project metadata + dependency declarations (committed to git)
- `uv.lock` — exact versions + hashes for reproducible installs (committed to git)
- Note: Docker containers have their own Python (managed by Dockerfiles). Host uses uv init (project mode). Airflow container uses uv pip install --system for fast installs.
- Activate with `source .venv/bin/activate` in each new terminal
- Recreate on another machine with `uv sync` (reads lockfile)
- Note: Docker containers have their own Python (managed by Dockerfiles). uv is host-only.

### Pending
- Copy `.env.example` to `.env` and fill in values
- `docker compose build` — build custom Airflow + Spark images
- `docker compose up -d` — start all services
- Verify: Postgres schemas exist, Airflow UI loads (admin/admin via SimpleAuthManager), Spark master UI loads

---

## 2026-07-09 — Airflow 2.8.4 → 3.0.0 Upgrade

### Why
Airflow 2.x reached end-of-life in April 2026. No more security patches or bug fixes. Airflow 3.0.0 (April 2025) is the first stable 3.x release with 15 months of production hardening. Chose 3.0.0 over 3.3.0 (released July 6, 2026 — only 3 days old, too new for stability).

### Files Changed
- `airflow/Dockerfile` — image tag `apache/airflow:2.8.4-python3.11` → `apache/airflow:3.0.0-python3.11`
- `docker-compose.yml` — removed `airflow users create` from airflow-init command, added `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` + `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` env vars, mounted `airflow/passwords.json` into container, removed `_AIRFLOW_WWW_USER_USERNAME`/`_AIRFLOW_WWW_USER_PASSWORD` env vars
- `.env.example` — removed `AIRFLOW_WWW_USER`/`AIRFLOW_WWW_PASSWORD`, added `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` + `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/opt/airflow/config/passwords.json`

### Files Created
- `airflow/passwords.json` — SimpleAuthManager passwords file (`{"admin": "admin"}`)

### Breaking Changes Handled
- **Authentication**: Flask-AppBuilder (FAB) → SimpleAuthManager (new default in 3.0)
- **User creation**: `airflow users create` CLI removed → users defined via `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` env var + `passwords.json` file
- **airflow-init command**: simplified to `airflow db migrate` only (no user creation step)

### What Stayed the Same
- `AIRFLOW__CORE__EXECUTOR=LocalExecutor` — still works in 3.0
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` — still works for core components
- `AIRFLOW__CORE__LOAD_EXAMPLES=False` — still works
- `airflow db migrate` command — still works in 3.0
- Docker CLI installation + docker.sock mount for DockerOperator — unchanged


---

## 2026-07-09 — Chat History System Created

### Why
GLM 5.2 has a 200k context window. At ~75% usage, auto-compaction compresses older messages and loses detail. The `chat-history/` folder is a permanent, uncompressed reference — a "second brain" that a fresh or compacted session can read to reconstruct what was done, why, and what's next.

### Structure
```
chat-history/
├── README.md                    ← explains the system, structure, chunk format
├── current-state.md             ← HANDOFF DOC — read first in new session
├── 2026-07-08/
│   └── 01-project-setup-and-migration.md
├── 2026-07-09/
    ├── 01-docker-setup-env-and-init.md
    ├── 02-docker-compose-and-dockerfiles.md
    ├── 03-uv-init.md
    ├── 04-airflow-upgrade.md
    └── 05-chat-history-system.md
```

### Files Created
- `chat-history/README.md` — explains the system, how to use it, chunk format
- `chat-history/current-state.md` — handoff document with current project state, active decisions, next steps, constraints, user preferences
- `chat-history/2026-07-08/01-project-setup-and-migration.md` — initial planning, WSL migration, three-doc system
- `chat-history/2026-07-09/01-docker-setup-env-and-init.md` — .env.example + init.sql
- `chat-history/2026-07-09/02-docker-compose-and-dockerfiles.md` — 6 services, Dockerfiles, YAML anchors
- `chat-history/2026-07-09/03-uv-init.md` — uv project mode + uv in Docker
- `chat-history/2026-07-09/04-airflow-upgrade.md` — Airflow 2.8.4 → 3.0.0, SimpleAuthManager migration
- `chat-history/2026-07-09/05-chat-history-system.md` — this system (meta)

### Key Decision
| Decision | Choice | Why |
|---|---|---|
| Chat history tracking | Structured Markdown chunks in date folders | Complements the three-doc system (which tracks the project). This tracks the *conversation*. Chunked by topic, not by message, for easy lookup. `current-state.md` is the handoff doc for fresh sessions. |

---

## 2026-07-09 — Bitnami Spark → apache/spark Migration

### Why
`docker compose build` failed: `bitnami/spark:3.5: not found`. Bitnami moved their Docker images behind a commercial subscription ("Bitnami Secure Images") in 2026. The free `docker.io/bitnami/*` images are no longer available.

### Files Changed
- `spark/Dockerfile` — base image `bitnami/spark:3.5` → `apache/spark:3.5.1`, JDBC path `/opt/bitnami/spark/jars/` → `/opt/spark/jars/`, non-root user UID 1001 → `spark` (UID 185)
- `docker-compose.yml` — Spark services rewritten:
  - `spark-master`: replaced `SPARK_MODE=master` env var with `command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master`
  - `spark-worker`: replaced `SPARK_MODE=worker` + `SPARK_MASTER_URL` env vars with `command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077`
  - Volume paths: `/opt/bitnami/spark/jobs/` → `/opt/spark/jobs/`
  - Added `SPARK_MASTER_HOST=spark-master` env var (tells master to advertise Docker service name)
  - Healthcheck: bash `/dev/tcp` → python3 socket check (more portable)
  - Removed Bitnami-specific RPC/SSL env vars (`SPARK_RPC_AUTHENTICATION_ENABLED`, etc.)
  - `SPARK_WORKER_CORES=2` and `SPARK_WORKER_MEMORY=2G` still work as env vars (spark-class reads them)

### Current Repo Structure
```
~/chicago-data-pipeline/
├── .env.example
├── .gitignore
├── AGENTS.md
├── README.md
├── changelog.md
├── chicago-pipeline-plan.md
├── docker-compose.yml
├── init.sql
├── pyproject.toml
├── uv.lock
├── airflow/
│   ├── Dockerfile
│   ├── passwords.json
│   ├── requirements.txt
│   └── dags/.gitkeep
├── spark/
│   ├── Dockerfile
│   └── jobs/.gitkeep
├── chat-history/
│   ├── README.md
│   ├── current-state.md
│   └── 2026-07-0{8,9}/*.md
└── docs/
    ├── knowledge.md
    ├── learning-protocol.md
    ├── operations-performed.md
    ├── phases/
    │   ├── README.md
    │   ├── TEMPLATE.md
    │   └── phase-1.1-docker.md
    └── conventions/
        ├── airflow.md
        ├── dbt.md
        ├── docker.md
        └── spark.md
```

**Phase 1.1 (Docker Compose services) is complete and verified.** All 6 services running and healthy. Next: Phase 1.2 (ingestion script).

---

## 2026-07-09 — Airflow 3.0 Runtime Fixes + Phase Documentation System

### Airflow 3.0 Runtime Fixes
- `docker-compose.yml` — 4 fixes:
  - `airflow-webserver` command: `webserver` → `api-server` (command removed in 3.0)
  - `airflow-scheduler`: added explicit `command: scheduler` (3.0 image has no default CMD)
  - `airflow-webserver` healthcheck: `/health` → `/api/v2/monitor/health` (endpoint moved in 3.0)
  - Added missing `healthcheck:` key (was accidentally under `ports:`)
- `.env.example` — `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` → `AIRFLOW__API__PORT` (config section moved in 3.0)
- `airflow/passwords.json` — `chmod 666` on host (SimpleAuthManager opens with `a+` mode, airflow user UID 50000 needs write access)

### Phase Documentation System Created
- `docs/phases/README.md` — explains the phase-doc system: when to create, what goes in each section, relationship to other docs
- `docs/phases/TEMPLATE.md` — copy this to start a new phase doc (sections: summary, files, architecture with mermaid, errors, decisions, verification, what's next)
- `docs/phases/phase-1.1-docker.md` — Phase 1.1 completion document with 7 section-by-section mermaid diagrams, all 8 errors, 10 decisions, verification output
- `AGENTS.md` — added rule 13: create phase-completion doc after each sub-phase
- `docs/knowledge.md` — added "How Everything Connects" section (8 subsections, 9 mermaid diagrams) explaining how uv, Spark, Airflow, init.sql, .env, docker.sock, and docker-compose.yml all link together

---

## 2026-07-11 — Phase 1.2: Ingestion Script

### Files Created
- `ingestion/download_crime.py` — Socrata API ingestion script: paginates Chicago crime data, cleans API quirks, writes to Parquet
- `ingestion/.gitkeep` — ensures ingestion/ directory exists in git
- `data/raw/crime/` — output directory for Parquet files (gitignored)
- `data/raw/crime/crime_2023.parquet` — 263,393 rows of 2023 crime data (11.5 MB, gitignored)

### Files Modified
- `docker-compose.yml` — added `./data:/opt/spark/data` mount to spark-master, spark-worker, and `./data:/opt/airflow/data` to airflow-common volumes
- `docs/knowledge.md` — expanded Data Sources Reference with correct resource ID (`ijzp-q8t2`), SoQL parameter table, API response quirks, column reference table
- `changelog.md` — added Phase 1.2 errors (wrong resource ID, missing import, missing data mount)

### Packages Installed (host .venv)
- `pyarrow==25.0.0` — Parquet read/write
- `pandas==3.0.3` — DataFrame operations
- `python-dotenv` — .env loading
- `numpy==2.5.1` — pandas dependency

### Verification Results
- Script downloaded 263,393 rows in 6 pages (50K + 50K + 50K + 50K + 50K + 13,393)
- Parquet file: 21 columns, 11.5 MB
- Data quality: 0.8% null lat/long, 0.6% null location_description, 31 unique primary_type values
- Spark successfully read the Parquet with correct schema and sample rows verified

---

## 2026-07-11 — Mermaid Diagram Rendering Fixes

### Files Modified
- `docs/knowledge.md` — quoted 10 unquoted colon labels across 4 diagrams (Spark, Airflow, startup order, docker.sock); added "Mermaid Diagram Syntax Rules" section with quoting rules and scanner script
- `docs/phases/phase-1.1-docker.md` — quoted 5 unquoted colon labels in service overview diagram
- `changelog.md` — added mermaid rendering errors entry (5 error types, 3 files affected)

### What was fixed
15 mermaid node labels had unquoted special characters (`:`, `/`, `$`, `{`, `}`) that break rendering. All labels containing these characters are now wrapped in double quotes. A Python scanner was added to `docs/knowledge.md` to catch future issues.

---

## 2026-07-11 — Knowledge Base Split into Sectioned Files

### Why
`docs/knowledge.md` grew to 43KB / 1003 lines — too large for quick lookup. Split into `docs/knowledge/` folder with one file per topic + an `index.md` with navigation links.

### Files Created
- `docs/knowledge/index.md` — section directory with navigation table and relationship to other docs
- `docs/knowledge/wsl.md` — WSL commands and tips
- `docs/knowledge/uv.md` — uv package manager reference
- `docs/knowledge/docker-compose.md` — Docker Compose patterns
- `docs/knowledge/architecture.md` — How Everything Connects (9 mermaid diagrams)
- `docs/knowledge/postgres.md` — Postgres commands and schema reference
- `docs/knowledge/dbt.md` — DBT reference
- `docs/knowledge/spark.md` — Spark reference
- `docs/knowledge/kafka.md` — Kafka reference (Phase 2)
- `docs/knowledge/airflow.md` — Airflow 2.x vs 3.x comparison (9 subsections)
- `docs/knowledge/git.md` — Git commands
- `docs/knowledge/data-sources.md` — Socrata + Divvy API reference
- `docs/knowledge/mermaid-syntax.md` — Mermaid quoting rules + scanner

### Files Deleted
- `docs/knowledge.md` — replaced by `docs/knowledge/` folder

### Files Modified (references updated)
- `AGENTS.md` — line 4, rule 10, repo structure block
- `README.md` — project structure, documentation table
- `chat-history/current-state.md` — file tree, three-doc constraint
- `docs/operations-performed.md` — header reference, TOC added
- `docs/phases/README.md` — architecture and relationship references
- `docs/phases/TEMPLATE.md` — architecture reference
- `docs/phases/phase-1.1-docker.md` — architecture reference
- `docs/phases/phase-1.2-ingestion.md` — architecture reference

## 2026-07-13 — Phase 1.3: Spark Batch Job

### Files Created
- `spark/jobs/crime_batch.py` — Spark batch ETL job. Reads `data/raw/crime/crime_2023.parquet` (263,393 rows), cleans (cast id to long, parse dates to timestamp, uppercase primary_type, cast community_area to int, dedup on id, drop null ids), writes to Postgres `raw.crime_events` via JDBC with `overwrite` mode. Includes built-in verification step (reads back from Postgres and compares row counts).

### Files Modified
- `docker-compose.yml` — Added Postgres env vars to `spark-master` and `spark-worker` services: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST=postgres`, `POSTGRES_PORT=5432`. These are read by `crime_batch.py` via `os.environ` for JDBC credentials. Passed through from `.env` via `${...}` interpolation.

### Infrastructure Changes
- Spark services now have Postgres credentials in their environment, enabling JDBC writes without hardcoding passwords in job scripts
- `raw.crime_events` table created in Postgres: 263,393 rows, 21 columns
  - Schema: `id` (bigint), `case_number` (text), `date` (timestamp), `block` (text), `iucr` (text), `primary_type` (text), `description` (text), `location_description` (text), `arrest` (boolean), `domestic` (boolean), `beat` (text), `district` (bigint), `ward` (double), `community_area` (integer), `fbi_code` (text), `x_coordinate` (text), `y_coordinate` (text), `year` (bigint), `updated_on` (timestamp), `latitude` (double), `longitude` (double)

### Verification
- Spark job ran successfully: 263,393 rows read from Parquet, 0 dropped, 263,393 written to Postgres
- Postgres row count verified: `SELECT count(*) FROM raw.crime_events` → 263,393
- Column types verified via `information_schema.columns` — all casts correct

## 2026-07-13 — Phase 1.4: DBT Models

### Files Created
- `dbt/dbt_project.yml` — DBT project config. Models: staging (view, `staging` schema) + marts (table, `mart` schema). Seeds: `mart` schema.
- `dbt/profiles.yml` — Postgres connection config (host: localhost, user: chicago, db: chicago_analytics). NOT committed to git (credentials).
- `dbt/macros/try_cast.sql` — Warehouse-portable cast macro. Postgres: plain `::` cast (fails loudly). BigQuery: `SAFE_CAST` (returns null).
- `dbt/macros/generate_schema_name.sql` — Overrides DBT's default schema concatenation. Returns custom schema name as-is (e.g. `mart`, not `staging_mart`).
- `dbt/models/staging/stg_crime_events.sql` — Staging view. 1:1 with `raw.crime_events`. Renames columns (id→crime_id, date→occurred_at, updated_on→updated_at), casts types, deduplicates on id via `DISTINCT ON`.
- `dbt/models/staging/schema.yml` — Source definition for `raw.crime_events` + staging model tests (unique, not_null, dbt-expectations range/bounds for community_area_id, lat/long, not_null for arrest/domestic).
- `dbt/models/marts/dim_date.sql` — Date dimension. 365 rows (2023-01-01 to 2023-12-31). Columns: date_key, year, month, day, day_of_week, day_name, month_name, month_start, quarter_start.
- `dbt/models/marts/dim_community_area.sql` — Chicago's 77 community areas. Sourced from seed `community_areas.csv`.
- `dbt/models/marts/dim_crime_type.sql` — 323 distinct primary_type + description combinations. Surrogate key: `primary_type || '|' || description`.
- `dbt/models/marts/fact_crime_events.sql` — Main fact table. 263,393 rows. FKs: date_key→dim_date, community_area_id→dim_community_area, crime_type_key→dim_crime_type.
- `dbt/models/marts/schema.yml` — 20 standard + 11 dbt-expectations data tests (31 total). Standard: unique, not_null, relationships. dbt-expectations: range bounds on year, month, community_area_id, latitude, longitude.
- `dbt/seeds/community_areas.csv` — 77 community areas from Chicago Data Portal (resource `igwz-8jzy`). Columns: community_area_id, community_area_name.
- `dbt/packages.yml` — dbt-expectations package: `metaplane/dbt_expectations` 0.10.10 (Great Expectations macros for dbt). Installed via `dbt deps`.
- `.vscode/settings.json` — dbt Power User extension config: `dbt.allowListFolders: ["dbt"]` (find project in subdirectory), `python.defaultInterpreterPath` and `dbt.dbtPythonPathOverride` (use `.venv/bin/python` where dbt-core is installed).
- `dbt/package-lock.yml` — Auto-generated by `dbt deps`, locks package versions for reproducible installs.
- `.gitignore` (modified) — Added exceptions: `!dbt/seeds/*.csv` (seed must be committable), `!.vscode/settings.json` (extension config must be shared). Added `dbt/profiles.yml` to ignore (contains hardcoded Postgres password).
- `~/.dbt/profiles.yml` (created, outside repo) — Copy of `dbt/profiles.yml` so dbt Power User extension finds profiles at the default location.

### Infrastructure Changes
- `staging` schema now contains `stg_crime_events` view
- `mart` schema now contains: `community_areas` (seed), `dim_community_area`, `dim_crime_type`, `dim_date`, `fact_crime_events`
- DBT installed on host: dbt-core 1.11.12 + dbt-postgres 1.10.2 (via `uv sync`)

### Verification
- `dbt build` — 37/37 PASS (1 seed + 5 models + 31 tests: 20 standard + 11 dbt-expectations, 0 errors, 0 warnings)
- `dbt debug` — connection to Postgres verified (all checks passed)
- Analytical query verified: top 10 community areas by crime count (Austin: 12,700, Near North Side: 11,196, ...)
- Row counts: stg_crime_events=263,393, fact_crime_events=263,393, dim_date=365, dim_community_area=77, dim_crime_type=323
- `changelog.md` — TOC added

## 2026-07-13 — Phase 1.5: Airflow DAG

### What was built
- `airflow/dags/crime_batch_dag.py` — Airflow DAG with 3 tasks: download_crime (BashOperator) → spark_crime_batch (BashOperator, docker exec) → dbt_build (BashOperator, docker run). `schedule=None`, `max_active_runs=1`, `retries=1`.
- `dbt/Dockerfile` — Separate dbt image (python:3.11-slim + dbt-core 1.11.12 + dbt-postgres 1.10.2). Needed because dbt-core 1.11's protobuf >=6.0 conflicts with Airflow 3.0's protobuf 4.x.
- `airflow/dbt_profiles/profiles.yml` — DBT profiles for Airflow container: `host: postgres` (Docker service name), credentials via `env_var()`.
- `dbt-build` service in docker-compose.yml — build-only service (never starts), exists so `docker compose build` builds the dbt image.

### Files Created
- `airflow/dags/crime_batch_dag.py` — DAG file
- `dbt/Dockerfile` — dbt container image
- `airflow/dbt_profiles/profiles.yml` — dbt profiles for in-container use

### Files Modified
- `docker-compose.yml` — added: ingestion/dbt/dbt_profiles mounts, Postgres env vars for Airflow, execution API URL, shared secrets (internal_api, JWT, webserver), dbt-build service
- `airflow/Dockerfile` — added: docker group (GID 1001) for docker.sock access, ingestion deps via requirements.txt
- `airflow/requirements.txt` — added: pandas, pyarrow, requests, python-dotenv. Removed: dbt-core, dbt-postgres (moved to separate image)
- `.env` / `.env.example` — added: `AIRFLOW__CORE__INTERNAL_API_SECRET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__WEBSERVER__SECRET_KEY`
- `ingestion/download_crime.py` — fixed docstring resource ID typo (ijzp-q4t2 → ijzp-q8t2), increased API timeout 60s → 120s

### Infrastructure Changes
- Airflow container now has: docker CLI access (docker.sock), ingestion deps, dbt project + profiles mounted
- Separate dbt image built and available as `chicago-data-pipeline-dbt:latest`
- Airflow 3.0 execution API configured: `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/`
- Shared secrets set for scheduler-webserver mutual auth

### Verification
- DAG parses: `DagBag` loads `crime_batch` with no import errors
- DAG run `manual__2026-07-13T12:55:11...tnc7INKH` — all 3 tasks succeeded:
  - download_crime: success (117s) — Socrata API → Parquet
  - spark_crime_batch: success (32s) — Parquet → Postgres raw.crime_events
  - dbt_build: success (11s) — seed + staging + marts + 37 tests
  - DagRun state: success (137s total)
- Marts queryable: dim_date=365, dim_community_area=77, dim_crime_type=323, fact_crime_events=263,394

## 2026-07-13 — GID Portability Fix + Socrata Credentials

### What was done
Made the hardcoded docker GID in `airflow/Dockerfile` configurable via a build arg, and stored all 4 Socrata API credentials in `.env`.

### Files Modified
- `airflow/Dockerfile` — replaced hardcoded `groupadd -g 1001 docker` with `ARG DOCKER_GID=999` + `groupadd -g ${DOCKER_GID} docker`
- `docker-compose.yml` — added `build.args.DOCKER_GID: ${DOCKER_GID:-999}` under `x-airflow-common`, added `SOCRATA_APP_TOKEN: ${SOCRATA_APP_TOKEN}` to Airflow env
- `.env` — added `DOCKER_GID=1001`, `SOCRATA_APP_TOKEN`, `SOCRATA_API_KEY_ID`, `SOCRATA_API_KEY_SECRET`, `SOCRATA_SECRET_TOKEN`
- `.env.example` — added placeholders for all above with comments
- `chat-history/current-state.md` — updated GID reference, Socrata token status, DockerOperator risk, files tree, chat history table

### Files Created
- `chat-history/2026-07-13/04-gid-portability-and-socrata-credentials.md` — session chunk

### Verification
- Rebuilt Airflow image with `DOCKER_GID=1001` build arg
- Verified `docker:x:1001:airflow` in container `/etc/group`
- Verified docker.sock access: `docker ps` returns container IDs from inside Airflow container

## 2026-07-13 — Phase 1.6: Verification (Phase 1 Gate)

### What was done
Formal end-to-end verification of Phase 1 batch pipeline. Cold-started all services, triggered DAG, verified marts. Two issues discovered and fixed: DBT views blocking Spark overwrite, and missing Airflow 3.0 dag-processor service.

### Files Modified
- `airflow/dags/crime_batch_dag.py` — added `clear_dbt_schemas` task (4th task: drops staging + mart schemas CASCADE before Spark runs)
- `docker-compose.yml` — added `airflow-dag-processor` service (Airflow 3.0 separates DAG processing from scheduler)

### Files Created
- `docs/phases/phase-1.6-verification.md` — phase completion doc
- `chat-history/2026-07-13/05-phase-1.6-verification.md` — session chunk

### Verification
- Cold start: `docker compose down` + `up -d` → all 7 services healthy
- DAG run `manual__2026-07-13T14:11:11...9MkcEDt7` — all 4 tasks succeeded (163s total):
  - download_crime: success (119s)
  - clear_dbt_schemas: success (0.3s)
  - spark_crime_batch: success (34s)
  - dbt_build: success (9s)
  - DagRun state: success
- Marts queryable: dim_date=365, dim_community_area=77, dim_crime_type=323, fact_crime_events=263,394
- fact_crime_events matches raw.crime_events (263,394) — no data loss
- **Phase 1 gate: PASSED. Phase 2 unlocked.**

---

## 2026-07-15 — Phase 2.1: Divvy GBFS Data Source Exploration

### What was done
Explored and documented the Divvy GBFS (General Bikeshare Feed Specification) live API — the streaming data source for Phase 2. Fetched all relevant feeds, analyzed schema, identified quirks that affect pipeline design.

### Files Modified
- `docs/knowledge/data-sources.md` — expanded Divvy GBFS section from 7-line summary to full schema documentation: discovery endpoint, 12 available feeds, station_status fields (12 mandatory + 2 optional), station_information fields, pipeline implications

### Key Findings
- **2,016 stations** in both station_status and station_information (perfect 1:1 match)
- **station_id is mixed format**: 667 UUIDs + 1,349 numeric strings → must stay as string (plan's DBT model had `station_id::bigint` which will fail)
- **is_renting/is_returning/is_installed are integers 0/1**, NOT booleans → need explicit cast
- **Optional fields** (num_scooters_available, num_scooters_unavailable) not in all stations → Spark schema must tolerate absence
- **One dead station** with last_reported=86400 (Jan 2, 1970) → filter stale stations
- **system_regions** has only 1 entry (Evanston) → not useful for community area mapping
- **No community area in GBFS** → will need spatial join of station lat/lon to community area boundaries later
- GBFS version 1.1, TTL 60s, timezone America/Chicago, no auth required

### No files created, no code written
Phase 2.1 is data source exploration only. No Docker services, no producer, no Spark job yet. Those start in Phase 2.2.

---

## 2026-07-15 — Phase 2.2: Kafka + Zookeeper Docker Services

### What was done
Added Kafka and Zookeeper to `docker-compose.yml` as the streaming backbone for Phase 2. Brought up both services, created the `divvy_station_status` topic, verified end-to-end message produce/consume round-trip.

### Files Modified
- `docker-compose.yml` — added 2 new services (zookeeper, kafka) + 3 named volumes (kafka_data, zookeeper_data, zookeeper_log). Updated header comment. Added `KAFKA_BOOTSTRAP_SERVERS: kafka:9092` env var to both spark-master and spark-worker.

### Services Added

| Service | Image | Ports | Healthcheck |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | 2181 (internal only) | `echo srvr \| nc localhost 2181` |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | 9092 (internal), 29092 (host) | `kafka-broker-api-versions --bootstrap-server localhost:9092` |

### Key Configuration
- **Confluent Platform 7.6.0** — free, stable, production-hardened (Bitnami no longer free)
- **Zookeeper mode** (not KRaft) — more educational, traditional setup
- **Two listeners:** `PLAINTEXT://kafka:9092` (Docker network) + `PLAINTEXT_HOST://localhost:29092` (host testing)
- **Single-broker overrides:** replication factor 1 for offsets, transaction state log, and min ISR
- **Auto-create topics enabled** — dev convenience; producer will create `divvy_station_status` on first message
- **3 named volumes** — kafka_data, zookeeper_data, zookeeper_log (data survives restarts)
- **Kafka depends on Zookeeper healthy** before starting

### Verification
- `docker compose config --quiet` — YAML syntax valid
- `docker compose up -d zookeeper kafka` — both started, Zookeeper healthy in ~10s, Kafka healthy in ~40s
- Created topic `divvy_station_status` (3 partitions, replication factor 1) — success
- Produced test JSON message via console producer — success
- Consumed test message back via console consumer — message received correctly
- Produced second message via host listener (localhost:29092) — success, confirms host-side access works
- Deleted test topic to start fresh for Phase 2.3 producer
- Phase 1 services unaffected (were stopped, not running during this test)

---

## 2026-07-15 — Phase 2.3: Kafka Producer

### What was done
Created the Kafka producer script that polls the Divvy GBFS `station_status.json` feed and publishes each station's status as a JSON message to the `divvy_station_status` topic. Verified end-to-end: real Divvy data flowing through Kafka with proper partition distribution.

### Files Created
- `kafka/producers/divvy_producer.py` — Python script that polls GBFS every 60s, publishes each station as JSON to Kafka. Key = station_id (partition ordering), value = full station status JSON. Graceful SIGINT/SIGTERM shutdown. Supports `--once` (single poll), `--interval` (custom cadence), `--bootstrap` (Kafka address).

### Files Modified
- `pyproject.toml` / `uv.lock` — added `kafka-python` 3.0.8 to host venv
- `airflow/requirements.txt` — added `kafka-python` (for Phase 2.6 when DAG runs the producer)
- `docker-compose.yml` — added `./kafka:/opt/airflow/kafka` volume mount to Airflow common config (for Phase 2.6)

### Key Design Decisions
- **Message key = station_id** — ensures same station always goes to same partition, preserving chronological order per station
- **acks="all"** — safest delivery guarantee (waits for all in-sync replicas)
- **Graceful shutdown** — SIGINT/SIGTERM sets a flag, current poll finishes, messages flush, producer closes cleanly
- **Retry with backoff on connect** — Kafka may still be starting up (5 retries, 3s apart)
- **Consistent poll cadence** — sleep time = interval - elapsed, so polls don't drift
- **Explicit topic creation (3 partitions)** — auto-create defaults to 1 partition; `KAFKA_NUM_PARTITIONS` env var didn't work with Confluent image; explicit `kafka-topics --create` is the correct approach

### Verification
- `--once` mode: fetched 2,016 stations, sent 2,016 messages in 0.88s
- Topic auto-created with 1 partition (broker default) → deleted, created explicitly with 3 partitions
- Re-ran `--once`: 2,016 messages distributed across 3 partitions (720 + 661 + 635)
- Console consumer verified real Divvy station data (station_id, bikes, docks, is_renting, etc.)
- Continuous mode (5s interval): 2 polls in 10s, graceful shutdown on SIGTERM
- Total after all tests: 6,048 messages (3 polls × 2,016 stations)
- Topic deleted for clean Phase 2.4 start

## 2026-07-15 — Phase 2.4: Spark Structured Streaming

### What was done
Built the Spark Structured Streaming consumer that reads station status messages from Kafka topic `divvy_station_status`, parses JSON payloads, casts types (int→boolean for is_* fields, epoch→timestamp for last_reported), filters stale stations (last_reported > 1 hour ago), and writes each micro-batch to Postgres `raw.station_status` via foreachBatch + JDBC. Verified end-to-end: producer → Kafka → Spark streaming → Postgres with row count growing continuously.

### Files Created
- `spark/jobs/divvy_stream.py` — Structured Streaming consumer. `readStream.format("kafka")` → `from_json()` with typed schema → cast/filter → `foreachBatch` JDBC write to `raw.station_status`. Supports `--once` (single batch test), `--bootstrap` (custom Kafka address). 60s trigger matches producer poll interval. Checkpoint location at `/opt/spark/checkpoints/divvy_stream` for fault recovery.

### Files Modified
- `spark/Dockerfile` — added 4 Kafka connector JARs (spark-sql-kafka-0-10_2.12-3.5.1, spark-token-provider-kafka-0-10_2.12-3.5.1, kafka-clients-3.5.1, commons-pool2-2.11.1) + checkpoint directory creation with spark user ownership
- `docker-compose.yml` — added `spark_checkpoints` named volume + mounted to spark-master at `/opt/spark/checkpoints`
- Postgres `raw.station_status` table created (18 columns: 14 station data fields + 3 Kafka metadata + 1 ingest timestamp)

### Key Design Decisions
- **4 Kafka JARs baked into image** — apache/spark:3.5.1 ships only core JARs; Kafka connector needs 4 additional JARs. Same pattern as PostgreSQL JDBC driver.
- **foreachBatch bridges streaming→JDBC** — JDBC has no native streaming sink; foreachBatch gives each micro-batch as a static DataFrame for standard JDBC writer
- **Checkpoint via named volume** — `spark_checkpoints` volume persists Kafka offsets across container restarts; without it, restart re-reads all messages from earliest
- **Stale station filter at 1 hour** — 888/2016 stations (44%) had stale `last_reported`; filtering in Spark (not DBT) keeps warehouse clean
- **station_id as StringType** — mixed format (UUIDs + numeric strings); casting to bigint would fail
- **is_* fields cast int→boolean** — GBFS returns 0/1 integers, not booleans; `CAST(col AS BOOLEAN)` in Spark
- **Optional scooter fields nullable** — not all stations have scooters; `from_json` returns null for missing fields
- **Kafka metadata columns** — partition, offset, timestamp stored for traceability

### Verification
- `--once` mode: 2,016 Kafka messages → 1,128 rows inserted (888 stale stations filtered)
- Boolean casts verified: is_renting/is_returning/is_installed show true/false correctly
- Optional scooter fields: 1,099/1,128 non-null (tolerated absence correctly)
- Continuous mode (both producer + streaming): 5 micro-batches in 5 minutes, ~1,128 rows per batch, 5,640 total rows
- Row count grew from 1,128 → 5,640 over 150s, confirming continuous pipeline operation
- Multiple ingest timestamps visible (one per minute per micro-batch)

## 2026-07-16 — Phase 2.5: DBT Stream Models

### What was done
Created DBT staging and mart models for the Divvy station status streaming data. `stg_station_status` is a staging view on `raw.station_status` that renames columns and deduplicates on Kafka coordinates. `fact_station_reads` is a mart table with one row per station poll, a `date_key` FK to `dim_date`, and a derived `total_vehicles_available` column. Updated `dim_date` to span both crime (2023) and station read (2026) dates. All 59 DBT tests pass.

### Files Created
- `dbt/models/staging/stg_station_status.sql` — staging view on `raw.station_status`. Renames `last_reported`→`reported_at`, `ingest_timestamp`→`ingested_at`. Deduplicates on `kafka_partition, kafka_offset` (Kafka message uniqueness). Casts all types explicitly.
- `dbt/models/marts/fact_station_reads.sql` — mart table: one row per station poll. Includes `date_key` (FK to dim_date), `reported_at` (station's self-reported time), `ingested_at` (pipeline receive time), all availability counts, boolean status flags, derived `total_vehicles_available` (bikes + ebikes + scooters with COALESCE on nullable scooters), and Kafka traceability columns. Filters null station_id/reported_at.

### Files Modified
- `dbt/models/marts/dim_date.sql` — now spans both crime + station dates. Uses UNION ALL of min/max from `stg_crime_events` and `stg_station_status`, then `date_bounds` CTE to get overall min/max. Generates 1,292 rows (2023-01-01 through 2026-07-15).
- `dbt/models/staging/schema.yml` — added `station_status` to `raw` source (with column documentation), added `stg_station_status` model definition with tests (not_null on station_id/reported_at/is_renting/is_returning, expect_between 0-100 on bikes/docks).
- `dbt/models/marts/schema.yml` — updated `dim_date` description ("spanning all fact tables") and year test bounds (2023–2026), added `fact_station_reads` model definition with tests (not_null, expect_between, relationships to dim_date).

### Key Design Decisions
- **Dedup on Kafka coordinates** — partition + offset uniquely identifies each Kafka message; same pattern as crime's `DISTINCT ON (id)` but adapted for streaming
- **Column renames for clarity** — `last_reported`→`reported_at` (when station reported), `ingest_timestamp`→`ingested_at` (when pipeline received); matches `occurred_at`/`updated_at` pattern from crime staging
- **Fact grain = one row per station poll** — most granular level, supports any aggregation without pre-aggregation
- **Derived `total_vehicles_available`** — bikes + ebikes + COALESCE(scooters, 0); convenience column for analytics
- **dim_date spans all sources** — UNION ALL of min/max from both fact sources ensures FK relationship tests pass for all fact tables
- **No unique test on station_id** — station_id repeats across polls (unlike crime_id which is unique); grain is station + reported_at

### Verification
- `dbt build` completed: 1 seed, 5 table models, 2 view models, 51 data tests — all 59 passed (PASS=59 WARN=0 ERROR=0 SKIP=0)
- `fact_station_reads`: 5,640 rows, 1,128 unique stations, 5 polls per station
- `dim_date`: 1,292 rows (spanning 2023-01-01 through 2026-07-15)
- Analytics query verified: "avg bikes available per station" returns correct results (top station: 42.0 avg bikes, 75.0 avg total vehicles)
- FK relationship test `fact_station_reads.date_key → dim_date.date_key` passes

## 2026-07-16 — Phase 2.6: Airflow Stream DAG

### What Was Built
- `airflow/dags/divvy_stream_dag.py` — Airflow DAG orchestrating the full streaming lifecycle:
  - `create_topic` — creates `divvy_station_status` Kafka topic with 3 partitions (idempotent via `--if-not-exists`)
  - `start_producer` — runs `divvy_producer.py --once` (single GBFS poll, ~2,016 messages to Kafka, exits cleanly)
  - `start_stream` — starts Spark Structured Streaming as background process in spark-master via `docker exec`
  - `wait_for_data` — polls Postgres every 15s for 5 min until `COUNT(raw.station_status) > INITIAL`
  - `dbt_build` — runs `dbt build` in separate container (stg_station_status + fact_station_reads + all crime models)
  - `stop_stream` — kills Spark background process (trigger_rule=ALL_DONE)
  - `stop_producer` — cleans up PID file (trigger_rule=ALL_DONE, no-op in --once mode)

### Infrastructure Changes
- `airflow/Dockerfile` — switched from `uv pip install --system` (root) to `pip install` (airflow user). Removed uv COPY. The apache/airflow image uses a venv at `/home/airflow/.local` and refuses pip as root.
- `spark/Dockerfile` — added `COPY entrypoint.sh`, `USER root`, `ENTRYPOINT ["/entrypoint.sh"]`. The entrypoint chowns the checkpoint volume before dropping to spark via gosu.
- `spark/entrypoint.sh` — new entrypoint script: `chown -R spark:spark /opt/spark/checkpoints 2>/dev/null || true` then `exec gosu spark "$@"`. Fixes named volume root ownership on every container start.
- `docker-compose.yml` — renamed `./kafka:/opt/airflow/kafka` to `./kafka:/opt/airflow/kafka_scripts`. The old mount path shadowed the `kafka-python` Python package (namespace package collision).

### Key Design Decisions
- **Producer in `--once` mode** — Airflow's BashOperator kills background processes when the task shell exits. `nohup`/`disown` don't reliably survive. `--once` runs in foreground, publishes one batch, exits cleanly. For 24/7 streaming, run producer as a separate Docker service (Phase 3).
- **Spark stream as background process** — started via `docker exec ... nohup ... &` inside spark-master. The `stop_stream` task kills it after DBT build. `trigger_rule=ALL_DONE` ensures cleanup even on failure.
- **`wait_for_data` delta logic** — captures initial row count, polls until `CURRENT > INITIAL`. Ensures we wait for NEW data, not just detect pre-existing rows.
- **Cleanup with `ALL_DONE`** — `stop_stream` and `stop_producer` use `trigger_rule=ALL_DONE` so they run even if upstream tasks fail. No orphaned processes.

### Verification
- All 7 DAG tasks succeeded (create_topic, start_producer, start_stream, wait_for_data, dbt_build, stop_stream, stop_producer)
- `raw.station_status`: 2,001 rows (2,016 Kafka messages, 15 dropped by stale station filter)
- `fact_station_reads`: 2,001 rows, 1,125 unique stations
- Analytics query: avg 5.55 bikes, 2.37 ebikes per station read
- DBT build: all tests pass (streaming + crime models)
- **Phase 2 gate met**: full end-to-end `docker compose up` → Kafka → producer → Spark streaming → Postgres → DBT → queryable marts
---

## 2026-07-18 — Phase 3.1: Grafana

### Why
Phase 3 (Observability) requires Grafana dashboards for pipeline health and analysis. Phase 3.1 is the Grafana foundation — service, datasources, dashboards. DBT tests (3.2) and Airflow SLAs (3.3) will feed metrics into these dashboards.

### Files Created
- `grafana/provisioning/datasources/postgres.yml` — provisions two Postgres datasources: `chicago-analytics` (uid: `chicago-analytics`, database: `chicago_analytics`) + `airflow-metadata` (uid: `airflow-metadata`, database: `airflow_metadata`). Uses shell-style `$VAR` env interpolation (NOT Go templates).
- `grafana/provisioning/dashboards/dashboards.yml` — dashboard provider scanning `/var/lib/grafana/dashboards/` every 30s, loading into "Chicago Pipeline" folder.
- `grafana/dashboards/pipeline_health.json` — 10-panel pipeline health dashboard (uid: `pipeline-health`):
  - 4 stat panels: row counts for raw.crime_events, mart.fact_crime_events, raw.station_status, mart.fact_station_reads
  - 1 time series: stream ingestion rate (rows/hour)
  - 2 stat panels: stream freshness (seconds since last ingest), latest Kafka message timestamp
  - 1 stat panel: DBT tests passing (static placeholder — wired in Phase 3.2)
  - 1 pie chart: Airflow DAG runs (last 7 days, from airflow-metadata datasource)
  - 1 table: recent Airflow task instances (last 2 days, from airflow-metadata datasource)
- `grafana/dashboards/crime_divvy_analysis.json` — 6-panel analysis dashboard (uid: `crime-divvy-analysis`):
  - 1 bar chart: top 15 community areas by crime count
  - 1 pie chart: top 10 crime types
  - 1 time series: avg vehicles available per station (hourly)
  - 1 heatmap: station availability (top 20 stations × hour of day)
  - 1 time series: crime vs Divvy ridership proxy (monthly) — THE DRIVING QUESTION
  - 1 heatmap: crime by day of week × hour of day
- `docs/knowledge/grafana.md` — comprehensive Grafana reference: core concepts (datasource, dashboard, panel, query, provisioning), our setup, file-based provisioning, env var interpolation gotcha, `jsonData.database` deep dive (browser vs API code paths), two-datasource pattern, dashboard inventory, DAG run order (stream first), useful commands, 10 common mistakes, 8 mermaid diagrams
- `docs/phases/phase-3.1-grafana.md` — phase completion doc (4 errors, architecture diagram, verification)

### Files Modified
- `docker-compose.yml` — added `grafana` service (image `grafana/grafana:12.4.0`, port 3000, `grafana_data` named volume, healthcheck on `/api/health`, anonymous Viewer access, env vars for Postgres + Airflow DB creds). Added `grafana_data` to volumes section.
- `.env.example` — added `GRAFANA_ADMIN_USER=admin` + `GRAFANA_ADMIN_PASSWORD=admin` section
- `.env` — appended Grafana admin creds
- `docs/knowledge/index.md` — updated grafana.md entry with expanded description
- `docs/phases/README.md` — added phase-3.1-grafana.md to index
- `README.md` — added Grafana to services table, URLs table, project structure, Phase 3.1 progress section, corrected DAG run order (stream first, then crime batch)
- `grafana/provisioning/datasources/postgres.yml` — added `database:` field inside `jsonData:` block for both datasources (fix for error #4 — browser panels showed "No data")

### Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Grafana version | 12.4.0 (not 13.1.0) | 13.1.0 released 2026-06-23 — 25 days old. 12.4.0 is production-hardened. Matches "stable versions only" rule. |
| Image | `grafana/grafana` (not `grafana-oss`) | `grafana-oss` repo deprecated since 12.4.0. `grafana/grafana` includes Enterprise features, free to use. |
| Two datasources | One per Postgres database | Postgres can't cross-query databases without `postgres_fdw`. Simpler to provision two datasources than set up FDW. |
| File-based provisioning | YAML + JSON, no UI config | Version-controlled, reproducible, no manual UI steps. |
| Anonymous Viewer access | Enabled for local dev | Dashboards viewable without login. Set `GF_AUTH_ANONYMOUS_ENABLED=false` in shared env. |
| DBT tests panel (static) | Placeholder `SELECT 59` | Real DBT test results need artifact parsing — wired in Phase 3.2. |
| Crime-vs-ridership is a proxy | System-wide avg bikes, not per-area | GBFS doesn't include `community_area_id` on stations. Real join needs geospatial matching, deferred to Phase 4+. |

### Verification
- Grafana healthy: version 12.4.0, database ok
- Both datasources provisioned: Chicago Analytics + Airflow Metadata
- Both dashboards loaded: Pipeline Health (10 panels) + Crime + Divvy Analysis (6 panels)
- All 16 panel queries verified against live data (status 200)
- Live data confirmed: 263,401 crime rows, 1,130 station reads, Airflow DAG runs (both DAGs succeeded)
- Browser rendering verified (not just API): panels display live data after `jsonData.database` fix
- DAG run order confirmed: stream first (divvy_stream), then crime batch — eliminates dbt_build race condition

---

## 2026-07-20 — Phase 3.2: DBT Tests

### What Was Built
- Custom singular DBT test for geographic bounds checking
- DBT test results recorder (Python script) that parses `run_results.json` and writes to Postgres
- New `record_dbt_results` Airflow task in both DAGs
- Grafana "DBT tests" panel wired to real test outcomes (was a static placeholder)

### Files Created

| File | Purpose |
|---|---|
| `dbt/tests/assert_crime_in_chicago_bounds.sql` | Singular test — flags crime events with lat/long outside Chicago's bounding box (lat 41.64–42.03, lon -87.95–-87.52). Complements per-column range tests with a combined readable check. |
| `airflow/scripts/record_dbt_results.py` | Parses `dbt/target/run_results.json` after `dbt build`, upserts one row per test into `observability.dbt_test_results`. Idempotent (keyed on invocation_id + test_name). Identifies tests by `unique_id` prefix `test.` (dbt 1.11 has no `resource_type` field). |
| `docs/phases/phase-3.2-dbt-tests.md` | Phase completion doc. |

### Files Modified

| File | Change |
|---|---|
| `docker-compose.yml` | Added `./airflow/scripts:/opt/airflow/scripts` volume mount to `x-airflow-common` anchor (available to all Airflow containers). |
| `airflow/dags/crime_batch_dag.py` | Added `record_dbt_results` BashOperator after `dbt_build`; updated dependency chain to `... >> dbt_build >> record_dbt_results`. |
| `airflow/dags/divvy_stream_dag.py` | Added `record_dbt_results` BashOperator between `dbt_build` and `stop_stream`; updated dependency chain to `... >> dbt_build >> record_dbt_results >> stop_stream >> stop_producer`. |
| `grafana/dashboards/pipeline_health.json` | Rewired panel id 8 ("DBT tests") from static `SELECT 59 AS dbt_tests_passing` to real query against `observability.dbt_test_results` returning passing/failing/warnings counts for the latest invocation. Added field overrides (Passing=green, Failing=red at ≥1, Warnings=neutral). Retitled "DBT test outcomes (latest run)". |

### New Database Object

| Object | Purpose |
|---|---|
| `observability` schema | Dedicated schema for pipeline observability metadata (separate from `raw`/`staging`/`mart`). Created idempotently by `record_dbt_results.py`. |
| `observability.dbt_test_results` table | One row per test per dbt invocation. Columns: `invocation_id`, `generated_at`, `test_name`, `status`, `failures`, `execution_time`, `recorded_at`. PK: `(invocation_id, test_name)`. Queried by the Grafana panel. |

### Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Custom recorder script vs. dbt-artifacts package | Custom 40-line script | No new dbt dependency; project keeps `packages.yml` small. The artifact we need (test outcomes) is tiny — a script is clearer than pulling a package that writes 10+ tables. |
| Observability schema | Dedicated `observability` schema (not `mart` or `raw`) | Pipeline metadata ≠ analytics data. Keeps the mart clean for BI queries and the raw zone clean for ingested data. Created idempotently by the recorder (no init.sql change or volume wipe needed). |
| Test identification | `unique_id.startswith("test.")` | dbt 1.11's `run_results.json` does not populate `resource_type` (it is `None` for every entry). The `unique_id` prefix is the reliable identifier. |
| Test name extraction | Strip `test.chicago_crime.` prefix and trailing `.<hash>` from `unique_id` | The `name` field is `None` in dbt 1.11's run_results. The readable name lives inside `unique_id`. |
| Recorder runs in Airflow container | Not in the dbt container | Airflow already has psycopg2 (via postgres provider). The dbt container is `python:3.11-slim` with only dbt installed. The recorder reads `run_results.json` from the shared `./dbt` mount. |
| Panel shows latest invocation only | `WHERE generated_at = (SELECT max(generated_at) ...)` | The panel answers "is the latest dbt build healthy?" not "what's the history?". History is queryable directly from the table. |

### Verification
- `dbt build` (via divvy_stream DAG): PASS=60 WARN=0 ERROR=0 SKIP=0 TOTAL=60 (1 seed + 7 models + 52 tests)
- `record_dbt_results` task succeeded in both DAGs (crime_batch + divvy_stream)
- `observability.dbt_test_results` populated: 52 tests recorded, all status='pass'
- Singular bounds test `assert_crime_in_chicago_bounds` ran and passed
- Grafana panel query via `/api/ds/query` returns: passing=52, failing=0, warnings=0
- Dashboard loads via `/api/dashboards/uid/pipeline-health` with updated panel title "DBT test outcomes (latest run)"
- 3 dbt invocations recorded total (manual + divvy_stream DAG + crime_batch DAG)
- Top community area by crime: Austin (12,700) — correct

---

## 2026-07-20 — Phase 3.3: Airflow Robustness

### What Was Built
- SqlSensor in `crime_batch` that fixes the race condition with `divvy_stream` (waits for `raw.station_status` to exist before `dbt_build`)
- Shared `on_failure_callback` that logs structured failure context to Airflow logs
- Retries (3x with 5min delay) on all non-cleanup tasks in both DAGs
- `execution_timeout=30min` on `dbt_build` tasks in both DAGs
- `retries=0` on cleanup tasks (stop_stream, stop_producer) — don't retry cleanup
- Airflow Postgres connection (`postgres_default`) via env var for the SqlSensor
- Grafana "Failed tasks (last 7 days)" panel on Pipeline Health dashboard

### Files Created

| File | Purpose |
|---|---|
| `airflow/dags/callbacks.py` | Shared `on_failure_callback` — logs dag_id, task_id, run_id, try_number, exception to Airflow task logs. Imported by both DAGs. |
| `docs/phases/phase-3.3-airflow-robustness.md` | Phase completion doc. |

### Files Modified

| File | Change |
|---|---|
| `docker-compose.yml` | Added `AIRFLOW_CONN_POSTGRES_DEFAULT` env var to `x-airflow-common` anchor. SqlSensor uses this connection to query the analytics warehouse. |
| `airflow/dags/crime_batch_dag.py` | Added `SqlSensor` (`wait_for_stream_data`) between `spark_crime_batch` and `dbt_build`. Updated `default_args` (retries=3, retry_delay=5min, on_failure_callback). Added `execution_timeout=30min` on `dbt_build`. Updated dependency chain. |
| `airflow/dags/divvy_stream_dag.py` | Updated `default_args` (retries=3, retry_delay=5min, on_failure_callback). Added `execution_timeout=30min` on `dbt_build`. Set `retries=0` on `stop_stream` + `stop_producer`. |
| `grafana/dashboards/pipeline_health.json` | Added panel id 11 "Failed tasks (last 7 days)" — queries `task_instance` for failed/upstream_failed states. Moved task instances table (id 10) down to y=30. |

### Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Race condition fix: sensor vs. split dbt models | SqlSensor | `dim_date` legitimately spans both sources (crime + station). Splitting dbt models would break `dim_date`'s UNION ALL. The sensor makes the implicit dependency explicit without splitting models. |
| `execution_timeout` vs `sla=` | `execution_timeout` | Airflow 3.0 removed the SLA feature — `sla=` is a no-op with a deprecation warning. `execution_timeout` actually fails the task if it exceeds the limit. |
| Grafana panel: failed tasks vs SLA misses | Failed tasks | Airflow 3.0 doesn't record SLA misses in `dag_warning` (SLA feature removed). Failed tasks (including timeout failures) are queryable from `task_instance`. |
| `on_failure_callback` vs email alerts | Callback (logging) | No email server in local dev. The callback logs structured failure context to Airflow logs — visible in the UI. In production, this is where Slack/email/PagerDuty would go. |
| `retries=0` on cleanup tasks | Don't retry cleanup | `stop_stream` and `stop_producer` are best-effort cleanup with `trigger_rule=ALL_DONE`. If `kill` fails, the process is already gone. Retrying adds delay without value. |
| SqlSensor `mode="reschedule"` | Not `mode="poke"` | The sensor may wait up to 1hr for `divvy_stream` to run. `reschedule` releases the worker slot between pokes; `poke` holds the slot for the entire wait. |

### Verification
- Both DAGs parse successfully (Airflow `dags list` shows crime_batch + divvy_stream)
- `postgres_default` connection created via env var (verified with `airflow connections get`)
- `callbacks.py` imports successfully from both DAGs
- `divvy_stream` DAG run: all 8 tasks succeeded (create_topic → start_producer → start_stream → wait_for_data → dbt_build → record_dbt_results → stop_stream → stop_producer)
- `crime_batch` DAG run: all 6 tasks succeeded (download_crime → clear_dbt_schemas → spark_crime_batch → wait_for_stream_data → dbt_build → record_dbt_results)
- SqlSensor `wait_for_stream_data` passed immediately (raw.station_status exists from prior divvy_stream run)
- Grafana dashboard loads with 11 panels (was 10 — added "Failed tasks (last 7 days)")
- Failed tasks panel query returns failed_tasks=1 (from the earlier failed SqlSensor attempt before the fix)
- `on_failure_callback` wired via `default_args` — fires when a task exhausts all 3 retries

---

## 2026-07-20 — Phase 3.4: Verification

### What Was Done
- **Verification phase — no new permanent code.** Broke the pipeline in 3 ways and confirmed all observability mechanisms (Grafana panels, DBT tests, Airflow retries + callback) catch the failures.
- Created a throwaway DAG (`verify_failure_dag.py`) with `exit 1` to test task failure handling. Deleted it after verification.
- Pipeline restored to working state after each test: bad row removed, dbt build re-run (PASS=60), recorder updated Grafana.

### Files Created (temporary)

| File | Purpose | Status |
|---|---|---|
| `airflow/dags/verify_failure_dag.py` | Throwaway DAG with `exit 1` task, retries=3, on_failure_callback | **Deleted** after verification |
| `docs/phases/phase-3.4-verification.md` | Phase completion doc | Permanent |

### Verification Scenarios

#### Scenario 1: Stream freshness alert
- **Break:** Producer stopped (divvy_stream DAG completed, no new data flowing)
- **Observability:** Grafana "Stream freshness" panel (id 6) — threshold red at 900s (15min)
- **Result:** Freshness = 1195s (19.9min) > 900s → panel RED ✅
- **Query:** `SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ingest_timestamp))) AS seconds_since_last_ingest FROM raw.station_status;`

#### Scenario 2: DBT test failure
- **Break:** Injected bad crime row into `raw.crime_events` (id=99999999, lat=45.0, lon=-100.0 — South Dakota, outside Chicago bounds 41.64–42.03 / -87.95–-87.52)
- **Observability:** DBT bounds tests in `staging/schema.yml` (latitude + longitude range checks) + Grafana "DBT test outcomes" panel (id 8)
- **Result:** 2 tests failed (`expect_column_values_to_be_between` for latitude + longitude), recorder captured fail=2, Grafana panel showed passing=30 failing=2 → RED ✅
- **Restore:** Deleted bad row, re-ran `dbt build` (PASS=60), ran recorder → Grafana panel back to passing=52 failing=0 → GREEN

#### Scenario 3: Task failure + retries + callback
- **Break:** Throwaway DAG `verify_failure_handling` with `fail_on_purpose` task (`exit 1`), retries=3, retry_delay=10s, on_failure_callback
- **Observability:** Airflow retries (4 attempts) + on_failure_callback (structured log) + Grafana "Failed tasks" panel (id 11)
- **Result:**
  - Task failed after 4 attempts (try_number=4 = 1 initial + 3 retries) ✅
  - `on_failure_callback` fired on final failure, logged: `dag=verify_failure_handling task=fail_on_purpose run=manual__... try=4 exception=None` ✅
  - Grafana "Failed tasks" panel showed failed_tasks=2 (verify task + earlier sensor failure) → RED ✅
- **Restore:** Deleted DAG file, ran `airflow dags delete verify_failure_handling` (removed 5 metadata records)

### Phase 3 Gate — MET
- Grafana shows live row counts and stream freshness ✅ (Phase 3.1)
- Breaking the pipeline (stop the producer) shows up as a Grafana alert within minutes ✅ (Scenario 1 — panel red at 900s threshold)
- DBT tests catch a deliberately introduced data quality issue ✅ (Scenario 2 — 2 tests failed on bad lat/lon)
- Airflow retries a deliberately failing task and alerts on SLA miss ✅ (Scenario 3 — 4 attempts, callback fired, Grafana failed-tasks panel red. Note: Airflow 3.0 removed SLA feature; used execution_timeout + failed-tasks panel instead.)

## 2026-07-21 — Phase 4.1: Warehouse Choice + GCP Project Setup

**Phase 4 (Cloud) started.** Goal: move warehouse to cloud (BigQuery) and answer the driving question (crime vs ridership) using full historical data. Phase 4.1 is the warehouse decision + GCP environment setup — no code written, all infrastructure plumbing.

### Warehouse decision: BigQuery
Chose BigQuery over Snowflake/Redshift. Rationale documented in `docs/knowledge/gcp.md` + `chicago-pipeline-plan.md` §4.1. Key reasons: free tier covers project entirely ($0), serverless (no warehouse sizing/suspend-resume to learn alongside Terraform+Airbyte+partitioning), DBT integration is first-class (one-line adapter switch), dominant cloud warehouse in the market.

### GCP project + auth setup (environment plumbing)
Ran gcloud CLI on **Windows PowerShell** (browser auth needs Windows), moved credentials to WSL (where Terraform + repo live). Full step-by-step + pitfalls documented in `docs/knowledge/gcp.md`.

**Resources created on GCP:**
- Project: `chicago-divvy-pipeline` (ID `480666653891`, name "Chicago Divvy Pipeline")
- Billing account linked: `01A22E-2FC963-7B008D`
- APIs enabled: `bigquery.googleapis.com`, `storage.googleapis.com`, `cloudresourcemanager.googleapis.com`
- Service account: `terraform-runner@chicago-divvy-pipeline.iam.gserviceaccount.com`
- Roles granted: `bigquery.dataOwner`, `bigquery.jobUser`, `storage.admin`, `iam.serviceAccountTokenCreator` (scoped — NOT `roles/owner`)
- Service account key downloaded → `~/chicago-divvy-pipeline-credentials.json` (chmod 600, gitignored)

**Files created/modified:**
- `docs/knowledge/gcp.md` — NEW. Comprehensive GCP reference: auth model (two layers), setup process, WSL vs Windows command differences, pitfalls/risks/cautions, our setup, useful commands.
- `docs/knowledge/index.md` — MODIFIED. Added `gcp.md` row to sections table.
- `.gitignore` — MODIFIED. Added `*-credentials.json`, `credentials.json`, `google-credentials.json` patterns to prevent committing GCP keys.

**Why this matters:** Terraform (Phase 4.2) auths to GCP via the service account key, not via `gcloud auth login`. The service account has scoped roles (least privilege) — if the key leaks, blast radius is limited to BigQuery + Storage, not project deletion. The two-layer auth model (human Gmail for setup, service account for automation) is the core concept.

**Errors hit:** 2 (documented in changelog + gcp.md) — (1) `~` not expanded by gcloud (Python tool treats it as literal path), (2) `\` line continuation fails in PowerShell (uses backtick, not backslash).

### Next — Phase 4.2: Terraform
Write `terraform/main.tf` + `providers.tf` + `variables.tf` + `terraform.tfvars` to provision: `google_bigquery_dataset.raw`, `google_bigquery_dataset.mart`, `google_storage_bucket.data_lake`. Local state first; migrate to GCS backend later.

## 2026-07-21 — Phase 4.2: Terraform (BigQuery + GCS provisioning)

**Phase 4.2 complete.** Wrote Terraform config to provision the cloud containers (2 BigQuery datasets + 1 GCS bucket), ran `terraform init`/`plan`/`apply`, verified resources exist.

### Files created
- `terraform/providers.tf` — Google provider config (v7.40.0 pinned `~> 7.40`). Auths via service account key file path (variable), NOT `gcloud auth login`.
- `terraform/variables.tf` — 4 input variables: `project_id`, `region` (default `US`), `credentials_path` (sensitive), `bucket_name`.
- `terraform/main.tf` — 3 resources: `google_bigquery_dataset.raw`, `google_bigquery_dataset.mart`, `google_storage_bucket.data_lake`. Key decisions: `delete_contents_on_destroy=true` (learning project, re-run from scratch), `force_destroy=true` on bucket, 90-day lifecycle auto-delete on bucket objects, `uniform_bucket_level_access=true`.
- `terraform/terraform.tfvars` — actual values (GITIGNORED — environment-specific).
- `terraform/terraform.tfvars.example` — template to copy from (committable).

### Files modified
- `.gitignore` — added Terraform patterns (`terraform/.terraform/`, `*.tfstate`, `*.tfstate.*`, `terraform.tfvars`, `*.tfvars.local`, `crash.log`). Removed older duplicate block.
- `docs/knowledge/gcp.md` — expanded with 2 new WSL pitfalls (#5 separate WSL auth state, #6 least-privilege SA can't list APIs), Terraform section (file structure, workflow, key decisions, what was created), gsutil deprecation note, updated useful commands.
- `docs/knowledge/index.md` — updated `gcp.md` description.

### Resources created on GCP (verified)
| Resource | Verified via |
|---|---|
| `google_bigquery_dataset.raw` | `bq ls` → shows `raw` |
| `google_bigquery_dataset.mart` | `bq ls` → shows `mart` |
| `google_storage_bucket.data_lake` (`gs://chicago-divvy-pipeline-data-lake`) | `gsutil ls` → shows bucket |

### Errors hit
1. **WSL gcloud had separate config + auth state from Windows** — `gcloud config set project` in PowerShell didn't carry to WSL. WSL was still authed as the old `dtc-de-course` service account. Fix: `gcloud auth activate-service-account --key-file=/home/sagar/chicago-divvy-pipeline-credentials.json` in WSL (non-interactive, key-based — same way CI/CD auths).
2. **`~` not expanded by gcloud (again)** — `gcloud auth activate-service-account --key-file=~/...` failed with `No such file or directory: '~/...'`. Fix: use `/home/sagar/...` explicit path.
3. **`gcloud services list` failed with AUTH_PERMISSION_DENIED** — expected, not a bug. The SA's scoped roles don't include `serviceusage.services.list` (admin role). Least privilege working as designed. Verified APIs from personal Gmail in PowerShell instead; used `bq ls` + `gsutil ls` (permissions ARE in SA roles) for resource verification.

### Next — Phase 4.3: Architecture change (DONE)
Rewired the batch pipeline from local Postgres to GCS/BigQuery. See Phase 4.3 section below.

## 2026-07-21 — Phase 4.3: Architecture Change (Postgres → GCS/BigQuery)

### What was built
Migrated the batch analytics path from local Postgres to GCP (GCS + BigQuery). The streaming path (Divvy GBFS) stays on local Postgres — only the crime batch path moved to cloud. The pipeline is now: `download_crime.py` → Spark (Parquet → GCS) → `bq load` (GCS → BigQuery) → DBT (BigQuery marts) → record_dbt_results (Postgres observability).

### Files created
- `airflow/scripts/bq_load_crime.py` — NOT created (used `bq` CLI in BashOperator instead, consistent with existing patterns).

### Files modified
- `spark/Dockerfile` — added `gcs-connector-hadoop3-latest.jar` to `/opt/spark/jars/` (GCS connector for Spark).
- `spark/jobs/crime_batch.py` — rewrote sink: `write_to_postgres()` → `write_to_gcs()` (Parquet to `gs://chicago-divvy-pipeline-data-lake/raw/crime/`). Updated verification to read back from GCS.
- `docker-compose.yml` — added GCP env vars (`GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT_ID`, `GCS_BUCKET`, `BIGQUERY_LOCATION`) + credentials volume mount to spark-master, spark-worker, and `x-airflow-common` anchor (Airflow services).
- `airflow/Dockerfile` — added Google Cloud SDK installation block (apt repo + `google-cloud-cli` package) for `bq` CLI.
- `airflow/requirements.txt` — added `google-cloud-bigquery` (Python fallback for bq CLI).
- `airflow/dags/crime_batch_dag.py` — rewrote tasks: new `bq_load_crime` (GCS Parquet → BigQuery via `bq load`), removed `clear_dbt_schemas` + `wait_for_stream_data` sensor, updated `dbt_build` with `--exclude stg_station_status fact_station_reads` + GCP env passthrough, updated docstring.
- `dbt/Dockerfile` — switched from `dbt-postgres==1.10.2` to `dbt-bigquery==1.12.0`.
- `dbt/profiles.yml` — rewrote for BigQuery adapter (service-account key auth, host keyfile path).
- `airflow/dbt_profiles/profiles.yml` — rewrote for BigQuery adapter (container keyfile path).
- `dbt/macros/try_cast.sql` — added BigQuery branch with Postgres→BigQuery type mapping (`int`→`INT64`, `timestamp`→`TIMESTAMP`, etc.) + `SAFE_CAST`.
- `dbt/models/staging/stg_crime_events.sql` — `DISTINCT ON` → `QUALIFY ROW_NUMBER()`, `::type` → `SAFE_CAST`/`CAST`.
- `dbt/models/marts/dim_date.sql` — dropped station_status UNION (crime dates only), `generate_series` → `GENERATE_DATE_ARRAY + UNNEST`, `TO_CHAR` → `FORMAT_TIMESTAMP`, `EXTRACT(dow FROM)` → `EXTRACT(DAYOFWEEK FROM)`.
- `dbt/models/marts/dim_community_area.sql` — `::int`/`::text` → `CAST(... AS INT64/STRING)`.
- `dbt/models/marts/fact_crime_events.sql` — `::date` → `DATE()` (idiomatic BigQuery).
- `dbt/models/staging/schema.yml` — updated source comments for BigQuery, added `station_status` source back (for parsing only — excluded from build).
- `dbt/models/marts/schema.yml` — updated `dim_date` description + year test bounds (2023 only).
- `.env` + `.env.example` — added GCP section (`GCP_CREDENTIALS_PATH`, `GCP_PROJECT_ID`, `GCS_BUCKET`, `BIGQUERY_LOCATION`).

### Data flow (Phase 4.3)
```mermaid
graph LR
    A[Socrata API] -->|download_crime.py| B[Parquet /data/raw/crime/]
    B -->|Spark crime_batch.py| C[GCS gs://...data-lake/raw/crime/]
    C -->|bq load --replace| D[BigQuery raw.crime_events]
    D -->|DBT build| E[BigQuery mart.* tables]
    E -->|record_dbt_results.py| F[Postgres observability.dbt_test_results]
    F -->|Grafana query| G[DBT tests panel]
```

### Verification (full DAG run — all 5 tasks succeeded)
The DAG was triggered and all 5 tasks succeeded end-to-end:

| Task | Result | Duration |
|---|---|---|
| `download_crime` | ✅ Socrata API → Parquet (passed on retry after transient timeout) | ~2min |
| `spark_crime_batch` | ✅ 263,402 rows written to GCS Parquet | ~1.5min |
| `bq_load_crime` | ✅ 263,403 rows loaded into BigQuery `raw.crime_events` | ~11s |
| `dbt_build` | ✅ 38/38 tests pass. Marts: dim_date (365), fact_crime_events (263,403), dim_community_area (77), dim_crime_type (323) | ~30s |
| `record_dbt_results` | ✅ 32 test results recorded into Postgres `observability.dbt_test_results` | <1s |

Also tested each task individually via `airflow tasks test` during debugging (before the full DAG run succeeded).

### Errors hit
1. **Docker credential helper error** (`docker-credential-desktop.exe: exec format error`) — WSL2 Docker config pointed to Windows exe. Fix: `"credsStore": ""` in `~/.docker/config.json`.
2. **`bq` CLI not found in Airflow container** — stale image. Compose generated separate image names per service. Fix: `docker compose build --no-cache airflow-scheduler` + `--force-recreate`.
3. **`bq load` auth failure** ("no active account selected") — `bq` CLI doesn't read `GOOGLE_APPLICATION_CREDENTIALS` like the Python client. Fix: `gcloud auth activate-service-account --key-file=...` before `bq load` in the DAG task.
4. **Credentials file permission denied** — `chmod 600` owned by host UID 1000, Airflow runs as UID 50000. Fix: `chmod 644` on host (still gitignored).
5. **DBT compilation error** ("source raw.station_status not found") — `--exclude` prevents building but not parsing. Fix: added `station_status` source back to `schema.yml` for parsing.

### Next — Phase 4.4: Divvy trip history (Airbyte S3 → BigQuery)
Use Airbyte to ingest Divvy trip history from S3 into BigQuery. Then update `dim_date` to span crime + Divvy trip dates, and build the final mart that answers the driving question.


## 2026-07-22 — Phase 4.4: Divvy Trip History + Correlation Analysis

### Why
Phase 4.4 is the final phase that answers the driving question: "Does crime near a Divvy station affect ridership?" It ingests full Divvy trip history from S3, switches crime data to the BigQuery public dataset, builds the geospatial analytics mart, and produces the correlation coefficient.

### Files Created
- `ingestion/load_divvy_trips.py` — dlt S3→BigQuery ingestion script. Reads monthly ZIP files from `divvy-tripdata.s3.amazonaws.com`, extracts CSV, streams rows to BigQuery `raw.divvy_trips` (append mode). CLI: `--month YYYYMM`, `--from/--to YYYYMM`, `--all`, `--dry-run`.
- `airflow/dags/divvy_trip_history_dag.py` — 3-task DAG: `load_divvy_trips` (BashOperator runs dlt script) → `dbt_build_divvy` (DockerOperator runs dbt build) → `record_dbt_results` (BashOperator runs recorder script).
- `dbt/models/staging/stg_divvy_trips.sql` — Staging view on `raw.divvy_trips`. Casts types (SAFE_CAST for lat/lng, try_cast for timestamps), filters null ride_id/started_at + out-of-bounds coordinates (Montreal row).
- `dbt/models/marts/dim_stations.sql` — Station dimension from trip data. Groups by `start_station_id`, picks most common station name (`ANY_VALUE HAVING MAX`) + most common coordinate pair (`ROW_NUMBER` by occurrence count). Includes `ST_GEOGPOINT` for geospatial joins.
- `dbt/models/marts/fact_divvy_trips.sql` — Divvy trip fact table. Partitioned by `started_at` (daily), clustered by `start_station_id`. Includes derived `trip_duration_seconds` = `TIMESTAMP_DIFF(ended_at, started_at, SECOND)`.
- `dbt/models/marts/fact_station_day.sql` — THE analytics mart. One row per station per day. `trip_count` from `fact_divvy_trips`, `crime_count_within_quarter_mile` from geospatial join: `ST_DISTANCE(ST_GEOGPOINT(crime.lng, crime.lat), station.geo_point) <= 402` (0.25 mile). Partitioned by `date_key`, clustered by `station_id`.
- `dbt/models/marts/crime_ridership_correlation.sql` — CORR(trip_count, crime_count_within_quarter_mile) at three scopes: overall, per_station (≥30 days), per_month (≥30 station-days). UNION ALL result with scope label.
- `grafana/provisioning/datasources/bigquery.yml` — BigQuery datasource for Grafana (uid: `bigquery-analytics`, project: `chicago-divvy-pipeline`).
- `docs/phases/phase-4.4-divvy-trip-history.md` — Phase completion doc.
- `docs/knowledge/dlt.md` — dlt reference doc.

### Files Modified
- `airflow/requirements.txt` — Added `dlt[bigquery]`.
- `airflow/dags/crime_batch_dag.py` — Simplified to 2 tasks: `dbt_build` → `record_dbt_results`. Removed `download_crime`, `spark_crime_batch`, `bq_load_crime` (crime now sourced from `bigquery-public-data.chicago_crime` directly in DBT).
- `dbt/models/staging/schema.yml` — Added `chicago_crime_public` source (database: `bigquery-public-data`, schema: `chicago_crime`, table: `crime`). Added `divvy_trips` to `raw` source. Added `stg_divvy_trips` model with tests (unique ride_id, not_null, coordinate bounds).
- `dbt/models/staging/stg_crime_events.sql` — Rewrote to read from `bigquery-public-data.chicago_crime.crime`. Column mapping: `unique_key`→`crime_id`, `date`→`occurred_at`, `updated_on`→`updated_at`. Filter: `year >= 2018` + Chicago coordinate bounds (lat 41.64–42.03, lon -87.95–-87.52). Dedup: `QUALIFY ROW_NUMBER() OVER(PARTITION BY unique_key ORDER BY updated_on DESC) = 1`.
- `dbt/models/marts/dim_date.sql` — UNION of min/max dates from both `stg_crime_events` + `stg_divvy_trips` (2018–2026).
- `dbt/models/marts/fact_crime_events.sql` — Added `c.primary_type` to SELECT (needed for `cluster_by`). Partitioned by `date_key`, clustered by `community_area_id` + `primary_type`.
- `dbt/models/marts/schema.yml` — Added `dim_stations`, `fact_divvy_trips`, `fact_station_day`, `crime_ridership_correlation` model definitions with tests. Updated `dim_date` year test bounds (2018–2026).
- `grafana/dashboards/crime_divvy_analysis.json` — Added panel 7 (scatter plot: trip_count vs crime_count_within_quarter_mile, last 30 days) + panel 8 (gauge: overall Pearson correlation from `crime_ridership_correlation`). Updated description + tags.
- `docker-compose.yml` — Added `GF_INSTALL_PLUGINS=grafana-bigquery-datasource` to Grafana env. Mounted GCP credentials at `/opt/grafana/gcp-credentials.json:ro`.

### Data flow (Phase 4.4)
```mermaid
graph LR
    S3[Divvy S3 Bucket] -->|dlt| BQ_RAW[BigQuery raw.divvy_trips 34.8M rows]
    PUB[bigquery-public-data.chicago_crime] -->|stg_crime_events| STG[Staging Views]
    BQ_RAW -->|stg_divvy_trips| STG
    STG --> FSD[fact_station_day 1.5M rows]
    FSD --> CORR[crime_ridership_correlation]
    CORR -->|Grafana| PANELS[Scatter + Gauge]
```

### Verification
- **dlt sample test:** June 2023 → 719,618 rows loaded into `raw.divvy_trips` ✅
- **Full ingestion:** 34,751,413 rows across 75 months (2020-04 to 2026-06) ✅
- **DBT build:** 67/67 tests pass (PASS=67 WARN=0 ERROR=0 SKIP=0) ✅
- **Partition pruning:** Filtered query (Jan 2024) scans 254K bytes vs 11.7M full scan = 2.17% (97.8% savings) ✅
- **Correlation:** Overall Pearson r = +0.2003 (n=1,463,049). Per-month range: 0.08–0.31. Per-station range: NaN–0.85. ✅
- **Grafana:** Scatter plot + correlation gauge panels added with BigQuery datasource ✅

### Key Finding
**Overall Pearson correlation = +0.20** — weak positive correlation between daily trip count and crime count within 402m of a station. This does NOT mean crime causes ridership. Both are higher in busy, densely populated areas. The confounding variable is urban activity level. Per-month correlations trend upward from 0.08 (Apr 2020, COVID lockdown) to ~0.25-0.30 (2024-2025).

### Errors hit
1. **Airflow containers running old image** — `--force-recreate` didn't pick up new dlt image. Fix: `docker compose down` + `docker compose build` + `docker compose up -d`.
2. **Coordinate test failures** — Public crime dataset had 4 rows in Missouri; Divvy had 1 row in Montreal. Fix: Added coordinate bounds filters in staging models.
3. **`cluster_by` column missing from SELECT** — `fact_crime_events` clustered by `primary_type` but only selected `crime_type_key`. Fix: Added `c.primary_type` to SELECT.
4. **Missing FROM clause** — Edit accidentally removed `FROM {{ source() }}` from `stg_divvy_trips`. Fix: Re-added the line.
5. **Column name mismatch in correlation CTE** — `overall` CTE used `total_station_days` but SELECT referenced `station_day_count`. Fix: Renamed for consistency.
6. **Stale dimension tables** — `dim_date` and `dim_crime_type` not rebuilt because `--select +fact_station_day` didn't include them in parent graph. Fix: Full `dbt build --exclude stg_station_status fact_station_reads`.

### Next — Phase 5: CI/CD (GitHub Actions + GHCR)
Branch protection, PR checks (ruff + dbt parse + compose validate), versioned releases, image push to GHCR.

---

## 2026-07-22 — Phase 4.8: BigQuery ML (stretch goal)

### What was built
- **BQML linear regression model** (`mart.crime_ridership_model`) trained via dbt post_hook. Predicts daily Divvy trip_count from crime_count_within_quarter_mile, day_of_week, month, and station_id (fixed effect).
- **4 new DBT mart models:**
  - `crime_ridership_model_training_data` — 815K rows (2020-2023), table + post_hook that `CREATE MODEL`s the BQML model
  - `crime_ridership_model_evaluation` — `ML.EVALUATE` on auto-split validation set (R²=0.434, MAE=13.4)
  - `crime_ridership_model_weights` — `ML.WEIGHTS` (5 rows: intercept + 4 features). Crime coefficient = +1.45
  - `crime_ridership_predictions` — `ML.PREDICT` on 648K 2024+ test rows
- **DBT schema tests:** 13 tests added for BQML models (not_null, range checks on features, not_null on predictions)
- **Airflow DAGs updated:** `divvy_trip_history_dag.py` `--select` includes BQML models; `crime_batch_dag.py` `--exclude` excludes them
- **Knowledge doc:** `docs/knowledge/bigquery-ml.md` — BQML syntax, dbt post_hook integration, gotchas
- **Phase doc:** `docs/phases/phase-4.8-bigquery-ml.md`

### Files created/modified
| File | Action |
|---|---|
| `dbt/models/marts/crime_ridership_model_training_data.sql` | Created |
| `dbt/models/marts/crime_ridership_model_evaluation.sql` | Created |
| `dbt/models/marts/crime_ridership_model_weights.sql` | Created |
| `dbt/models/marts/crime_ridership_predictions.sql` | Created |
| `dbt/models/marts/schema.yml` | Modified (added 4 model definitions + tests) |
| `airflow/dags/divvy_trip_history_dag.py` | Modified (--select + comments + docstring) |
| `airflow/dags/crime_batch_dag.py` | Modified (--exclude + comments) |
| `docs/knowledge/bigquery-ml.md` | Created |
| `docs/phases/phase-4.8-bigquery-ml.md` | Created |

### Data flow (Phase 4.8)
```mermaid
graph TB
    FSD["fact_station_day 1.5M rows"] --> TRAIN["training_data 815K rows (2020-2023)"]
    TRAIN -->|post_hook| MODEL["BQML: crime_ridership_model linear_reg"]
    MODEL --> EVAL["model_evaluation ML.EVALUATE"]
    MODEL --> WEIGHTS["model_weights ML.WEIGHTS"]
    FSD -->|2024+ test| PRED["predictions ML.PREDICT 648K rows"]
    MODEL --> PRED
```

### Verification
- **DBT build:** 17/17 tests pass (PASS=17 WARN=0 ERROR=0 SKIP=0) ✅
- **In-sample R²:** 0.434 (MAE=13.4 trips) ✅
- **Seen-station out-of-sample R²:** 0.447 (MAE=11.4 — temporal generalization works for known stations) ✅
- **Crime coefficient:** +1.45 (positive — confirms correlation finding) ✅
- **Predictions:** 647,577 rows with predicted_trip_count ✅

### Key Finding
Crime coefficient = +1.45 — each additional crime near a station predicts 1.45 more trips, even after controlling for station identity, day of week, and month. Confirms Phase 4.4: the crime-ridership relationship is positive (confounded by urban activity), not negative. The model generalizes to new time periods for known stations (R²=0.447) but fails on stations that opened after 2023 (R²=-199K) — the high-cardinality station fixed effect has no learned weight for unseen stations.

### Errors hit
1. **`not_null` test failed on `model_weights.weight`** — BQML returns NULL weight for categorical features (per-category weights in `category_weights` JSON). Fix: removed not_null test on `weight`.
2. **Out-of-sample R² = -173,642** — `no_split` + `ML.EVALUATE` on 2024+ data; 50% of test rows are unseen stations. Fix: switched to `auto_split` (default) for in-sample evaluation.

### Next — Phase 5: CI/CD (GitHub Actions + GHCR)
Branch protection, PR checks (ruff + dbt parse + compose validate), versioned releases, image push to GHCR.

---

## 2026-07-22 — Data Inventory Verification

### What was checked
End-to-end verification of all data sources actually loaded in BigQuery + Postgres. Queried each table directly for row counts and date ranges.

### BigQuery — ALL PRESENT ✅
- `staging.stg_crime_events`: 2,073,670 rows (2018–2026) — from `bigquery-public-data.chicago_crime`
- `mart.fact_crime_events`: 2,073,670 rows (2018–2026)
- `raw.divvy_trips`: 34,751,413 rows (2020–2026) — dlt from S3, 75 monthly files
- `mart.fact_divvy_trips`: 34,751,412 rows (2020–2026)
- `mart.fact_station_day`: 1,463,049 rows (2020–2026) — geospatial join
- `mart.crime_ridership_correlation`: ~3,200 rows — CORR() at 3 scopes
- `mart.crime_ridership_model_training_data`: 815,472 rows (2020–2023)
- `mart.crime_ridership_model_evaluation`: 1 row — ML.EVALUATE
- `mart.crime_ridership_model_weights`: 5 rows — ML.WEIGHTS
- `mart.crime_ridership_predictions`: 647,577 rows (2024–2026)

### Postgres — PARTIAL ⚠️
- `observability.dbt_test_results`: ✅ Present
- `raw.station_status`: ❌ Missing — streaming table, run `divvy_stream` DAG to populate
- `raw.crime_events`: ❌ Missing — Phase 1 Socrata extract, analytics use BigQuery public dataset

### Key facts documented
- Analytics overlap window: 2020-04 to 2026-06 (6+ years)
- Crime filtered to `year >= 2018` (public dataset has 2001-present, ~8.6M total)
- Divvy trips: full history loaded (all 75 months available on S3)
- Streaming tables (Postgres) are architecturally separate from analytics (BigQuery)
- Updated `docs/knowledge/data-sources.md` with full "Current Data Inventory" section
- Updated `chat-history/current-state.md` with data inventory table + corrected stale row counts

---

## 2026-07-22 — dbt docs generate + serve

### What was built
- Ran `dbt docs generate` against BigQuery — produced `catalog.json` (15 models, 4 sources, 89 tests, 1 seed, 871 macros). Introspects warehouse for column types, table metadata.
- Started `dbt docs serve` in a Docker container on port 8090 (8080 conflicts with Airflow). Interactive HTML docs at http://localhost:8090.
- Catalog verified: all Phase 4.4 + 4.8 models present (crime_ridership_correlation, fact_station_day, fact_divvy_trips, dim_stations, all 4 BQML models).

### Files created/modified
| File | Action |
|---|---|
| `dbt/target/catalog.json` | Generated (warehouse introspection metadata) |
| `dbt/target/manifest.json` | Updated (by every dbt build — model/test/source graph) |
| `docs/knowledge/dbt.md` | Modified — added "dbt docs" section with commands, port, BigQuery gotcha |
| `chat-history/current-state.md` | Modified — added dbt docs URL to URLs section |

### How to run
```bash
# Generate (after dbt build so manifest.json is fresh)
docker run --rm --network chicago-data-pipeline_default --volumes-from chicago-data-pipeline-airflow-scheduler-1 \
  -e GOOGLE_APPLICATION_CREDENTIALS -e GCP_PROJECT_ID \
  chicago-data-pipeline-dbt:latest \
  dbt docs generate --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt_profiles

# Serve (port 8090, 8080 conflicts with Airflow)
docker run --rm --network chicago-data-pipeline_default --volumes-from chicago-data-pipeline-airflow-scheduler-1 \
  -p 8090:8090 -e GOOGLE_APPLICATION_CREDENTIALS -e GCP_PROJECT_ID \
  chicago-data-pipeline-dbt:latest \
  dbt docs serve --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt_profiles --port 8090 --host 0.0.0.0
```

### Verification
- `curl http://localhost:8090/` → HTTP 200 ✅
- Catalog contains 15 nodes including all BQML models ✅
- `RuntimeWarning: "table_owner" does not match` — harmless (BigQuery has no table owners) ✅