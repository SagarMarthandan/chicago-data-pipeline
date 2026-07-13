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