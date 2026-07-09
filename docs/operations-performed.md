# Operations Performed

A chronological log of operations, files created, and structural changes made to this repo. Explains *what* exists and *why* — not errors (those go in `changelog.md`) or reference material (that goes in `knowledge.md`).

> **Format:** `YYYY-MM-DD` — what was done, what was created, and the reasoning.

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

### `airflow/Dockerfile` (created, updated to Airflow 3.0)
- Based on `apache/airflow:3.0.0-python3.11` (upgraded from 2.8.4 — 2.x is EOL since April 2026)
- Installs `docker.io` (Docker CLI) for DockerOperator — official image doesn't include it
- Copies uv binary from `ghcr.io/astral-sh/uv:latest` via multi-stage COPY (no install script needed)
- Installs Airflow providers from `airflow/requirements.txt` using `uv pip install --system` (10-100x faster than pip)
- Uses `--system` flag (installs into container's system Python, no venv needed in containers)
- Uses `uv pip install` not `uv sync` because host and containers need different packages

### `airflow/passwords.json` (created)
- Airflow 3.0 SimpleAuthManager passwords file
- JSON mapping of username → password: `{"admin": "admin"}`
- Mounted into container at `/opt/airflow/config/passwords.json` via docker-compose.yml
- Replaces Airflow 2.x's `airflow users create` CLI command (removed in 3.0)

### `airflow/requirements.txt` (created)
- `apache-airflow-providers-postgres` — PostgresHook, SqlSensor
- `apache-airflow-providers-docker` — DockerOperator

### `spark/Dockerfile` (created)
- Based on `bitnami/spark:3.5`
- Downloads PostgreSQL JDBC driver (`postgresql-42.7.3.jar`) into `/opt/bitnami/spark/jars/`
- Needed for Spark to write to Postgres via JDBC (`df.write.format("jdbc")`)

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
