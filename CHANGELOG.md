# Changelog

A running log of changes, errors, and fixes throughout the project. Use this to spot patterns in mistakes and avoid repeating them.

> **Format:** `YYYY-MM-DD` — what happened, what broke, what fixed it, and the lesson.

## Table of Contents

- [2026-07-08 — Project Setup (Windows → WSL migration)](#2026-07-08--project-setup-windows--wsl-migration)
- [2026-07-08 — Documentation & Repo Finalization](#2026-07-08--documentation--repo-finalization)
- [2026-07-09 — Phase 1.1 Docker Setup (started)](#2026-07-09--phase-11-docker-setup-started)
- [2026-07-09 — Phase 1.1 init.sql created](#2026-07-09--phase-11-initsql-created)
- [2026-07-09 — Schema architecture decision (3-layer)](#2026-07-09--schema-architecture-decision-3-layer)
- [2026-07-09 — docker-compose.yml + Dockerfiles created](#2026-07-09--docker-composeyml--dockerfiles-created)
- [2026-07-09 — Migrated from uv venv to uv init](#2026-07-09--migrated-from-uv-venv-to-uv-init-project-mode)
- [2026-07-09 — uv pip install in Airflow Dockerfile](#2026-07-09--uv-pip-install-in-airflow-dockerfile)
- [2026-07-09 — Upgraded Airflow 2.8.4 → 3.0.0](#2026-07-09--upgraded-airflow-284--300)
- [2026-07-09 — Bitnami Spark image not found](#2026-07-09--bitnami-spark-image-not-found--switched-to-apachespark)
- [2026-07-09 — Airflow Dockerfile permission denied](#2026-07-09--airflow-dockerfile-permission-denied-during-uv-pip-install)
- [2026-07-09 — Airflow 3.0 runtime breaking changes](#2026-07-09--airflow-30-runtime-breaking-changes-webserver-scheduler-health-permissions)
- [2026-07-11 — Phase 1.2: Ingestion script errors](#2026-07-11--phase-12-ingestion-script-errors)
- [2026-07-11 — Mermaid diagram rendering errors](#2026-07-11--mermaid-diagram-rendering-errors-across-md-files)
- [2026-07-13 — Phase 1.3: Spark batch job](#2026-07-13--phase-13-spark-batch-job)

- [2026-07-13 — Phase 1.4: DBT Models](#2026-07-13--phase-14-dbt-models)
- [2026-07-13 — Phase 1.5: Airflow DAG](#2026-07-13--phase-15-airflow-dag)
- [2026-07-15 — Phase 2.1: Divvy GBFS data source exploration](#2026-07-15--phase-21-divvy-gbfs-data-source-exploration)
- [2026-07-15 — Phase 2.2: Kafka + Zookeeper Docker services](#2026-07-15--phase-22-kafka--zookeeper-docker-services)
- [2026-07-15 — Phase 2.3: Kafka producer](#2026-07-15--phase-23-kafka-producer)
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
- [2026-07-22 — Phase 4.8: BigQuery ML (stretch goal)](#2026-07-22--phase-48-bigquery-ml-stretch-goal)
- [2026-07-22 — Data Inventory Verification](#2026-07-22--data-inventory-verification)
- [2026-07-22 — dbt docs generate + serve](#2026-07-22--dbt-docs-generate--serve)
- [2026-07-22 — Phase 5: CI/CD GitHub Actions workflows](#2026-07-22--phase-5-cicd-github-actions-workflows)

---

## 2026-07-08 — Project Setup (Windows → WSL migration)

### Changes
- Migrated project planning files from Windows (`C:\Users\sagar\Documents\chicago-data-pipeline\`) to WSL (`~/chicago-data-pipeline/`)
- Flattened folder structure — moved `AGENTS.md`, `chicago-pipeline-plan.md`, and `docs/` to repo root (was nested inside a `devin/` subfolder)
- Renamed repo root from `chicago-divvy-DE-project` → `chicago-data-pipeline` to match `COMPOSE_PROJECT_NAME`
- Initialized git repo, default branch `main`
- Created `.gitignore`, `README.md` with Mermaid diagrams

### Errors & Fixes

| # | Error | Root Cause | Fix | Lesson |
|---|---|---|---|---|
| 1 | `TRY_CAST(... AS timestamp)` fails in Postgres | `TRY_CAST` is Snowflake/DuckDB syntax, doesn't exist in Postgres | Created `try_cast` DBT macro that dispatches per-warehouse (`::` on Postgres, `SAFE_CAST` on BigQuery) | Always check which SQL dialect your warehouse supports. Don't assume syntax transfers between databases. |
| 2 | `EXTRACT(date FROM c.occurred_at)` errors in Postgres | `date` is not a valid `EXTRACT` field in Postgres | Replaced with `c.occurred_at::date` | Postgres `EXTRACT` only accepts specific fields (year, month, day, hour, etc.). For date casting, use `::date`. |
| 3 | `try_cast` macro silently falls to else branch | Used `target_var.adapter` which isn't a valid DBT Jinja variable | Changed to `adapter.type() == 'postgres'` / `'bigquery'` | DBT Jinja uses `adapter.type()`, not `target_var.adapter`. Test macros with `dbt run` to verify dispatch works. |
| 4 | Docker Compose network names unpredictable with mixed-case folder name | Compose derives project name from directory name; `chicago-divvy-DE-project` has uppercase `DE` | Set fixed `COMPOSE_PROJECT_NAME=chicago-data-pipeline` in `.env` | Always set `COMPOSE_PROJECT_NAME` explicitly. Don't rely on directory-name derivation, especially with mixed-case paths. |
| 5 | Bash commands failed with "Working directory does not exist" | Tool cwd was set to old folder path after rename | Used `cd ~/chicago-data-pipeline && ...` with absolute paths | After renaming the working directory, always verify cwd exists before running commands. Use absolute paths as a safety net. |

### Lessons Summary
- **SQL portability:** Never assume SQL syntax works across warehouses. Test on your actual target.
- **DBT Jinja:** `adapter.type()` is the correct dispatch method, not `target_var.*`.
- **Docker Compose:** Set `COMPOSE_PROJECT_NAME` explicitly — don't rely on folder name derivation.
- **WSL paths:** Keep the repo on the WSL filesystem (`~/`), not `/mnt/c/`. Cross-filesystem mounts are slow.
- **Tool cwd:** After folder renames, verify your working directory before running commands.

---

## 2026-07-08 — Documentation & Repo Finalization

### Changes
- Created `changelog.md` (repo root) — error/fix/lesson log, pre-populated with 5 planning-phase bugs
- Created `docs/knowledge.md` — reference lookup by tool (WSL, Docker, Postgres, DBT, Spark, Kafka, Airflow, Git, data sources)
- Created `docs/phase/ (absorbed)` — structural audit trail of what was built and why
- Created `README.md` with 3 Mermaid diagrams (architecture, pipeline flow, roadmap)
- Updated `AGENTS.md` — added rules 9–11 (read changelog, read knowledge, update operations-performed), updated header note and repo structure to reference all three docs
- Updated `docs/wiki/conventions/airflow.md` — DockerOperator network/volume names → `chicago-data-pipeline_*` with `COMPOSE_PROJECT_NAME` reference
- Updated `docs/wiki/conventions/docker.md` — added `COMPOSE_PROJECT_NAME=chicago-data-pipeline` to `.env` example and networking section; updated WSL path to `~/chicago-data-pipeline`
- Fixed missing ` ``` ` code fence in `AGENTS.md` repo structure block (lost during edit)

### Errors & Fixes

| # | Error | Root Cause | Fix | Lesson |
|---|---|---|---|---|
| 6 | `AGENTS.md` repo structure block lost opening ` ``` ` fence | Edit replaced the line containing the fence without re-adding it | Re-inserted ` ``` ` before the tree | When editing around code fences, verify both opening and closing fences survive the edit. Read after editing. |
| 7 | README.md Mermaid diagrams didn't persist after incremental edits | Edits applied but file state was inconsistent | Rewrote entire file with `write` tool in one pass | For multi-section files with complex content, use `write` (full overwrite) instead of chained `edit` calls. Verify with `grep -c mermaid` after. |
| 8 | Bash commands failed with "Working directory does not exist" after folder rename | Tool cwd was cached to old `chicago-divvy-DE-project` path | Prefixed commands with `cd ~/chicago-data-pipeline && ...` | Same lesson as #5 — always verify cwd after folder renames. Use absolute paths. |

### Lessons Summary
- **Edit tool pitfalls:** Code fences and structural elements can be lost during `SWAP` edits. Always read after editing to verify.
- **Write vs edit:** For complex multi-section files, `write` (full overwrite) is more reliable than chained `edit` calls.
- **Three-doc system:** `changelog.md` (errors), `docs/knowledge.md` (reference), `docs/phase/ (absorbed)` (audit trail) — each has a distinct purpose, don't merge them.

---

## 2026-07-09 — Phase 1.1 Docker Setup (started)

### Changes
- Created `.env.example` — environment variable template with Postgres credentials (warehouse + Airflow metadata DB), Airflow config (LocalExecutor, UI creds), `COMPOSE_PROJECT_NAME`, and Socrata API token placeholder

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Airflow executor | `LocalExecutor` | Parallelism without extra containers. SequentialExecutor is too slow even for dev. CeleryExecutor needs Redis/RabbitMQ — overkill for Phase 1. |
| Database architecture | Two DBs in one Postgres instance | `chicago_analytics` (warehouse) + `airflow_metadata` (Airflow internal state). Same container, different DBs/users. Cheaper than two containers, fine for local. |
| Socrata token | Left empty | Not needed until ingestion script (Phase 1.2). Documented now so it's not forgotten. |

### Lessons
- **Airflow needs its own DB** — pointing Airflow at your analytics DB pollutes it with `task_instance`, `dag_run`, etc. Always separate.
- **`.env.example` is committed, `.env` is gitignored** — template documents required vars, real secrets stay local.
- **DockerHub image names are NOT env vars** — they go in `docker-compose.yml` under `image:`, not in `.env`. `.env` is for secrets and environment-specific config only.

---

## 2026-07-09 — Phase 1.1 init.sql created

### Changes
- Created `init.sql` — Postgres init script that creates `raw` + `mart` schemas in `chicago_analytics`, and `airflow_metadata` database + `airflow` user for Airflow's internal state

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Hardcoded values vs shell script | Hardcoded in `init.sql` | SQL files can't read `.env` vars. A `.sh` script could, but adds complexity. Values match `.env.example` and init only runs once. |
| `DO $$ ... $$` block for user creation | Used anonymous function | Postgres has no `CREATE USER IF NOT EXISTS`. The DO block checks `pg_roles` before creating. |
| `\gexec` for database creation | Used psql meta-command | `CREATE DATABASE` can't run inside a transaction (which `IF NOT EXISTS` requires). `\gexec` executes the generated string as a separate command. |

### Lessons
- **Init scripts run once** — only when the Postgres data volume is empty. Changing `init.sql` after first run requires `docker compose down -v` (destroys data).
- **Postgres has no `CREATE DATABASE IF NOT EXISTS`** — must use workarounds like `\gexec` or check `pg_database` manually.
- **`CREATE DATABASE` can't run in a transaction** — this is a Postgres limitation, not a bug. It's why the `\gexec` trick is needed.

---

## 2026-07-09 — Schema architecture decision (3-layer)

### Changes
- Updated `init.sql` — added `staging` schema (was only `raw` + `mart`)

### Key Decision

| Decision | Choice | Why |
|---|---|---|
| Schema architecture | 3 schemas: `raw`, `staging`, `mart` (skip `intermediate`) | Traditional DBT layering. `raw` = Spark/Kafka landing zone, `staging` = DBT light cleaning/renaming/casting, `mart` = final facts + dims. Skipped `intermediate` schema to keep it simpler — can add later if joins/aggregations need their own layer. |

### Lesson
- **Postgres schemas vs DBT layers are different concepts** — Postgres schemas are physical namespaces in the database. DBT layers are logical transformation stages (folders in your dbt project). You can have 3 DBT model layers mapped to 3 Postgres schemas, or all DBT output in one schema. Schema-per-layer gives clearer separation and finer-grained access control.

---

## 2026-07-09 — Phase 1.1 docker-compose.yml + Dockerfiles created

### Changes
- Created `docker-compose.yml` — 6 services (postgres, spark-master, spark-worker, airflow-init, airflow-webserver, airflow-scheduler)
- Created `airflow/Dockerfile` — custom Airflow image with docker CLI + postgres/docker providers
- Created `airflow/requirements.txt` — provider packages for Airflow
- Created `spark/Dockerfile` — custom Spark image with PostgreSQL JDBC driver baked in
- Created `airflow/dags/.gitkeep` and `spark/jobs/.gitkeep` — directory placeholders

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Spark UI port | Remapped to 8180 | Spark master Web UI defaults to 8080, which conflicts with Airflow webserver (also 8080) |
| Spark JDBC driver | Baked into image via Dockerfile | More reliable than `--packages` at runtime (works offline, faster startup, no Maven Central dependency) |
| Airflow DockerOperator | docker CLI installed + docker.sock mounted | DockerOperator needs to talk to the Docker daemon to spawn Spark containers. Without docker.sock, it can't create containers. |
| YAML anchors (`x-airflow-common`) | Shared config across 3 Airflow services | Avoids repeating 10+ lines of env vars and volumes 3 times. `<<: *airflow-common` merges the anchor. |
| `airflow-init` as one-shot service | Runs migrations + creates user, then exits | webserver + scheduler use `depends_on: condition: service_completed_successfully` to wait for init. `|| true` on user create handles re-runs. |
| `$$` in init command | Double dollar sign | Compose interpolates `$VAR`. `$$` escapes to literal `$` so bash reads from environment instead. |
| Spark worker resources | 2G memory, 2 cores | Enough for ~8M crime rows. User's i7-7700HQ has 4 cores/8 threads — leaves resources for Postgres + Airflow. |
| `DAGS_ARE_PAUSED_AT_CREATION=False` | New DAGs start unpaused | Convenient for dev. In production, you'd want True to review before running. |

### Lessons
- **Port conflicts** — Spark UI and Airflow both default to 8080. Always check for port collisions before bringing up multiple services. Remap with `"host_port:container_port"`.
- **DockerOperator needs docker.sock** — Airflow runs inside a container but needs to create OTHER containers. Mounting `/var/run/docker.sock` bridges the Airflow container to the host's Docker daemon.
- **`$$` vs `$` in Compose** — Compose interprets `$VAR` as variable interpolation from `.env`. To pass a literal `$` to the container's shell (for bash variable expansion), use `$$VAR`.
- **`service_completed_successfully`** — a `depends_on` condition for one-shot init services. Unlike `service_healthy` (for long-running services), this waits for the init container to exit with code 0.

---

## 2026-07-09 — Migrated from uv venv to uv init (project mode)

### Changes
- Removed `.venv/` and `requirements.txt` (old uv venv approach)
- Ran `uv init --bare --name chicago-data-pipeline` — created `pyproject.toml`
- Ran `uv add requests sodapy dbt-core dbt-postgres python-dotenv psycopg2-binary` — populated dependencies + generated `uv.lock`
- Verified: all imports work, `dbt --version` → 1.11.12

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| uv venv vs uv init | `uv init` (project mode) | Lockfile (`uv.lock`) guarantees reproducible installs. `pyproject.toml` is the modern Python standard (PEP 621). `uv add` is cleaner than manually editing `requirements.txt`. |
| Docker + uv | Independent — containers keep using pip | uv manages host Python only. Containers have their own Python. Can switch containers to uv later if build speed becomes a bottleneck. |

### Lesson
- **Lockfile vs requirements.txt** — `requirements.txt` resolves versions at install time (can vary between machines). `uv.lock` pins exact versions + hashes, guaranteeing identical installs everywhere. For a project meant to be documented and reproducible, the lockfile is the right choice.
- **Docker and uv are independent** — uv on the host doesn't affect containers. Each container has its own Python managed by its Dockerfile. You CAN use uv inside Docker (faster builds), but it's optional.

---

## 2026-07-09 — uv pip install in Airflow Dockerfile

### Changes
- Updated `airflow/Dockerfile` — replaced `pip install` with `uv pip install --system`
- Added `COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv` — multi-stage copy of uv binary

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| uv in Docker | `uv pip install --system` (not `uv sync`) | Host and containers need different packages. `uv sync` reads root `uv.lock` (host deps) — would install dbt-core, sodapy etc. in Airflow container unnecessarily. `uv pip install -r airflow/requirements.txt` installs only container-specific deps. |
| How to install uv in container | `COPY --from=ghcr.io/astral-sh/uv:latest` | Multi-stage copy — pulls just the binary, no install script, no pip install uv. Cleaner and more reliable than curl \| sh. |
| `--system` flag | Used | Installs into container's system Python. No venv needed inside containers — they're already isolated. |

### Lesson
- **Multi-stage COPY for tools** — `COPY --from=<image>:<tag> /path/to/binary /local/path` copies a single binary from another image without installing it. Common pattern for adding tools (uv, docker CLI, etc.) to containers.

---
## 2026-07-09 — Upgraded Airflow 2.8.4 → 3.0.0

### Changes
- Updated `airflow/Dockerfile` — `apache/airflow:2.8.4-python3.11` → `apache/airflow:3.0.0-python3.11`
- Updated `docker-compose.yml` — removed `airflow users create` from airflow-init, added SimpleAuthManager env vars + passwords.json mount
- Updated `.env.example` — removed `AIRFLOW_WWW_USER`/`AIRFLOW_WWW_PASSWORD`, added `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` + `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`
- Created `airflow/passwords.json` — JSON mapping of username → password for SimpleAuthManager

### Why Airflow 2.x is EOL
Airflow 2.x reached end-of-life in April 2026. The final 2.x release was 2.11.2 (March 2026). No more security patches or bug fixes. Airflow 3.0.0 (April 2025) is the first stable 3.x release with 15 months of production hardening.

### Breaking Changes from 2.x → 3.0

| Change | 2.x | 3.0 | Impact |
|---|---|---|---|
| Authentication | Flask-AppBuilder (FAB) | SimpleAuthManager (new default) | `airflow users create` CLI is GONE. Users defined via env vars + passwords.json |
| User creation | `airflow users create --username ... --password ...` | `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` + passwords.json | No CLI user creation. Users defined in config. |
| Passwords | Database-backed | JSON file (`passwords.json`) | Mount file into container, define username→password mapping |
| Roles | Created via CLI | Predefined: viewer, user, op, admin | Assigned in `SIMPLE_AUTH_MANAGER_USERS` env var |
| `airflow db migrate` | Works | Still works | No change |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Works | Still works (core components only) | No change for our setup |

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Airflow version | 3.0.0 (not 3.3.0) | 3.0.0 has 15 months of production hardening. 3.3.0 released 3 days ago — too new for stability. |
| Auth manager | SimpleAuthManager (default) | Simpler than FAB for dev. No database-backed users. If we need `airflow users create` later, can install `apache-airflow-providers-fab` and switch to FabAuthManager. |
| Passwords file | `airflow/passwords.json` mounted into container | Static, predictable password (`admin`/`admin`). SimpleAuthManager auto-generates passwords if file doesn't exist — mounting gives us control. |

### Lessons
- **Always check version status before pinning** — Airflow 2.8.4 was EOL. The plan was written when 2.x was current. Version currency matters.
- **Airflow 3.0 is a major breaking change** — not a drop-in upgrade. Auth, user management, and some config paths changed. Always read migration docs.
- **SimpleAuthManager is dev-oriented** — it's the default for 3.0 but designed for development/testing. For production, FabAuthManager (via `apache-airflow-providers-fab`) restores database-backed auth.

---

## 2026-07-09 — Bitnami Spark image not found → switched to apache/spark

### Error
`docker compose build` failed: `bitnami/spark:3.5: not found`

### Root Cause
Bitnami moved their Docker images behind a commercial subscription ("Bitnami Secure Images") in 2026. The free `docker.io/bitnami/*` images are no longer available on Docker Hub.

### Fix
Switched from `bitnami/spark:3.5` to `apache/spark:3.5.1` (official Apache Spark image, free, actively maintained).

### Breaking Changes (bitnami → apache/spark)

| Concept | bitnami/spark | apache/spark |
|---|---|---|
| Start master | `SPARK_MODE=master` env var | `spark-class org.apache.spark.deploy.master.Master` command |
| Start worker | `SPARK_MODE=worker` + `SPARK_MASTER_URL` env vars | `spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077` command |
| SPARK_HOME | `/opt/bitnami/spark` | `/opt/spark` |
| JDBC jar path | `/opt/bitnami/spark/jars/` | `/opt/spark/jars/` |
| Jobs mount path | `/opt/bitnami/spark/jobs/` | `/opt/spark/jobs/` |
| Non-root user | UID 1001 | `spark` (UID 185) |
| RPC/SSL env vars | `SPARK_RPC_AUTHENTICATION_ENABLED=no` etc. | Not needed (defaults are open) |

### Additional Changes
- Added `SPARK_MASTER_HOST=spark-master` env var — tells master to advertise the Docker service name so workers can resolve it. Without this, master advertises a random container hostname.
- Healthcheck switched from bash `/dev/tcp` to `python3` socket check — more portable across base images.
- Worker resources (`SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=2G`) still work as env vars — `spark-class` startup scripts read them.

### Lesson
- **Bitnami images are no longer free** — as of 2026, Bitnami moved behind a commercial subscription. Always verify Docker image availability before pinning. The official `apache/spark` image is the upstream source, free, and actively maintained.
- **Different Spark images have different interfaces** — Bitnami wrapped Spark with env var config (`SPARK_MODE`). The official image uses raw `spark-class` commands. When switching base images, expect config interface changes, not just path changes.

---

## 2026-07-09 — Airflow Dockerfile permission denied during uv pip install

### Error
`uv pip install --system` failed: `failed to create directory /usr/local/lib/python3.11/site-packages/markdown_it_py-4.2.0.dist-info: Permission denied (os error 13)`

### Root Cause
The Dockerfile switched to `USER airflow` (UID 50000) before running `uv pip install --system`. The airflow user doesn't have write access to `/usr/local/lib/python3.11/site-packages/` — that directory is owned by root.

### Fix
Run `uv pip install --system` as root, then switch to `USER airflow` for the final image. The Dockerfile now stays as root through both `apt-get install` and `uv pip install`, then switches to airflow at the end.

### Lesson
- **`--system` installs need root** — `uv pip install --system` writes to the system Python's site-packages directory, which is owned by root. If you switch to a non-root user before this command, it will fail with permission denied. Install packages as root, then switch to the runtime user for the final image.

---

## 2026-07-09 — Airflow 3.0 runtime breaking changes (webserver, scheduler, health, permissions)

### Errors
Four issues discovered during `docker compose up`:

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Spark master unhealthy | Healthcheck checked RPC port 7077 on `127.0.0.1`, but Spark binds RPC to container's Docker network IP (172.18.0.x), not localhost. Web UI (8080) binds to 0.0.0.0. | Changed healthcheck to check port 8080 (Web UI) instead of 7077 (RPC) |
| 2 | Airflow webserver crashes: `airflow command error: arguments required` | Airflow 3.0 removed `airflow webserver` command. Replaced by `airflow api-server`. | Changed `command` to `api-server` |
| 3 | Airflow scheduler crashes: same error | Airflow 3.0 image has no default CMD. Without explicit `command: scheduler`, the entrypoint runs `airflow` with no subcommand. | Added `command: scheduler` |
| 4 | Airflow webserver: `PermissionError: /opt/airflow/config/passwords.json` | SimpleAuthManager opens passwords.json with `a+` mode (read+write). File was root-owned, airflow user (UID 50000) couldn't write. | `chmod 666 airflow/passwords.json` on host |
| 5 | Healthcheck 404 on `/health` | Airflow 3.0 moved health endpoint to `/api/v2/monitor/health` | Updated healthcheck URL |
| 6 | `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` deprecated | Airflow 3.0 moved port config from `[webserver]` to `[api]` section | Changed env var to `AIRFLOW__API__PORT` |

### Airflow 3.0 Breaking Changes Summary (runtime)

| Concept | Airflow 2.x | Airflow 3.0 |
|---|---|---|
| Web UI command | `airflow webserver` | `airflow api-server` |
| Scheduler command | Default CMD in image | Explicit `command: scheduler` needed |
| Health endpoint | `/health` | `/api/v2/monitor/health` |
| Port config section | `[webserver]` | `[api]` |
| Port env var | `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` | `AIRFLOW__API__PORT` |

### Lessons
- **Airflow 3.0 is NOT a drop-in upgrade from 2.x** — beyond auth changes, the webserver command, health endpoint, config sections, and default CMD all changed. Always test with `docker compose up` after upgrading, not just build.
- **Spark master binds RPC to Docker network IP, not localhost** — the Web UI binds to 0.0.0.0 but the RPC port binds to the container's specific IP. Healthchecks inside the container should check the Web UI port, not the RPC port.
- **Bind-mounted files need permissions for the container user** — when mounting a file from host into a container, the file's host permissions carry over. If the container user (UID 50000) needs write access, `chmod 666` on the host.

---

## 2026-07-11 — Phase 1.2: Ingestion script errors

### Errors

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Socrata API 404: `dataset.missing` for resource `ijzp-q4t2` | The plan had a typo — the correct resource ID is `ijzp-q8t2` (with an 8, not a 4). The dataset was migrated to a new ID on the Chicago Data Portal. | Queried Socrata catalog API, found correct ID `ijzp-q8t2`, updated `SOCRATA_URL` in `download_crime.py` |
| 2 | `NameError: name 'time' is not defined` | `import time` was accidentally consumed during an edit that replaced it with a misplaced `SOCRATA_URL` line | Re-added `import time` to the imports block |
| 3 | Spark can't read Parquet — `data/` not mounted | `docker-compose.yml` only mounted `./spark/jobs` into Spark containers, not `./data` | Added `./data:/opt/spark/data` to spark-master, spark-worker, and airflow-common volumes |

### Lessons
- **Always verify API endpoints before writing code** — the plan's resource ID (`ijzp-q4t2`) was a typo. The Socrata catalog API (`api.us.socrata.com/api/catalog/v1?q=...`) can confirm the correct ID.
- **Socrata API returns nested `location` dict and `:@computed_region_*` columns** — the `location` column duplicates `latitude`/`longitude`, and computed region columns are internal geocoding metadata. Both should be dropped during cleaning.
- **Data directory must be mounted into Spark containers** — Parquet files written by the host ingestion script need to be accessible inside Spark containers via a bind mount.

---

## 2026-07-11 — Mermaid diagram rendering errors across .md files

### Errors

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Parallelogram shape collision — `[/text/]` interpreted as parallelogram, not text | Mermaid treats `[/.../]` as a parallelogram shape syntax, not a label with slashes | Quoted all labels containing `/`: `NODE["/path/..."]` |
| 2 | Slash-bracket ending — `text/]` breaks rendering | `/]` without quotes confuses the parser | Quoted all labels ending with `/`: `NODE["name/"]` |
| 3 | `${VAR}` in labels breaks parsing | Curly braces are special syntax in mermaid | Quoted labels and edge labels containing `${...}` |
| 4 | `$$` in edge labels breaks parsing | Double dollar signs are special syntax | Quoted edge labels: `\|"$$VAR..."\|` |
| 5 | Unquoted colons in node labels break rendering | Colons in `NODE[text:text]` confuse some mermaid parsers | Quoted all labels containing `:`: `NODE["text:text"]` |

**Files affected:** `docs/knowledge.md` (9 diagrams), `docs/phase/phase-1.1-docker.md` (1 diagram), `README.md` (3 diagrams)

### Lessons
- **Always quote mermaid node labels containing special characters** — `/`, `:`, `$`, `{`, `}` all break rendering when unquoted. The safe rule: if a label contains anything other than letters, numbers, spaces, and `<br/>`, wrap it in double quotes.
- **Edge labels with special chars need quotes too** — use `-->|"label with : or /"|` not `-->|label with : or /|`
- **Built a scanner to catch these** — the Python scanner in the eval cell checks all `.md` files for unquoted problematic patterns. Run it after adding any new mermaid diagram.

---

## 2026-07-13 — Phase 1.3: Spark batch job

### Changes
- Created `spark/jobs/crime_batch.py` — Spark batch ETL: reads Parquet → cleans → writes to Postgres `raw.crime_events` via JDBC
- Updated `docker-compose.yml` — added Postgres env vars (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`) to both `spark-master` and `spark-worker` services so JDBC credentials are available inside containers

### Errors & Fixes

| # | Error | Root Cause | Fix | Lesson |
|---|---|---|---|---|
| 1 | `spark-submit: executable file not found in $PATH` | apache/spark image doesn't add `/opt/spark/bin` to PATH for `docker compose exec` | Use full path: `/opt/spark/bin/spark-submit` | The apache/spark image (unlike bitnami) doesn't put Spark binaries on PATH. Always use the full path `/opt/spark/bin/spark-submit` when exec'ing into the container. |
| 2 | Duplicate `environment:` block in docker-compose.yml for spark-worker | Edit tool replaced only part of the old block, leaving a stale duplicate | Deleted the stale lines, merged all env vars under one `environment:` key | When using edit tool on YAML, verify the full service block after editing — partial replacements can leave orphaned keys that silently override each other. |
| 3 | `spark-worker:` service key dropped during edit | The SWAP operation consumed the service header lines | Re-inserted `spark-worker:`, `build: ./spark`, `restart: unless-stopped` before the `environment:` block | Always verify service keys exist after editing docker-compose.yml — a missing service key silently drops the entire service. |
| 4 | `raw.crime_events` table missing after WSL restart | WSL `--shutdown` + Docker restart didn't preserve Spark-written tables (the table is created by the job, not by `init.sql`) | Re-ran the batch job (idempotent via `mode("overwrite")`) | Spark-written tables are not part of `init.sql` — they're created at job runtime. If the volume is wiped or container recreated, re-run the job. `overwrite` mode makes this safe. |

### Lessons
- **apache/spark PATH:** The official `apache/spark` image doesn't add `/opt/spark/bin` to PATH. Use `/opt/spark/bin/spark-submit` explicitly.
- **Idempotent batch jobs:** `mode("overwrite")` means the job is safe to re-run anytime — it replaces the whole table. This is the Phase 1 pattern; Phase 2+ will use upserts.
- **Docker Compose env propagation:** Spark executors run on workers, not just the master. Both services need Postgres credentials for JDBC writes in cluster mode.
- **Data persistence:** Named volumes preserve `init.sql` output (schemas, users) but NOT Spark-written tables. Those are application data, created at runtime.

---

## 2026-07-13 — Phase 1.4: DBT models

### Changes
- Created `dbt/` project: `dbt_project.yml`, `profiles.yml`, `macros/`, `models/staging/`, `models/marts/`, `seeds/`, `tests/`
- `macros/try_cast.sql` — warehouse-portable cast macro (Postgres `::` cast, BigQuery `SAFE_CAST`)
- `macros/generate_schema_name.sql` — overrides DBT's schema concatenation so models go to `staging` and `mart` schemas (not `staging_staging`/`staging_mart`)
- `models/staging/stg_crime_events.sql` — 1:1 with `raw.crime_events`, renames columns, casts types, deduplicates on `id` via `DISTINCT ON`
- `models/staging/schema.yml` — source definition for `raw.crime_events`
- `models/marts/dim_date.sql` — date dimension (365 rows, generated from min/max crime dates)
- `models/marts/dim_community_area.sql` — Chicago's 77 community areas from seed
- `models/marts/dim_crime_type.sql` — 323 distinct primary_type + description combinations
- `models/marts/fact_crime_events.sql` — main fact table (263,393 rows) with FKs to all dims
- `models/staging/schema.yml` + `models/marts/schema.yml` — 31 tests: 20 standard (unique, not_null, relationships) + 11 dbt-expectations (range bounds, value sets)
- `seeds/community_areas.csv` — 77 community areas from Chicago Data Portal (resource `igwz-8jzy`)
- `packages.yml` — `metaplane/dbt_expectations` 0.10.10 (Great Expectations macros for dbt)
- `.vscode/settings.json` — dbt Power User extension config (`dbt.allowListFolders`, `dbt.dbtPythonPathOverride`)
- `.gitignore` updated — added exceptions for `!dbt/seeds/*.csv`, `!.vscode/settings.json`; added `dbt/profiles.yml` to ignore (contains hardcoded password)
- `~/.dbt/profiles.yml` — copy of `dbt/profiles.yml` for dbt Power User extension (default profiles location)
- `dbt build` passes: 1 seed + 5 models + 31 tests = 37/37 PASS

### Errors & Fixes

| # | Error | Root Cause | Fix | Lesson |
|---|---|---|---|---|
| 1 | DBT created `staging_mart` and `staging_staging` schemas instead of `mart` and `staging` | DBT's default `generate_schema_name` concatenates profile schema + custom schema (`staging` + `_` + `mart` = `staging_mart`) | Created `macros/generate_schema_name.sql` override that returns the custom schema name as-is | DBT's default schema naming concatenates, doesn't replace. Always override `generate_schema_name` when you want models in specific named schemas. |
| 2 | `where` config warning in DBT 1.11 | `where` as a top-level property of relationships test is deprecated | Moved `where` under `config:` in the test definition | DBT 1.11 deprecates top-level `where` on tests. Use `config: where: "..."` instead. |
| 3 | DBT not installed despite being in `pyproject.toml` | `uv sync` was run previously but packages weren't fully installed | Ran `uv sync` again — resolved and installed dbt-core 1.11.12 + dbt-postgres 1.10.2 | Always verify with `dbt --version` after `uv sync`. `pyproject.toml` lists intent; `uv sync` makes it real. |
| 4 | `expect_column_values_to_be_in_set` on BOOLEAN columns fails: `operator does not exist: boolean = text` | dbt-expectations generates a text comparison (`v.value_field = s.value_field`) that Postgres can't compare to boolean | Replaced with `not_null` — a Postgres BOOLEAN column can only hold true/false/null, so the in-set test adds no value | Don't use `expect_column_values_to_be_in_set` on Postgres BOOLEAN columns. The type mismatch is unfixable without casting, and the test is meaningless since BOOLEAN can't hold other values. |
| 5 | `expect_column_values_to_be_between` on longitude failed (801 rows) | Bounds `[-87.9, -87.5]` were too tight — actual data ranges `[-87.94, -87.52]` | Widened to `[-87.95, -87.52]` based on actual `min()/max()` from the data | Always check actual data bounds with `SELECT min(), max()` before setting range test thresholds. Chicago's city limits extend slightly beyond the commonly cited rounded values. |
| 6 | dbt Power User extension "dbt language server is not running" | Extension couldn't find `dbt_project.yml` (in `dbt/` subdirectory, not workspace root) and `profiles.yml` (in `dbt/`, not `~/.dbt/`) | Created `.vscode/settings.json` with `"dbt.allowListFolders": ["dbt"]` and copied `profiles.yml` to `~/.dbt/` | dbt Power User scans workspace root by default. For dbt projects in subdirectories, set `dbt.allowListFolders` in `.vscode/settings.json` and ensure `profiles.yml` is in `~/.dbt/`. |

### Lessons
- **DBT schema naming:** The default `generate_schema_name` macro concatenates the profile schema with the custom schema. Override it when you want models in specific named schemas (`staging`, `mart`).
- **DBT 1.11 test config:** The `where` clause on generic tests must be nested under `config:`, not as a top-level property.
- **Staging dedup:** Use `DISTINCT ON (id) ... ORDER BY id, updated_at DESC` in Postgres for deduplication that keeps the most recently updated row per id.
- **Community area 0:** `community_area_id = 0` means "unassigned" in the crime data — it's not a real community area. The relationships test uses `where: "community_area_id != 0"` to exclude it from referential integrity checks.
- **Seed data:** Chicago's 77 community areas are available via Socrata API at resource `igwz-8jzy` (Boundaries - Community Areas). Selected `area_numbe` and `community` columns for the seed CSV.
- **dbt-expectations on BOOLEAN:** `expect_column_values_to_be_in_set` fails on Postgres BOOLEAN columns due to type mismatch. Use `not_null` instead — BOOLEAN can't hold values outside {true, false, null}.
- **Data bounds validation:** Always check actual `min()/max()` before setting range test thresholds. Chicago's longitude ranges `[-87.94, -87.52]`, wider than the commonly cited `[-87.9, -87.5]`.
- **dbt Power User extension:** Needs `dbt_project.yml` discoverable via `dbt.allowListFolders` in `.vscode/settings.json` and `profiles.yml` in `~/.dbt/` (default location).

---

<!-- Append new entries below. Keep the format consistent. -->

## 2026-07-13 — Phase 1.5: Airflow DAG

### What was built
- `airflow/dags/crime_batch_dag.py` — Airflow DAG orchestrating: download_crime → spark_crime_batch → dbt_build
- `dbt/Dockerfile` — Separate dbt image (dbt-core 1.11 + dbt-postgres 1.10) to avoid protobuf conflict with Airflow 3.0
- `airflow/dbt_profiles/profiles.yml` — DBT profiles for Airflow container (host: postgres, env_var credentials)
- `docker-compose.yml` updated: added ingestion/dbt/dbt_profiles mounts, Postgres env vars, execution API URL, shared secrets, dbt-build service
- `airflow/Dockerfile` updated: added docker group (GID 1001) for docker.sock access, ingestion deps (pandas, pyarrow, requests, python-dotenv)
- `airflow/requirements.txt` updated: ingestion deps added, dbt removed (separate image)
- `.env` / `.env.example` updated: added `AIRFLOW__CORE__INTERNAL_API_SECRET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__WEBSERVER__SECRET_KEY`
- `ingestion/download_crime.py` — fixed docstring resource ID typo (ijzp-q4t2 → ijzp-q8t2), increased API timeout 60s → 120s

### Errors and fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `@manual` schedule rejected: "Exactly 5, 6 or 7 columns" | Airflow 3.0 doesn't accept `@manual` as a cron expression | Changed `schedule="@manual"` to `schedule=None` |
| 2 | `not found in serialized_dag table` + `Connection refused` | Scheduler couldn't reach the API server — `execution_api_server_url` defaulted to `localhost:8080` (wrong inside scheduler container) | Set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/` |
| 3 | `Invalid auth token: Signature verification failed` | Scheduler and webserver generated different `jwt_secret` and `webserver.secret_key` at startup | Set shared `AIRFLOW__API_AUTH__JWT_SECRET` and `AIRFLOW__WEBSERVER__SECRET_KEY` in .env |
| 4 | `protobuf 4.25.6` vs dbt-core 1.11 requires `>=6.0` | Airflow 3.0 pins protobuf 4.x; dbt-core 1.11 needs protobuf >=6.0 — incompatible in same Python env | Built separate dbt Docker image, run via `docker run --rm` from BashOperator |
| 5 | `Permission denied: docker.sock` | Airflow container runs as UID 50000 (airflow), not in docker group | `groupdel docker; groupadd -g 1001 docker; usermod -aG docker airflow` in Dockerfile (GID must match host) |
| 6 | `PermissionError: [Errno 13] Failed to open local file crime_2023.parquet` | Existing Parquet created by host user, Airflow container runs as UID 50000 | `chmod 666` on Parquet file + `chmod 777` on data/raw/crime/ directory |
| 7 | `Env var required but not provided: 'POSTGRES_USER'` | `docker run` for dbt container doesn't inherit Airflow container's env vars | Added `-e POSTGRES_USER -e POSTGRES_PASSWORD -e POSTGRES_DB` to docker run command |
| 8 | `ReadTimeoutError` on Socrata API (60s timeout) | Downloading 50K rows per page takes >60s from inside container | Increased `requests.get()` timeout from 60s to 120s |
| 9 | `./dbt` and `./ingestion` not mounted into Airflow container | docker-compose.yml only mounted dags, spark/jobs, data, docker.sock | Added `./ingestion:/opt/airflow/ingestion`, `./dbt:/opt/airflow/dbt`, `./airflow/dbt_profiles:/opt/airflow/dbt_profiles` to x-airflow-common volumes |
| 10 | Airflow image missing ingestion deps (pandas, pyarrow, requests, python-dotenv) | `airflow/requirements.txt` only had provider packages | Added ingestion deps to requirements.txt |
| 11 | DBT profiles `host: localhost` won't work inside Airflow container | dbt/profiles.yml uses localhost (host-side), but container needs Docker service name | Created separate `airflow/dbt_profiles/profiles.yml` with `host: postgres` and `env_var()` credentials |
| 12 | `groupadd -f -g 1001 docker` silently failed | `-f` flag means "exit success if group exists" — docker group already existed at GID 102, so GID wasn't changed | Changed to `groupdel docker 2>/dev/null; groupadd -g 1001 docker` — delete first, then recreate with correct GID |
| 13 | Socrata resource ID typo in docstring | `download_crime.py` docstring said `ijzp-q4t2` (line 47 already had correct `ijzp-q8t2`) | Fixed docstring lines 9-10 |

### Lessons
- **Airflow 3.0 `@manual` is gone:** Use `schedule=None` for manual-trigger DAGs. `@manual` was never a valid cron expression.
- **Airflow 3.0 execution API:** The scheduler needs `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` pointing to the webserver's execution API. Inside Docker, this must use the service name (`http://airflow-webserver:8080/execution/`), not `localhost`.
- **Shared secrets are mandatory:** `AIRFLOW__API_AUTH__JWT_SECRET` and `AIRFLOW__WEBSERVER__SECRET_KEY` must be set as env vars in both webserver and scheduler. Without them, each generates its own random secret and JWT signature verification fails.
- **dbt + Airflow protobuf conflict:** dbt-core 1.11 (protobuf >=6.0) and Airflow 3.0 (protobuf 4.x) cannot coexist in the same Python environment. Run dbt in a separate container via `docker run --rm`.
- **docker.sock GID must match host:** The `docker.io` package creates a `docker` group at GID 102. On WSL2, docker.sock has GID 1001. Must `groupdel docker` then `groupadd -g 1001 docker` to align.
- **`--volumes-from` doesn't pass env vars:** When using `docker run --volumes-from`, the new container inherits volumes but NOT environment variables. Pass them explicitly with `-e VAR_NAME`.
- **`max_active_runs=1`:** Prevents overlapping DAG runs — critical when tasks are resource-heavy (Spark, Socrata downloads).
- **Missing mounts are silent killers:** BashOperator tasks fail with "file not found" when bind mounts are missing. Always verify mounts with `docker exec ... ls /path` before triggering the DAG.
- **`groupadd -f` is a silent no-op:** The `-f` flag makes `groupadd` succeed even if the group exists — but it does NOT change the existing group's GID. Must `groupdel` first, then `groupadd -g <correct GID>`.
- **Separate profiles for container vs host:** dbt/profiles.yml (host: localhost) works on the host but not inside Docker. Create a separate profiles.yml with `host: postgres` (Docker service name) and `env_var()` for credentials.

- **Never hardcode GIDs in Dockerfiles:** Hardcoding `groupadd -g 1001` breaks on any machine with a different docker.sock GID. Use `ARG DOCKER_GID=999` + `groupadd -g ${DOCKER_GID}` and override via `.env` + docker-compose `build.args`. Default 999 covers most native Linux; WSL2 typically uses 1001.

- **DBT views block Spark overwrite:** When Spark uses `mode("overwrite")` on a table that DBT views depend on, Postgres blocks the drop with `cannot drop table because other objects depend on it`. Drop the dependent schemas (staging, mart) with CASCADE before Spark runs — DBT rebuilds them idempotently.
- **Airflow 3.0 requires separate dag-processor:** Unlike Airflow 2.x where the scheduler parsed DAGs inline, Airflow 3.0 separates DAG processing into its own component. Without `airflow dag-processor` running, DAGs are never serialized and the scheduler can't find them. This is a breaking change from 2.x — docker-compose setups need the new service.

### Operational Mistakes (Phase 1.6)

These are not technical errors — they are process mistakes made by the AI assistant during Phase 1.6 verification. Documented to prevent repeating.

| # | Mistake | What Happened | What I Should Have Done |
|---|---|---|---|
| 1 | **Deleted working serialized_dag entry** | The DAG wasn't updating after editing the file. Instead of waiting for the scheduler's parse cycle or adding the dag-processor service, I deleted the serialized_dag row from Airflow's metadata DB. This broke the DAG entirely — the scheduler couldn't find it and logged `DAG 'crime_batch' not found in serialized_dag table` every second. | Diagnose first: check if a dag-processor is running, wait for the parse cycle (`min_file_process_interval=30s`), or restart the scheduler. The entry was working — it just had the old 3-task version. The right fix was adding the dag-processor service, not deleting the entry. |
| 2 | **4+ unnecessary `docker compose down/up` cycles** | After deleting the entry, I kept restarting all services hoping the scheduler would re-parse. Each cycle took ~60s and accomplished nothing — the dag-processor wasn't running, so no serialization happened regardless of restarts. | After the first restart failed to re-create the entry, stop and investigate WHY. Read the scheduler logs, check what component is responsible for DAG serialization, look for missing services. One restart to test a hypothesis is fine; repeated restarts without new information is thrashing. |
| 3 | **Manually mutated Airflow's internal metadata tables** | Tried to write a new serialized_dag entry via Python scripts (`SerializedDagModel.write_dag()`). Failed multiple times — wrong API signature, wrong arguments, and the resulting entry had 0 tasks because I didn't understand the serialization format. | Airflow's internal tables (serialized_dag, dag_run, task_instance, etc.) are managed by Airflow internals. Never manually insert/update/delete rows. Use the CLI (`airflow dags ...`) or let the dag-processor do its job. |

**Lessons:**
- **Diagnose first, touch second.** When something isn't updating, spend time understanding WHY before changing anything. Read logs, check config, identify the responsible component.
- **Never delete working state to "force" a refresh.** The serialized_dag entry was working — it just had stale content. Deleting it created a bigger problem than the one I was trying to solve.
- **Never manually mutate managed internal tables.** Airflow (and other systems) have internal metadata tables with specific serialization formats. Manual inserts will be malformed.
- **Repeated restarts without new information is thrashing.** If a restart didn't fix it, the next one won't either. Stop and gather new information (logs, config, docs) before acting again.


---

## 2026-07-15 — Phase 2.1: Divvy GBFS data source exploration

### Changes
- Explored live Divvy GBFS feeds (discovery, station_status, station_information, system_regions, system_information)
- Documented full GBFS schema in `docs/wiki/data-sources.md`

### Key Findings (design-changing)

| # | Finding | Impact on Plan | Fix |
|---|---|---|---|
| 1 | `station_id` is mixed format: 667 UUIDs + 1,349 numeric strings | Plan's DBT model had `station_id::bigint` — will fail on UUID IDs | Keep `station_id` as string throughout the pipeline |
| 2 | `is_renting`/`is_returning`/`is_installed` are integers 0/1, not booleans | Plan assumed booleans; Spark/DBT needs explicit cast | `CAST(col AS BOOLEAN)` in Spark (0→false, 1→true) |
| 3 | `num_scooters_available`/`num_scooters_unavailable` are optional (not in all stations) | Spark schema with strict mode will fail on missing fields | Use nullable schema fields, don't fail on absence |
| 4 | One station had `last_reported: 86400` (Jan 2, 1970 — dead station) | Stale data pollutes fact table | Filter `last_reported` to recent threshold |

### Lessons
- **Always inspect the live data before coding** — the plan's DBT model assumed `station_id::bigint` and boolean fields. Live API inspection revealed both assumptions wrong. Five minutes of API exploration saved hours of debugging downstream.
- **GBFS 1.1 uses integers for booleans** — `is_renting`, `is_returning`, `is_installed` are 0/1 integers, not JSON booleans. This is a GBFS spec quirk — the spec says "boolean" but the implementation uses integers. Always check actual API responses, not just spec docs.
- **Optional fields are common in GBFS** — not all stations report all fields. Spark's schema inference or strict schema will fail. Use nullable fields and tolerate absence.

---

## 2026-07-15 — Phase 2.2: Kafka + Zookeeper Docker services

### Changes
- Added `zookeeper` and `kafka` services to `docker-compose.yml` (Confluent Platform 7.6.0)
- Added `KAFKA_BOOTSTRAP_SERVERS: kafka:9092` env var to spark-master and spark-worker
- Added 3 named volumes: `kafka_data`, `zookeeper_data`, `zookeeper_log`
- Updated `docs/wiki/kafka.md` with full setup details, commands, and concepts

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Kafka image | `confluentinc/cp-kafka:7.6.0` | Bitnami images no longer free (commercial subscription since 2026). Confluent Platform is free, stable, production-hardened. Pinned version (not `latest`) for reproducibility. |
| Zookeeper vs KRaft | Zookeeper | KRaft is newer (no ZK dependency), but learning Zookeeper first is more educational — most existing Kafka deployments still use it. |
| Listeners | Two: internal (kafka:9092) + host (localhost:29092) | Internal for Spark/producer inside Docker network. Host listener for `kafka-console-consumer` testing from terminal. Without the host listener, you can't test from outside Docker. |
| Partitions | 3 for `divvy_station_status` | Allows parallelism — Spark can read from 3 partitions concurrently. `station_id` as message key ensures same station goes to same partition (ordered processing per station). |
| Auto-create topics | Enabled | Dev convenience — producer creates topic on first message. In production, you'd disable this and create topics explicitly to control partition count and replication. |
| Single-broker overrides | Replication factor 1 for all internal topics | Defaults assume 3 brokers. Without these overrides, Kafka can't create `__consumer_offsets` and consumers can't commit offsets. |

### Lessons
- **Single-broker Kafka needs replication overrides** — `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`, and `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` all default to 3 (for production clusters). With a single broker, they must be set to 1 or Kafka silently fails to create internal topics.
- **Two listeners for dev Kafka** — internal (`kafka:9092`) for Docker-network services, external (`localhost:29092`) for host-side testing. The `KAFKA_ADVERTISED_LISTENERS` config tells clients where to connect; without the host listener, you can't run `kafka-console-consumer` from your terminal.
- **Healthcheck with `kafka-broker-api-versions`** — more reliable than TCP port check. Queries Kafka's API endpoint and confirms the broker is actually serving, not just listening on a port. Needs `start_period: 20s` — Kafka takes ~30-40s to fully start.

---

## 2026-07-15 — Phase 2.3: Kafka producer

### Changes
- Created `kafka/producers/divvy_producer.py` — polls Divvy GBFS every 60s, publishes to Kafka
- Added `kafka-python` 3.0.8 to host venv and Airflow requirements
- Added `./kafka:/opt/airflow/kafka` volume mount to Airflow in docker-compose.yml

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `ImportError: cannot import name 'NoBrokersAvailable'` | `NoBrokersAvailable` was removed in kafka-python 3.0.x | Catch `KafkaError` (base class) instead |
| 2 | Auto-created topic had 1 partition (not 3) | `KAFKA_NUM_PARTITIONS` env var not applied by Confluent image — `server.properties` still showed `num.partitions=1` | Explicitly create topic with `kafka-topics --create --partitions 3`. Auto-create uses broker defaults; explicit creation is the correct approach for custom partition counts |

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Message key | `station_id` (string) | Same station → same partition → chronological order per station. Critical for time-series analysis. |
| Delivery guarantee | `acks="all"` | Safest — waits for all in-sync replicas. For single broker, equivalent to acks=1. |
| Shutdown | SIGINT/SIGTERM → flag → flush → close | Graceful shutdown prevents message loss. Pending messages are flushed before exit. |
| Poll cadence | `sleep = interval - elapsed` | Prevents drift — polls at consistent intervals regardless of fetch time. |
| Topic creation | Explicit (not auto-create) | Auto-create defaults to 1 partition. Explicit creation controls partition count. Auto-create still enabled as fallback. |

### Lessons
- **Auto-create uses broker defaults, not your desired config** — `KAFKA_NUM_PARTITIONS` env var didn't work with the Confluent image (`server.properties` showed `num.partitions=1`). For custom partition counts, create topics explicitly with `kafka-topics --create --partitions N`. Auto-create is a convenience, not a configuration mechanism.
- **kafka-python 3.0.x removed `NoBrokersAvailable`** — the exception hierarchy changed. Use `KafkaError` (base class) for catch-all error handling. Always check the installed version's API, not just documentation from older versions.
- **Key-based partitioning distributes evenly** — with 3 partitions and station_id as key, messages split 720/661/635 (not exactly equal, but close). The hash of station_id determines the partition — same station always goes to the same partition.

---

## 2026-07-15 — Phase 2.4: Spark Structured Streaming

### Changes
- Added 4 Kafka connector JARs to `spark/Dockerfile` (spark-sql-kafka, spark-token-provider, kafka-clients, commons-pool2)
- Created `spark/jobs/divvy_stream.py` — Structured Streaming consumer: Kafka → parse JSON → cast types → filter stale → foreachBatch → Postgres
- Added `spark_checkpoints` named volume to `docker-compose.yml` for checkpoint persistence
- Added checkpoint directory creation + ownership to `spark/Dockerfile`
- Created `raw.station_status` table in Postgres (18 columns: station data + Kafka metadata + ingest timestamp)

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `mkdir of file:/opt/spark/checkpoints/divvy_stream failed` | Named volume mounted as root:root, but Spark runs as user `spark` (UID 185) | `chown -R spark:spark /opt/spark/checkpoints` + added `RUN mkdir -p /opt/spark/checkpoints && chown spark:spark /opt/spark/checkpoints` to Dockerfile so future volume creations get correct ownership |
| 2 | `spark.sql.adaptive.enabled is not supported in streaming DataFrames` | AQE doesn't apply to streaming queries — only batch | Warning only, not an error. Spark automatically disables AQE for streaming. No action needed. |

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Kafka connector JARs | Baked into image (4 JARs) | Same approach as JDBC driver — reliable, offline, fast startup. Alternative: `--packages` at runtime (needs Maven, slow, fragile). |
| foreachBatch for JDBC | Standard pattern | JDBC has no native streaming sink. foreachBatch bridges: each micro-batch is a static DataFrame → standard JDBC writer works. |
| Checkpoint location | Named volume `spark_checkpoints` | Persists Kafka offsets across container restarts. Without it, restart re-reads all messages from `earliest`, causing duplicates. |
| Stale station filter | `last_reported > now() - 1 hour` | One station had `last_reported: 86400` (Jan 2 1970). Filtering at 1 hour drops dead stations. Result: 2016 → 1128 rows per poll (888 stale stations filtered). |
| is_* fields | `CAST(int AS BOOLEAN)` in Spark | GBFS returns 0/1 integers, not booleans. Cast in Spark so Postgres receives proper boolean. |
| station_id | StringType throughout | Mixed format (667 UUIDs + 1349 numeric). Casting to bigint would fail on UUIDs. |
| Optional scooter fields | Nullable in schema | Not all stations have scooters. `from_json` returns null for missing fields. 1099/1128 non-null in observed data. |
| Trigger interval | 60 seconds | Matches producer poll interval. Each micro-batch processes messages that arrived since the last batch. |
| Kafka metadata columns | partition, offset, timestamp | Traceability — can trace any row back to its Kafka position. Useful for debugging duplicates or gaps. |

### Lessons
- **apache/spark doesn't include Kafka connector** — the official image ships only core Spark JARs. Structured Streaming + Kafka needs 4 additional JARs (spark-sql-kafka, spark-token-provider, kafka-clients, commons-pool2). Baking them into the Dockerfile is the same pattern we used for the PostgreSQL JDBC driver.
- **Named volumes inherit ownership from the image directory** — when a named volume is first created, Docker copies permissions from the container's directory. If the directory is root-owned but the process runs as non-root, the volume will be root-owned too. Fix: create the directory with correct ownership in the Dockerfile BEFORE the volume mounts.
- **foreachBatch is the bridge for streaming-to-JDBC** — JDBC has no native Structured Streaming sink. foreachBatch gives each micro-batch as a static DataFrame, which can use the standard batch JDBC writer. This is the official Spark-recommended pattern.
- **Stale data filtering matters** — 888 of 2016 stations (44%) had stale `last_reported` timestamps. Without filtering, the warehouse would be polluted with dead-station data. Filter early in the pipeline (Spark), not late (DBT).
- **AQE doesn't apply to streaming** — `spark.sql.adaptive.enabled` is silently ignored for streaming queries. Don't rely on AQE for streaming partition coalescing.

---

## 2026-07-16 — Phase 2.5: DBT Stream Models

### Changes
- Created `dbt/models/staging/stg_station_status.sql` — staging view on `raw.station_status`: renames `last_reported`→`reported_at`, `ingest_timestamp`→`ingested_at`, deduplicates on Kafka coordinates (partition + offset)
- Created `dbt/models/marts/fact_station_reads.sql` — mart table: one row per station poll, with `date_key` FK to `dim_date`, derived `total_vehicles_available` column, filters null station_id/reported_at
- Modified `dbt/models/marts/dim_date.sql` — now spans both crime (2023) and station read (2026) dates using UNION ALL of min/max from both sources; added `date_bounds` CTE to aggregate across sources
- Updated `dbt/models/staging/schema.yml` — added `station_status` source, added `stg_station_status` model with tests (not_null, expect_between)
- Updated `dbt/models/marts/schema.yml` — updated `dim_date` description + year bounds (2023–2026), added `fact_station_reads` model with tests (not_null, expect_between, relationships to dim_date)

### Errors & Fixes

No errors encountered. All 59 DBT tests passed on first run.

### Key Decisions

| Decision | Choice | Why |
|---|---|---|
| Dedup key | Kafka partition + offset | Uniquely identifies each Kafka message. Same pattern as crime's `DISTINCT ON (id)` but adapted for streaming data where Kafka coordinates are the natural unique key. |
| Column renames | `last_reported`→`reported_at`, `ingest_timestamp`→`ingested_at` | Clearer naming: `reported_at` = when the station reported; `ingested_at` = when the pipeline received it. Matches `occurred_at`/`updated_at` pattern from crime staging. |
| Fact table grain | One row per station poll (Kafka message) | Most granular level — supports any aggregation (per station, per time window, per status). No pre-aggregation to avoid losing detail. |
| Derived column | `total_vehicles_available` = bikes + ebikes + scooters | Convenience for analytics. `COALESCE` on scooters since it's nullable. |
| dim_date expansion | UNION ALL of min/max from both sources | Single date dimension serves all fact tables. Without this, `fact_station_reads.date_key` FK test would fail (2026 dates missing from dim_date). |
| No unique test on station_id | Not unique — multiple polls per station | Unlike `fact_crime_events.crime_id` (unique), station_id repeats across polls. The grain is station + reported_at, not station alone. |

### Lessons
- **dim_date must span all fact tables** — when adding a second fact table with different date ranges, the date dimension must cover both. Otherwise the FK relationship test fails. Use UNION ALL of min/max from all sources.
- **Streaming fact tables have different grain than batch** — crime facts are one row per event (unique ID). Station reads are one row per poll (repeating station_id). Don't blindly copy the unique test pattern from batch fact tables.
- **Deduplication keys differ by source** — crime deduplicates on `id` (business key). Streaming data deduplicates on Kafka coordinates (partition + offset) since those are the system-of-record unique identifiers.

## 2026-07-16 — Phase 2.6: Airflow Stream DAG

### Errors and Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `ModuleNotFoundError: No module named 'kafka'` in Airflow container | `kafka-python` was in `airflow/requirements.txt` (added in Phase 2.3) but the Airflow image was never rebuilt | Rebuilt Airflow image with `--no-cache` |
| 2 | `uv pip install --system` fails to install kafka-python — permission denied creating `/usr/local/lib/python3.11/site-packages/kafka` | uv has a bug/quirk creating certain package directories in the apache/airflow image, even as root | Switched Dockerfile from `uv pip install --system` to `pip install` as the airflow user |
| 3 | `pip install` as root fails: "Please use 'airflow' user to run pip!" | apache/airflow image has a built-in guard that refuses pip as root | Run pip as `USER airflow` — the image uses a venv at `/home/airflow/.local` which is the install target |
| 4 | `kafka-python` installed but `from kafka import KafkaProducer` fails — `kafka.__path__` points to `/opt/airflow/kafka` | `./kafka:/opt/airflow/kafka` volume mount shadows the `kafka` Python package — Python treats the mounted directory as a namespace package | Renamed mount to `./kafka:/opt/airflow/kafka_scripts` in docker-compose.yml + updated DAG's `PRODUCER_SCRIPT` path |
| 5 | Spark streaming fails: `mkdir of /opt/spark/checkpoints/divvy_stream failed` | Named volume `spark_checkpoints` mounts as root:root; Spark runs as spark user and can't create subdirectories | Created `spark/entrypoint.sh` that chowns `/opt/spark/checkpoints` to spark:spark before dropping to spark via gosu. Added `ENTRYPOINT` to Spark Dockerfile. |
| 6 | `start_producer` task fails: `head: cannot open '/tmp/divvy_producer.log'` | Background `nohup` process dies immediately in Airflow's BashOperator; log file never created; `head` returns exit code 1 | Switched producer to `--once` mode (foreground, single poll, exits cleanly). Added `\|\| true` to `head` commands. |
| 7 | `stop_producer` task fails with exit code 1 | `kill` fails (process already dead) and `&&` short-circuits `rm` and `echo` | Changed `&&` to `;` after `kill` so cleanup runs regardless |
| 8 | `wait_for_data` times out — 0 new rows after 5 minutes | Producer died after first poll; Spark checkpoint consumed all Kafka messages in a previous run; no new messages to process | Fixed by `--once` producer mode + wiping checkpoint/table/topic for clean runs |
| 9 | DAG stuck in `queued` state, never picked up by scheduler | Previous failed DAG runs left orphaned task instances blocking the scheduler | `docker compose down` + fresh start clears all Airflow metadata |

### Lessons
- **Volume mount paths can shadow Python packages** — mounting `./kafka` to `/opt/airflow/kafka` made Python find the empty directory instead of the installed `kafka-python` package. Always check mount paths don't collide with package names.
- **apache/airflow image has a root pip guard** — it refuses `pip install` as root. The image uses a venv at `/home/airflow/.local`; run pip as the airflow user.
- **`uv pip install --system` can silently fail on certain packages** — uv couldn't create the `kafka` directory in site-packages despite running as root. When uv fails, fall back to pip.
- **Airflow BashOperator kills background processes** — `nohup` + `disown` don't reliably survive when the task's shell exits. For long-running processes, use `--once` mode or manage outside Airflow.
- **Named volumes mount as root** — Docker named volumes get root ownership on first mount, regardless of Dockerfile `chown`. Use an entrypoint script to fix permissions on every start.
- **`kill` with `&&` short-circuits cleanup** — if the process is already dead, `kill` returns non-zero and `&&` skips cleanup. Use `;` to ensure cleanup always runs.
---

## 2026-07-18 — Phase 3.1: Grafana

### Changes
- Added `grafana` service to `docker-compose.yml` (image `grafana/grafana:12.4.0`, port 3000, `grafana_data` named volume, healthcheck, anonymous Viewer access)
- Added `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` to `.env.example` + `.env`
- Created `grafana/provisioning/datasources/postgres.yml` — provisions two Postgres datasources: `chicago-analytics` (warehouse) + `airflow-metadata` (Airflow DB)
- Created `grafana/provisioning/dashboards/dashboards.yml` — dashboard provider scanning `./grafana/dashboards/` every 30s
- Created `grafana/dashboards/pipeline_health.json` — 10-panel pipeline health dashboard
- Created `grafana/dashboards/crime_divvy_analysis.json` — 6-panel analysis dashboard
- Created `docs/wiki/grafana.md` — comprehensive Grafana reference (concepts, provisioning, env var gotchas, jsonData.database deep dive, DAG run order, useful commands, 10 common mistakes) with 8 mermaid diagrams
- Created `docs/phase/phase-3.1-grafana.md` — phase completion doc
- Updated `README.md` — added Grafana to services table, URLs, project structure, Phase 3.1 progress section, corrected DAG run order (stream first, then crime batch)
- Updated `chat-history/current-state.md` — Phase 3.1 complete, 4 errors documented, DAG run order note added

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `Failed to provision data sources: yaml: unmarshal errors: line 25: cannot unmarshal !!map into string` | Used Go template syntax `{{.POSTGRES_USER}}` in datasource YAML. Grafana provisioning uses shell-style `$VAR`, not Go templates. | Changed to `user: $POSTGRES_USER` and `password: $POSTGRES_PASSWORD`. |
| 2 | `FATAL: no PostgreSQL user name specified in startup packet (SQLSTATE 28000)` on Airflow datasource | `AIRFLOW_DB_USER`/`AIRFLOW_DB_PASSWORD` env vars not in Grafana container. `docker compose restart` doesn't re-read env vars. | Added vars to docker-compose grafana env, then `docker compose up -d grafana` (recreates container). |
| 3 | `relation "airflow_metadata.dag_run" does not exist` | Tried to cross-database query from `chicago_analytics` datasource. Postgres can't query across databases without `postgres_fdw`. | Added second datasource `airflow-metadata` pointing at `airflow_metadata` DB. Updated Airflow panels to use it; dropped `airflow_metadata.` schema prefix from SQL. |
| 4 | Browser console: `You do not currently have a default database configured for this data source. Postgres requires a default database` — panels show "No data" despite API queries working | Grafana 12.4's Postgres plugin reads the database name from `jsonData.database`, NOT the top-level `database:` field. The top-level field works for the internal API (`/api/ds/query` with `datasourceId`) but the browser plugin uses a different code path that requires `jsonData.database`. | Added `database: chicago_analytics` (and `database: airflow_metadata`) inside the `jsonData:` block of each datasource in `postgres.yml`. Recreated Grafana container with `docker compose down grafana && docker compose up -d grafana`. |

### Lessons Summary
- **Grafana env var syntax is `$VAR`, not `{{.VAR}}`** — the cryptic "cannot unmarshal !!map into string" error is the signature of this mistake. Grafana's provisioning parser uses shell-style interpolation, not Go templates.
- **`docker compose restart` ≠ `docker compose up -d` for env changes** — restart reuses the existing container (with old env). `up -d` recreates the container when config changes.
- **Postgres databases are isolated** — unlike schemas (which share a database and can be cross-queried), databases are fully separate. Cross-DB queries need `postgres_fdw` or a second datasource. Our project has two databases (`chicago_analytics` + `airflow_metadata`), so Grafana needs two datasources.
- **`grafana/grafana-oss` is deprecated** since 12.4.0 — use `grafana/grafana` (includes Enterprise features, free to use).
- **Postgres datasource needs `jsonData.database`, not just top-level `database:`** — Grafana 12.4's Postgres plugin reads the DB name from `jsonData.database`. The top-level `database:` field is for older Grafana versions and is used by the internal API, but the browser plugin's query path requires `jsonData.database`. Without it, API queries succeed but browser panels show "No data" with a console error. Set both for compatibility.
- **DAG run order: stream first, then crime batch** — `crime_batch`'s `dbt_build` runs `dbt build` which builds ALL models including `stg_station_status` (depends on `raw.station_status`, created by `divvy_stream`). If `divvy_stream` hasn't run, `crime_batch`'s `dbt_build` fails on try 1 (succeeds on retry — a race condition). Running `divvy_stream` first eliminates the race. This is a pre-existing design issue to address in Phase 3.3 (separate batch/stream dbt models or add a sensor).
- **Verify in the browser, not just curl** — the internal API (`/api/ds/query` with `datasourceId`) uses the top-level `database:` field and gives false positives. The browser plugin uses `jsonData.database`. Always verify dashboards render in the browser after provisioning changes.

---

## 2026-07-20 — Phase 3.2: DBT Tests

### Changes
- Created `dbt/tests/assert_crime_in_chicago_bounds.sql` — singular test: flags crime events with populated lat/long outside Chicago's bounding box (lat 41.64–42.03, lon -87.95–-87.52). Complements the per-column range tests on `fact_crime_events.latitude/longitude` with a single readable combined check.
- Created `airflow/scripts/record_dbt_results.py` — parses `dbt/target/run_results.json` after `dbt build` and upserts one row per test into `observability.dbt_test_results` (new schema). Idempotent: keyed on `(invocation_id, test_name)`, DELETE-then-INSERT per invocation. Runs in the Airflow container (psycopg2 available via the postgres provider).
- Added `record_dbt_results` BashOperator task to both `crime_batch_dag.py` (after `dbt_build`) and `divvy_stream_dag.py` (between `dbt_build` and `stop_stream`). Runs `python /opt/airflow/scripts/record_dbt_results.py`.
- Mounted `./airflow/scripts:/opt/airflow/scripts` in the `x-airflow-common` anchor in `docker-compose.yml` so the recorder is available in all Airflow containers.
- Rewired the Grafana "DBT tests" panel on `pipeline_health.json` (id 8) from a static `SELECT 59 AS dbt_tests_passing` to a real query against `observability.dbt_test_results` returning `passing`/`failing`/`warnings` counts for the latest invocation. Added field overrides so Passing renders green, Failing renders red (threshold ≥1), Warnings neutral. Retitled "DBT test outcomes (latest run)".
- Stream `not_null` tests on `stg_station_status` and `fact_station_reads` (station_id, reported_at, is_renting, is_returning, num_bikes/docks_available) were already present from Phase 2.5 — no new tests needed there.

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Recorder captured 0 tests despite `dbt build` reporting `TOTAL=60` | Filtered results on `r.get("resource_type") == "test"`, but dbt 1.11's `run_results.json` does NOT populate `resource_type` (it is `None` for every entry). The `name` field is also `None`. | Changed filter to `r.get("unique_id", "").startswith("test.")`. Extracted the human-readable name from `unique_id` by stripping the `test.chicago_crime.` prefix and trailing `.<hash>` suffix. |
| 2 | Grafana dashboard JSON malformed after incremental edits to the DBT panel | Multiple `edit` ops dropped the `"fieldConfig": {` wrapper and the `"matcher": {` opener of the first override object, leaving `defaults`/`overrides` at the wrong nesting level. | Re-inserted the missing `"fieldConfig": {` wrapper and `"matcher": {` opener; validated with `python3 -c "import json; json.load(open(...))"`. Lesson: for deeply-nested JSON panel edits, rewrite the whole panel object in one op rather than patching field-by-field. |

### Lessons Summary
- **dbt 1.11 `run_results.json` has no `resource_type` field** — every entry has `resource_type: null`. Identify tests by `unique_id` prefix (`test.`), models by `model.`, seeds by `seed.`. The `name` field is also null; the readable name lives inside `unique_id` as `test.chicago_crime.<name>.<hash>`.
- **dbt's `TOTAL=N` counts all resources, not just tests** — `TOTAL=60` = 1 seed + 7 models + 52 tests. The recorder correctly captured 52 tests; the "missing 8" were non-test resources. Don't confuse dbt's resource total with the test count.
- **Edit JSON panel objects wholesale, not field-by-field** — the `edit` tool's line-range semantics make it easy to drop a closing brace or object opener when patching nested Grafana JSON. For panel rewrites, replace the entire panel object in one op and validate with `json.load` before moving on.
- **Observability metadata gets its own schema** — `observability.dbt_test_results` lives in a dedicated schema (not `mart` or `raw`), created idempotently by the recorder. Keeps pipeline metadata out of the analytics mart and the raw landing zone.

---

## 2026-07-20 — Phase 3.3: Airflow Robustness

### Changes
- Created `airflow/dags/callbacks.py` — shared `on_failure_callback` that logs structured failure context (dag_id, task_id, run_id, try_number, exception) to Airflow task logs. Wired into both DAGs via `default_args["on_failure_callback"]`.
- Added `SqlSensor` (`wait_for_stream_data`) to `crime_batch_dag.py` — gates `dbt_build` on `raw.station_status` existing. Fixes the race condition where `dim_date` (which spans both crime + station sources) causes `dbt build` to fail if `divvy_stream` hasn't run yet. The sensor makes the previously implicit dependency explicit. Uses `to_regclass('raw.station_status')` with `mode="reschedule"`, 60s poke interval, 1hr timeout.
- Updated `default_args` in both DAGs: `retries=3`, `retry_delay=timedelta(minutes=5)`, `on_failure_callback=on_failure_callback`.
- Added `execution_timeout=timedelta(minutes=30)` to `dbt_build` in both DAGs. (Originally tried `sla=` but Airflow 3.0 removed the SLA feature — see errors below.)
- Set `retries=0` on cleanup tasks (`stop_stream`, `stop_producer`) in `divvy_stream_dag.py` — retrying cleanup is pointless.
- Added `AIRFLOW_CONN_POSTGRES_DEFAULT` env var to `docker-compose.yml` `x-airflow-common` anchor — the SqlSensor needs a Postgres connection to query the warehouse. Format: `postgresql://user:pass@postgres:5432/db`.
- Added "Failed tasks (last 7 days)" panel (id 11) to `pipeline_health.json` — queries `task_instance` for failed/upstream_failed states. Originally planned as an SLA misses panel but Airflow 3.0 removed SLA tracking (see errors below).

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | SqlSensor `wait_for_stream_data` failed: `'str' object has no attribute 'fetchone'` | Used `success=lambda result: result.fetchone()[0] is not None` — assumed the callback receives a DB cursor. Airflow 3.0's `SqlSensor.poke` calls `hook.get_records(sql)` → list of rows, then passes `records[0]` (the first row tuple) to the success callable, not a cursor. | Changed to `success=lambda row: row[0] is not None`. The row is a 1-tuple like `('raw.station_status',)` or `(None,)`. |
| 2 | `sla=timedelta(minutes=30)` triggers deprecation warning: "The SLA feature is removed in Airflow 3.0, to be replaced with a new implementation in >=3.1" | Airflow 3.0 removed the entire SLA subsystem. The `sla=` parameter is accepted but is a no-op — no SLA misses are recorded in `dag_warning` or anywhere else. | Replaced `sla=` with `execution_timeout=timedelta(minutes=30)` in both DAGs. `execution_timeout` actually fails the task if it exceeds the limit. Changed the Grafana panel from "SLA misses" to "Failed tasks" (queries `task_instance` for failed states). |
| 3 | Stuck DAG run blocked new runs (`max_active_runs=1`) | The failed `wait_for_stream_data` task was `up_for_retry` (3 retries with 5min delay = 15min of retries). The DAG run stayed `running` while retrying, blocking the new triggered run which stayed `queued`. | Manually marked the stuck run as `failed` in the metadata DB. The new run then started immediately. In production, you'd wait for retries to exhaust or reduce retry count for sensor tasks. |

### Lessons Summary
- **Airflow 3.0 removed the SLA feature** — `sla=` parameter triggers a deprecation warning and is a no-op. No SLA misses are recorded in `dag_warning`. Use `execution_timeout=` instead — it actually fails the task if it exceeds the limit. SLA is planned to return in Airflow 3.1+.
- **Airflow 3.0 SqlSensor success callback receives a row, not a cursor** — `SqlSensor.poke` calls `hook.get_records(sql)` → list of rows, then passes `records[0]` to the success callable. The callable receives a single row (tuple), not a cursor object. Don't call `.fetchone()` on it.
- **Sensors + `max_active_runs=1` can block new runs** — a sensor in `up_for_retry` state keeps the DAG run in `running` state, which blocks new runs if `max_active_runs=1`. For sensor tasks, consider fewer retries or shorter retry delays to avoid long blocking periods.
- **Make implicit cross-DAG dependencies explicit with sensors** — `dim_date` spans both crime + station sources, creating an implicit dependency between `crime_batch` and `divvy_stream`. The SqlSensor makes this explicit: `crime_batch` waits for `raw.station_status` to exist before building marts. This is cleaner than splitting dbt models (batch-only vs stream-only) because `dim_date` legitimately needs both sources.
- **`AIRFLOW_CONN_<CONN_ID>` env var pattern** — Airflow auto-creates connections from env vars. `AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://user:pass@host:port/db` creates a connection with `conn_id="postgres_default"`. No need to use the Airflow UI or CLI.

---

## 2026-07-20 — Phase 3.4: Verification

### Changes
- **Verification phase — no new code.** Broke the pipeline in 3 ways and confirmed all observability mechanisms catch the failures. Pipeline restored to working state after each test.
- Created + deleted a throwaway DAG (`verify_failure_dag.py`) to test task failure handling without touching production DAGs.

### Verification Scenarios

| # | Scenario | What Was Broken | Observability That Caught It | Result |
|---|---|---|---|---|
| 1 | Stream freshness alert | Producer stopped (divvy_stream DAG completed, no new data) | Grafana "Stream freshness" panel (id 6) — red at 900s threshold | Freshness = 1195s (19.9min) > 900s → panel RED ✅ |
| 2 | DBT test failure | Injected bad crime row (lat=45.0, lon=-100.0 — South Dakota, outside Chicago bounds) into `raw.crime_events` | DBT bounds tests (latitude + longitude range checks in `staging/schema.yml`) + Grafana "DBT test outcomes" panel (id 8) | 2 tests failed (latitude + longitude bounds), recorder captured fail=2, Grafana panel showed passing=30 failing=2 → RED ✅ |
| 3 | Task failure + retries + callback | Throwaway DAG with `exit 1` task, retries=3, retry_delay=10s, on_failure_callback | Airflow retries (4 attempts: 1 initial + 3 retries) + on_failure_callback (structured log) + Grafana "Failed tasks" panel (id 11) | Task failed after 4 attempts, callback logged `dag=verify_failure_handling task=fail_on_purpose try=4`, Grafana panel showed failed_tasks=2 → RED ✅ |

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `dbt build` manual run failed: image `chicago-crime-dbt:latest` not found | Wrong image name. DAGs use `chicago-data-pipeline-dbt:latest`. | Used correct image name from `DBT_IMAGE` var in DAGs. |
| 2 | `dbt build` manual run failed: `--project-dir /opt/dbt` does not exist | Wrong path. DAGs use `/opt/airflow/dbt` + `/opt/airflow/dbt_profiles`. | Used correct paths from `DBT_DIR` + `DBT_PROFILES_DIR` vars in DAGs. |
| 3 | Throwaway DAG not found by `airflow dags trigger` | DAG bundle refresh interval is long (~30s+). New DAGs aren't immediately available. | Ran `airflow dags reserialize` to force bundle refresh, then triggered. |
| 4 | `airflow dags delete` failed with `EOFError: EOF when reading a line` | Delete command prompts for confirmation (`y/n`), but `docker compose exec -T` has no TTY. | Piped `echo "y"` into the command. |

### Lessons Summary
- **Panel thresholds are sufficient alerts for local dev** — Grafana's unified alerting system (contact points, notification policies, alert rules) is overkill for a learning project. The panel turning red at a threshold IS the alert. Full alerting would be a bonus feature, not a phase gate requirement.
- **DBT singular tests catch what column tests catch, but more readably** — the injected bad row (lat=45, lon=-100) failed BOTH the column-range tests in `staging/schema.yml` AND the singular bounds test `assert_crime_in_chicago_bounds.sql`. The column tests fired first (they run on `stg_crime_events`, the singular test runs on `fact_crime_events`). Both are valuable — column tests are granular, the singular test is a readable combined check.
- **Airflow 3.0 `try_number` starts at 1, not 0** — a task with `retries=3` has try_number values 1, 2, 3, 4 (1 initial + 3 retries). The final failed attempt has `try_number=4`, not 3.
- **`on_failure_callback` fires only after all retries are exhausted** — the callback logged `try=4` (the final attempt), not on each individual retry. This is the correct behavior — you want to alert once after retries are exhausted, not on every transient failure.
- **Throwaway DAGs are the right way to test failure handling** — creating a temporary DAG with `exit 1` tests retries + callbacks without risking production DAGs or needing to modify them. Delete the DAG file + run `airflow dags delete` to clean up metadata.
- **Manual `dbt build` is correct when you need to preserve test data** — triggering the crime_batch DAG would re-run `spark_crime_batch` and overwrite the injected bad row. Running `dbt build` manually (with the same image/paths from the DAG) preserves the bad row through the dbt build step.

## 2026-07-21 — Phase 4.1: Warehouse Choice + GCP Project Setup

### Changes
- Chose BigQuery as cloud warehouse (over Snowflake/Redshift). Free tier, serverless, DBT first-class.
- Created GCP project `chicago-divvy-pipeline` via gcloud CLI.
- Linked billing account, enabled BigQuery + Storage + Resource Manager APIs.
- Created service account `terraform-runner` with 4 scoped roles (NOT owner).
- Downloaded service account key to `~/chicago-divvy-pipeline-credentials.json` (chmod 600).
- Updated `.gitignore` to exclude `*-credentials.json` + variants.
- Created `docs/wiki/gcp.md` — comprehensive GCP reference (auth model, setup, WSL vs Windows, pitfalls).

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `gcloud iam service-accounts keys create ~/file.json` → `No such file or directory: '~/file.json'` | gcloud is a Python tool; it does NOT expand `~`. It treats `~/file.json` as a literal filename. | Used explicit path: `C:\Users\sagar\file.json` on Windows, then `cp` to WSL. |
| 2 | `gcloud iam service-accounts create terraform-runner \` → `unrecognized arguments: \` | PowerShell uses backtick (`` ` ``) for line continuation, not backslash (`\`). Bash uses `\`. | Put command on one line (no continuation), or use backtick in PowerShell. |
| 3 | `gcloud beta billing accounts list` → `You do not currently have this command group installed` | `beta` gcloud components not installed by default. | Ran `gcloud components install beta`. |

### Lessons Summary
- **GCP has two layers of identity** — personal Gmail (human, used once via browser for setup) vs service account (machine, used by Terraform via `credentials.json`). Confusing them is the #1 Terraform-on-GCP auth stumbling block. See `docs/wiki/gcp.md`.
- **Service account keys are passwords** — `chmod 600`, gitignore, never commit, never paste. A leaked key grants whatever roles the service account has. Grant scoped roles (bigquery.dataOwner, storage.admin), NOT `roles/owner`.
- **`~` is not expanded by gcloud** — it's a Python tool, not a shell. Always use explicit absolute paths for file output (`/home/sagar/...` on WSL, `C:\Users\sagar\...` on Windows).
- **PowerShell ≠ bash for line continuation** — PowerShell uses backtick `` ` ``, bash uses backslash `\`. When following bash-style multi-line commands in PowerShell, either use backtick or put the command on one line.
- **Browser auth requires Windows, not WSL** — `gcloud auth login` opens a browser; WSL has no browser. Run gcloud auth on Windows PowerShell, then move artifacts (credentials.json) to WSL via `/mnt/c/Users/sagar/`.
- **Billing account is required even for free tier** — BigQuery free tier (1 TB queries/mo + 10 GB storage/mo) won't activate without a billing account linked. You won't be charged if you stay in limits, but the card must be on file.
- **APIs are off by default** — `PERMISSION_DENIED: ... API has not been used in project ... or it is disabled` means you skipped `gcloud services enable`. Enable BigQuery + Storage + Resource Manager before Terraform.

## 2026-07-21 — Phase 4.2: Terraform (BigQuery + GCS provisioning)

### Changes
- Wrote Terraform config: `terraform/providers.tf` (Google provider v7.40.0, auths via SA key), `variables.tf` (4 inputs), `main.tf` (3 resources: 2 BigQuery datasets + 1 GCS bucket), `terraform.tfvars` (gitignored), `terraform.tfvars.example` (template).
- Ran `terraform init` (provider installed), `terraform plan` (3 to add, 0 change, 0 destroy), `terraform apply` (resources created).
- Verified: `bq ls` → `raw` + `mart` datasets; `gsutil ls` → `gs://chicago-divvy-pipeline-data-lake/`.
- Updated `.gitignore` with Terraform state + tfvars patterns (removed older duplicate block).
- Expanded `docs/wiki/gcp.md` with 2 new WSL pitfalls, Terraform section, gsutil deprecation note.

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | WSL `gcloud services list` → `AUTH_PERMISSION_DENIED` authenticated as `terraform-runner@dtc-de-course-497317...` (old course project) | WSL gcloud has separate config + auth state from Windows gcloud. Windows `gcloud auth login` + `config set project` didn't carry to WSL. WSL was still authed as the old course SA. | `gcloud auth activate-service-account terraform-runner@chicago-divvy-pipeline... --key-file=/home/sagar/chicago-divvy-pipeline-credentials.json` in WSL (non-interactive, key-based — same as CI/CD auth). Then `gcloud config set account` + `config set project`. |
| 2 | `gcloud auth activate-service-account --key-file=~/...` → `No such file or directory: '~/...'` | gcloud (Python) doesn't expand `~`. Treats it as literal path. (Same pitfall as Phase 4.1 Step 7.) | Used explicit path: `/home/sagar/chicago-divvy-pipeline-credentials.json`. |
| 3 | `gcloud services list --enabled` → `AUTH_PERMISSION_DENIED` even after SA authed | Expected — SA's scoped roles (bigquery.dataOwner, storage.admin, etc.) deliberately exclude `serviceusage.services.list` (admin role). Least privilege working as designed. | Not a bug. Verified APIs from personal Gmail in PowerShell (already done in 4.1). Used `bq ls` + `gsutil ls` (permissions ARE in SA roles) for resource verification instead. |

### Lessons Summary
- **WSL and Windows gcloud have separate state** — config (`~/.config/gcloud/`) and auth are per-environment. Setting project/account in PowerShell does NOT carry to WSL. After moving the key to WSL, you must also `gcloud auth activate-service-account --key-file=...` + `gcloud config set project/account` in WSL.
- **`gcloud auth activate-service-account` is the non-interactive auth path** — use it in WSL (no browser) and in CI/CD. It loads a service account key into gcloud's credential store. This is how automation auths; `gcloud auth login` (browser) is for humans only.
- **Least privilege means some admin commands fail by design** — `gcloud services list` failing for the SA is correct. The SA can create BigQuery datasets + GCS buckets (its job) but can't administer APIs (an admin's job). If `gcloud services list` worked, you'd over-granted. Verify with `bq ls` + `gsutil ls` instead — those permissions are in the SA's roles.
- **Terraform `delete_contents_on_destroy = true` is a learning-project tradeoff** — lets `terraform destroy` wipe data so you can re-run from scratch. NEVER set this in production — it would delete your warehouse on a typo. Same for `force_destroy = true` on buckets.
- **Pin Terraform provider versions with `~>`** — `~> 7.40` allows patch updates (7.40.1, 7.40.2) but blocks minor versions (7.41) that could change behavior. Stable, non-experimental versions per user preference.
- **`terraform.tfstate` is the source of truth for what Terraform manages** — gitignored (contains resource IDs + some metadata). Lose it and Terraform can't destroy cleanly. Local state is fine for one operator; migrate to GCS backend for team use (plan says later).

## 2026-07-21 — Phase 4.3: Architecture Change (Postgres → GCS/BigQuery)

### Changes
- **Spark**: Added GCS connector JAR to `spark/Dockerfile`. Rewrote `crime_batch.py` sink from Postgres JDBC to GCS Parquet (`gs://chicago-divvy-pipeline-data-lake/raw/crime/`). Added `GOOGLE_APPLICATION_CREDENTIALS` + `GCS_BUCKET` env vars + credentials volume mount to spark-master + spark-worker in `docker-compose.yml`.
- **Airflow**: Added Google Cloud SDK (gcloud + bq CLI) to `airflow/Dockerfile`. Added GCP env vars + credentials mount to `x-airflow-common` anchor. Added `google-cloud-bigquery` to `airflow/requirements.txt`. Rewrote `crime_batch_dag.py`: new `bq_load_crime` task (GCS Parquet → BigQuery `raw.crime_events`), removed `clear_dbt_schemas` + `wait_for_stream_data` sensor, updated `dbt_build` with `--exclude stg_station_status fact_station_reads` + GCP env passthrough.
- **DBT**: Switched `dbt/Dockerfile` from `dbt-postgres==1.10.2` to `dbt-bigquery==1.12.0`. Rewrote both `profiles.yml` files for BigQuery (service-account key auth). Fixed SQL for BigQuery dialect: `DISTINCT ON` → `QUALIFY ROW_NUMBER()`, `::type` → `SAFE_CAST`/`CAST`, `generate_series` → `GENERATE_DATE_ARRAY`, `TO_CHAR` → `FORMAT_TIMESTAMP`, `EXTRACT(dow FROM)` → `EXTRACT(DAYOFWEEK FROM)`. Dropped station_status UNION from `dim_date.sql`. Updated `try_cast` macro with BigQuery type mapping. Added `station_status` source back to `schema.yml` (for parsing only — excluded from build).
- **Env**: Added GCP section to `.env` + `.env.example` (`GCP_CREDENTIALS_PATH`, `GCP_PROJECT_ID`, `GCS_BUCKET`, `BIGQUERY_LOCATION`).
- **Verification**: Rebuilt all Docker images. Tested each task individually via `airflow tasks test`: spark_crime_batch (263,402 rows → GCS), bq_load_crime (263,403 rows → BigQuery), dbt_build (38/38 tests pass, 4 marts created), record_dbt_results (32 test results → Postgres observability). Verified BigQuery marts: dim_date (365), fact_crime_events (263,403), dim_community_area (77), dim_crime_type (323).

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `docker compose build` → `error getting credentials - fork/exec docker-credential-desktop.exe: exec format error` | WSL2 Docker config (`~/.docker/config.json`) had `"credsStore": "desktop.exe"` — points to Windows exe that can't run in WSL. | Set `"credsStore": ""` + `"auths": {}` in `~/.docker/config.json`. |
| 2 | `bq load` → exit code 127 (command not found) | Airflow services were running stale images — `--force-recreate` didn't pick up the new build. Compose generated separate image names per service. | `docker compose build --no-cache airflow-scheduler` then `docker compose up -d --force-recreate airflow-scheduler`. |
| 3 | `bq load` → "You do not currently have an active account selected" | The `bq` CLI (gcloud SDK) does NOT read `GOOGLE_APPLICATION_CREDENTIALS` env var like the Python client does. It uses gcloud's own credential store. | Added `gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS` before `bq load` in the DAG task. |
| 4 | `gcloud auth activate-service-account` → "Permission denied: /opt/airflow/gcp-credentials.json" | Credentials file was `chmod 600` owned by host UID 1000, but Airflow container runs as UID 50000. | `chmod 644 ~/chicago-divvy-pipeline-credentials.json` on host (still gitignored, on user's own machine). |
| 5 | `dbt build` → "Model stg_station_status depends on source raw.station_status which was not found" | `--exclude` prevents models from being BUILT but not from being PARSED. DBT still resolves `source()` refs during compilation. I had removed the source from `schema.yml`. | Added `station_status` source back to `schema.yml` (for parsing only). The `--exclude` flag still prevents it from being built against BigQuery. |
| 6 | `download_crime` task → `ReadTimeout: data.cityofchicago.org` | Socrata API network timeout (transient, pre-existing — not Phase 4.3 related). Data already existed from a prior run. | Not a Phase 4.3 bug. Tested remaining 4 tasks individually via `airflow tasks test`. |

### Lessons Summary
- **`bq` CLI ≠ Python client for auth** — The `google-cloud-bigquery` Python library reads `GOOGLE_APPLICATION_CREDENTIALS` automatically. The `bq` CLI (part of gcloud SDK) does NOT — it uses gcloud's credential store and needs `gcloud auth activate-service-account --key-file=...` first. This is a common gotcha when mixing CLI + library auth.
- **`--exclude` prevents building, not parsing** — DBT's `--exclude` flag skips model execution but NOT compilation. Excluded models still need their `source()` and `ref()` calls to resolve. Keep source definitions in `schema.yml` even for models that won't build — they're metadata, not runtime.
- **Docker Compose image naming** — without an explicit `image:` tag in a service, Compose generates a name per-service. `docker compose build airflow-init` builds a different image than `docker compose build airflow-scheduler`. Always build the specific service you're recreating, or add explicit `image:` tags.
- **Bind mount permissions across UIDs** — a file `chmod 600` owned by host UID 1000 is unreadable by a container running as UID 50000 (Airflow's default). For mounted secrets, use `chmod 644` or match the container UID. The file is still gitignored + on the user's machine.
- **BigQuery SQL dialect differences** — Postgres `DISTINCT ON` → BigQuery `QUALIFY ROW_NUMBER() OVER(...) = 1`. `generate_series` → `GENERATE_DATE_ARRAY + UNNEST`. `TO_CHAR` → `FORMAT_TIMESTAMP`. `::type` casts work in BigQuery but `SAFE_CAST`/`CAST` is more idiomatic. `DATE_TRUNC(date, MONTH)` arg order is the same in both.


## 2026-07-22 — Phase 4.4: Divvy Trip History + Correlation Analysis

### Changes
- **dlt ingestion:** Installed `dlt[bigquery]` 1.29.0 in Airflow. Created `ingestion/load_divvy_trips.py` — S3 ZIP → CSV extract → dlt load → BigQuery `raw.divvy_trips` (append mode). Ingested 34,751,413 rows across 75 months (2020-04 to 2026-06).
- **Crime source switch:** `stg_crime_events.sql` rewritten to read from `bigquery-public-data.chicago_crime.crime` (8.6M rows, 2001-present) instead of `raw.crime_events` (263K Socrata extract). Column mapping: `unique_key`→`crime_id`, `date`→`occurred_at`, `updated_on`→`updated_at`. Filtered to `year >= 2018` for Divvy overlap.
- **New DBT models:** `stg_divvy_trips` (staging view), `dim_stations` (station dimension with ST_GEOGPOINT), `fact_divvy_trips` (partitioned by `started_at`, clustered by `start_station_id`), `fact_station_day` (THE analytics mart — trip_count per station per day + crime_count_within_quarter_mile via ST_DISTANCE ≤ 402m, partitioned by `date_key`), `crime_ridership_correlation` (CORR() at overall/per_station/per_month scope).
- **Partitioning:** `fact_crime_events` partitioned by `date_key`, clustered by `community_area_id` + `primary_type`. `fact_divvy_trips` partitioned by `started_at`, clustered by `start_station_id`. `fact_station_day` partitioned by `date_key`, clustered by `station_id`.
- **Airflow DAGs:** Created `divvy_trip_history_dag.py` (3 tasks: load → dbt_build → record). Simplified `crime_batch_dag.py` to 2 tasks (dbt_build → record) — removed download/spark/bq_load since crime uses public dataset.
- **Grafana:** Added BigQuery datasource plugin (`grafana-bigquery-datasource`). Added scatter plot (panel 7: trip_count vs crime_count) + correlation gauge (panel 8: overall Pearson r).
- **dim_date:** Updated to UNION min/max dates from both crime + Divvy sources (2018–2026).

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Airflow containers running old image (ModuleNotFoundError: No module named 'dlt') | Compose `--force-recreate` reused cached image; separate image names per service | `docker compose down` + `docker compose build` + `docker compose up -d` |
| 2 | DBT stg_crime_events coordinate tests fail (4 rows in Missouri: lat ~36.6, lon ~-91.7) | Public dataset has data entry errors with out-of-bounds coordinates | Added WHERE clause filtering to Chicago bounds (lat 41.64–42.03, lon -87.95–-87.52); kept nulls (valid crimes with unknown location) |
| 3 | DBT stg_divvy_trips coordinate test fails (1 row in Montreal: lat 45.6, lon -73.8) | 1 row had Montreal coordinates — data entry error | Added WHERE clause filtering to Chicago area (lat 41.0–42.5, lon -88.5–-87.0); kept nulls (dockless ebikes) |
| 4 | DBT fact_crime_events error: "Unrecognized name: primary_type at [9:35]" | `cluster_by=["primary_type"]` but `primary_type` was not in the SELECT output (only `crime_type_key` was) | Added `c.primary_type` to the SELECT list |
| 5 | DBT stg_divvy_trips error: "Query without FROM clause cannot have WHERE clause at [56:1]" | Edit accidentally removed the `FROM {{ source('raw', 'divvy_trips') }}` line | Re-added the FROM clause |
| 6 | DBT crime_ridership_correlation error: "Unrecognized name: station_day_count at [79:5]" | `overall` CTE named the column `total_station_days` but the SELECT referenced `station_day_count` | Renamed to `station_day_count` in the `overall` CTE for consistency with `per_station` and `per_month` CTEs |
| 7 | DBT relationships tests fail (28M+ rows for date_key → dim_date, 106K for crime_type_key → dim_crime_type) | `dim_date` and `dim_crime_type` were stale from Phase 4.3 (built from 2023-only Socrata data). `--select +fact_station_day` only includes parent models via `ref()`, but `dim_date` is not a parent of `fact_station_day` | Ran full `dbt build --exclude stg_station_status fact_station_reads` to rebuild all models including `dim_date` and `dim_crime_type` |

### Lessons Summary
- **`cluster_by` columns must be in the SELECT output** — BigQuery can only cluster on columns that exist in the materialized table. If you cluster by a column you transform away (e.g. `primary_type` → `crime_type_key`), add the original column to the SELECT.
- **`--select +model` only includes parent models** — `dim_date` wasn't a parent of `fact_station_day` (no `ref()` call), so it wasn't rebuilt. When switching data sources, do a full build to refresh all dimensions.
- **Public datasets have data quality issues** — `bigquery-public-data.chicago_crime` has rows with Missouri coordinates. Always add coordinate bounds filters in staging, even for "trusted" public datasets.
- **dlt is a lightweight Airbyte alternative** — 1 Python file, no extra containers, native BigQuery support. Chosen over Airbyte (5-6 containers, 2-4GB RAM) for WSL2 resource constraints.
- **Edit tool can accidentally remove lines** — when using SWAP on a WHERE clause, the FROM clause above can be lost if the range is mis-specified. Always read after editing to verify structural integrity.
- **Correlation ≠ causation** — overall Pearson r = +0.20 (weak positive). Both crime and ridership are higher in busy areas. The confounding variable is urban activity level, not a causal relationship.


## 2026-07-22 — Phase 4.8: BigQuery ML (stretch goal)

### Changes
- **BQML linear regression model:** Trained `mart.crime_ridership_model` (linear_reg) via dbt post_hook on `crime_ridership_model_training_data`. Features: crime_count_within_quarter_mile (numeric), day_of_week (categorical), month (categorical), station_id (categorical fixed effect). Label: trip_count.
- **4 new DBT models:** `crime_ridership_model_training_data` (815K rows, 2020-2023 + post_hook), `crime_ridership_model_evaluation` (ML.EVALUATE on auto-split validation), `crime_ridership_model_weights` (ML.WEIGHTS — 5 rows), `crime_ridership_predictions` (ML.PREDICT on 648K 2024+ test rows).
- **Airflow DAGs updated:** `divvy_trip_history_dag.py` `--select` now includes BQML models. `crime_batch_dag.py` `--exclude` now excludes them.
- **Knowledge doc:** Created `docs/wiki/bigquery-ml.md` — BQML syntax, dbt integration via post_hook, gotchas (categorical weight NULL, high-cardinality unseen categories, no_split + ML.EVALUATE).

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `not_null` test failed on `crime_ridership_model_weights.weight` | BigQuery ML returns `weight = NULL` for categorical features — per-category weights are in `category_weights` JSON array, not `weight` column | Removed `not_null` test on `weight`; updated column description |
| 2 | Out-of-sample R² = -173,642 (catastrophically negative) | `data_split_method='no_split'` + `ML.EVALUATE` on 2024+ data. 50% of test rows are unseen stations (opened after 2023) — station fixed effect has no learned weight, predictions default to intercept (~16,762) vs actual ~10-50 | Switched to `data_split_method='auto_split'` (default) — `ML.EVALUATE` uses in-sample validation. 2024+ predictions remain as out-of-sample test; the generalization gap is the learning outcome |

### Lessons Summary
- **`ML.WEIGHTS.weight` is NULL for categoricals** — BigQuery ML one-hot encodes categorical features; per-category coefficients are in `category_weights` (JSON). Don't test `not_null` on `weight`.
- **High-cardinality categoricals break on unseen categories** — station_id (1,900+ values) as a fixed effect works in-sample (R²=0.43) but fails on new stations (R²=-199K). Predictions default to intercept for unseen categories. For production, use aggregate features (e.g. 30-day rolling average) instead of raw IDs.
- **`data_split_method='no_split'` + `ML.EVALUATE` (no data) = error** — no held-out validation set. Use `auto_split` (default) for in-sample evaluation.
- **BQML + dbt = post_hook pattern** — `CREATE MODEL` can't be a dbt materialization. Use a post_hook on the training-data model; downstream models `ref()` it to ensure the model is trained before evaluation/weights/predictions run.
- **Regression confirms correlation** — crime coefficient = +1.45 (positive) even after controlling for station/day/month. Confirms Phase 4.4 finding: crime doesn't reduce ridership; both are higher in busy areas.


## 2026-07-22 — Data Inventory Verification

### Changes
- Verified all BigQuery analytics tables by querying row counts + date ranges directly.
- Verified Postgres tables — only `observability.dbt_test_results` present; streaming tables (`raw.station_status`, `raw.crime_events`) missing.
- Corrected stale row counts in `current-state.md` (was showing 263K crime + 2.2K station_status from a previous session; those Postgres tables no longer exist).
- Added "Current Data Inventory (verified 2026-07-22)" section to `docs/wiki/data-sources.md` — full table-by-table breakdown of BigQuery + Postgres.
- Added data inventory table to `chat-history/current-state.md`.

### Findings
- **BigQuery:** ALL analytics data present and current. Crime: 2.08M rows (2018-2026). Divvy: 34.8M rows (2020-2026). fact_station_day: 1.46M rows. BQML models: all 4 tables present.
- **Postgres:** Only `observability.dbt_test_results` exists. Streaming tables not populated (DAG not run this session). This is expected — streaming is architecturally separate from analytics.
- **Stale data in handoff:** `current-state.md` line 47 had row counts from a previous session (263K crime, 2.2K station_status) that no longer reflect reality. Corrected.

### Lesson
- **Handoff docs can go stale between sessions** — row counts and table existence noted at end of one session may not hold at the start of the next (containers rebuilt, volumes cleared, DAGs not re-run). Always verify data presence by querying directly, not by trusting the handoff doc.


## 2026-07-22 — dbt docs generate + serve

### Changes
- Ran `dbt docs generate` against BigQuery — produced `catalog.json` with 15 models, 4 sources, 89 tests, 1 seed, 871 macros.
- Started `dbt docs serve` in Docker container on port 8090 (8080 conflicts with Airflow). Interactive HTML docs accessible at http://localhost:8090.
- Updated `docs/wiki/dbt.md` with "dbt docs" section (commands, port, BigQuery gotcha).
- Updated `chat-history/current-state.md` URLs section with dbt docs URL.

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `RuntimeWarning: "table_owner" does not match the name of any column` | BigQuery doesn't have table owners like Postgres — dbt's catalog builder expects this column | Harmless warning. Catalog is still built correctly. No fix needed. |

### Lessons Summary
- **dbt docs port conflict** — `dbt docs serve` defaults to port 8080, which conflicts with Airflow webserver. Use `--port 8090` (or any free port).
- **`--host 0.0.0.0` required in Docker** — without it, dbt docs serve binds to localhost inside the container and is unreachable from the host.
- **`dbt docs generate` needs warehouse access** — it introspects the warehouse (INFORMATION_SCHEMA) to build catalog.json. Unlike `dbt compile`, it can't run offline.

---

## 2026-07-22 — Phase 5: CI/CD GitHub Actions workflows

### Changes
- Created `.github/workflows/ci.yml` — 4 parallel jobs: ruff lint, dbt parse, compose validate, build images. Triggers on PR to dev/prod.
- Created `.github/workflows/build.yml` — builds + pushes images to GHCR tagged `:dev`. Triggers on push to dev.
- Created `.github/workflows/release.yml` — semantic version tag + GitHub Release + versioned GHCR images. Triggers on push to prod.
- Created `.github/ci/profiles.yml` — CI-safe dbt profiles (dummy keyfile, never connects). Needed because `dbt/profiles.yml` is gitignored.
- Added `[tool.ruff]` config to `pyproject.toml` — line-length 100, excludes `dbt/dbt_packages`.
- Fixed 5 ruff lint errors in existing code: 3x f-string without placeholders (crime_batch_dag, divvy_trip_history_dag, divvy_stream), 2x unused imports (load_divvy_trips: `sys`, `datetime`).

### Errors & Fixes

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | Plan's CI used `pip install dbt-postgres` but project uses dbt-bigquery | Phase 4.3 switched from Postgres to BigQuery — plan template was stale | Changed to `pip install dbt-bigquery==1.12.0` in ci.yml |
| 2 | `dbt parse` would fail in CI — `dbt/profiles.yml` is gitignored | profiles.yml contains the real GCP key path, can't be committed | Created `.github/ci/profiles.yml` with dummy keyfile (`/dev/null`). dbt parse never connects, just needs adapter type to match. |
| 3 | GHCR push would fail — `github.repository` has uppercase (`SagarMarthandan`) | GHCR requires lowercase image paths | Added `REPO_LC` env var that lowercases `github.repository` via `tr '[:upper:]' '[:lower:]'` |
| 4 | Release workflow: `git log v1.0.0..HEAD` fails — v1.0.0 doesn't exist as a real tag | Legacy tags (v1–v27) are non-semantic; resetting LATEST to v1.0.0 creates a non-existent range | Added `RANGE` variable: `HEAD` for legacy tags, `$LATEST..HEAD` for semantic tags |
| 5 | Plan's build.yml used `--build-arg BUILD_DATE/GIT_SHA/VERSION` | Dockerfiles have no corresponding `ARG` declarations | Removed build args — images build fine without them |
| 6 | Plan's build.yml tagged `chicago-data-pipeline-dbt-build` | Compose uses `image: chicago-data-pipeline-dbt:latest` (via `image:` key in dbt-build service) | Used correct image name `chicago-data-pipeline-dbt:latest` |
| 7 | ruff found 5 lint errors in existing code | F-strings without placeholders (F541) + unused imports (F401) | Fixed all 5: removed `f` prefix from 3 strings, removed `sys` + `datetime` imports |
| 8 | `softprops/action-gh-release@v1` is deprecated | v1 uses old Node 16 runtime | Upgraded to `@v2` |

### Lessons Summary
- **Plan templates go stale** — the Phase 5 plan was written before Phase 4.3's BigQuery switch. Always verify plan assumptions against current code before implementing.
- **dbt parse doesn't need real credentials** — it only parses SQL/Jinja, never opens a DB connection. A CI-safe profiles.yml with a dummy keyfile is sufficient.
- **GHCR requires lowercase** — `github.repository` preserves case from the GitHub URL. Always lowercase it for GHCR image paths.
- **Legacy tags break semantic versioning logic** — `git describe` returns the latest tag regardless of format. Non-semantic tags (v1, v27) need special handling in version bump logic.
- **ruff catches real issues** — 3 f-strings without placeholders and 2 unused imports were sitting in production code. CI linting would have caught these on the first PR.