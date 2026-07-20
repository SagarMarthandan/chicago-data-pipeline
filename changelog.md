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
- Created `docs/operations-performed.md` — structural audit trail of what was built and why
- Created `README.md` with 3 Mermaid diagrams (architecture, pipeline flow, roadmap)
- Updated `AGENTS.md` — added rules 9–11 (read changelog, read knowledge, update operations-performed), updated header note and repo structure to reference all three docs
- Updated `docs/conventions/airflow.md` — DockerOperator network/volume names → `chicago-data-pipeline_*` with `COMPOSE_PROJECT_NAME` reference
- Updated `docs/conventions/docker.md` — added `COMPOSE_PROJECT_NAME=chicago-data-pipeline` to `.env` example and networking section; updated WSL path to `~/chicago-data-pipeline`
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
- **Three-doc system:** `changelog.md` (errors), `docs/knowledge.md` (reference), `docs/operations-performed.md` (audit trail) — each has a distinct purpose, don't merge them.

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

**Files affected:** `docs/knowledge.md` (9 diagrams), `docs/phases/phase-1.1-docker.md` (1 diagram), `README.md` (3 diagrams)

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
- Documented full GBFS schema in `docs/knowledge/data-sources.md`

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
- Updated `docs/knowledge/kafka.md` with full setup details, commands, and concepts

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
- Created `docs/knowledge/grafana.md` — comprehensive Grafana reference (concepts, provisioning, env var gotchas, jsonData.database deep dive, DAG run order, useful commands, 10 common mistakes) with 8 mermaid diagrams
- Created `docs/phases/phase-3.1-grafana.md` — phase completion doc
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
