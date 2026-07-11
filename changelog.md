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

<!-- Append new entries below. Keep the format consistent. -->
