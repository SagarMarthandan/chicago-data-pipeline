# Changelog

A running log of changes, errors, and fixes throughout the project. Use this to spot patterns in mistakes and avoid repeating them.

> **Format:** `YYYY-MM-DD` — what happened, what broke, what fixed it, and the lesson.

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

<!-- Append new entries below. Keep the format consistent. -->
