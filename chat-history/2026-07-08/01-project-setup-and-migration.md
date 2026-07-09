# Project Setup & Migration (Windows → WSL)

## Summary
Initial project planning on Windows in Devin IDE, then migration to WSL filesystem. Created planning docs, conventions, AGENTS.md, and the three-doc system (changelog, knowledge, operations-performed). Renamed repo root to match `COMPOSE_PROJECT_NAME`.

## Decisions Made
- **Repo name `chicago-data-pipeline`** — matches `COMPOSE_PROJECT_NAME` for predictable Docker network/volume names (all lowercase)
- **Three-doc system** — `changelog.md` (errors/fixes/lessons), `docs/knowledge.md` (reference), `docs/operations-performed.md` (audit trail). Each has distinct purpose, don't merge.
- **Flattened folder structure** — moved `AGENTS.md`, plan, `docs/` from nested `devin/` subfolder to repo root
- **AGENTS.md rules 9-11** — AI must read changelog before work, read knowledge for reference, update operations-performed after structural changes
- **Socratic learning mode** — AI asks what user tried, doesn't hand fixes. Explicit mode switches: "write the code", "I give up just fix it", "pair with me"

## Files Created/Modified
- `chicago-pipeline-plan.md` — full phased build plan (27 KB)
- `AGENTS.md` — root agent instructions, phase gates, learning mode rules
- `docs/learning-protocol.md` — Socratic mode definition
- `docs/conventions/docker.md` — Docker best practices
- `docs/conventions/dbt.md` — DBT modeling conventions, `try_cast` macro
- `docs/conventions/spark.md` — Spark job conventions
- `docs/conventions/airflow.md` — Airflow DAG conventions
- `changelog.md` — pre-populated with 5 planning-phase bugs + 3 setup bugs
- `docs/knowledge.md` — reference by tool
- `docs/operations-performed.md` — audit trail
- `README.md` — 3 Mermaid diagrams (architecture, pipeline flow, roadmap)
- `.gitignore` — excludes .env, data files, Python artifacts, DBT target, etc.
- `git init` on `main` branch

## Key Context
- Project started on Windows in Devin IDE, then migrated to WSL for performance (cross-filesystem mounts are slow)
- User is learning data engineering — wants to understand the *why* behind every choice
- No pipeline code exists yet — all files are planning, conventions, documentation

## Errors Encountered
- `TRY_CAST` doesn't exist in Postgres (Snowflake/DuckDB syntax) — created `try_cast` DBT macro
- `EXTRACT(date FROM ...)` errors in Postgres — use `::date` instead
- `try_cast` macro used wrong Jinja variable — `adapter.type()` not `target_var.adapter`
- Docker Compose network names unpredictable with mixed-case folder name — set `COMPOSE_PROJECT_NAME`
- Bash commands failed after folder rename — tool cwd was cached to old path
- `AGENTS.md` lost opening code fence during edit — re-inserted
- README Mermaid diagrams didn't persist after incremental edits — rewrote with `write` tool
