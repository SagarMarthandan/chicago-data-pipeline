# AGENTS.md — Chicago Crime + Divvy Pipeline

> **Read this file before doing anything in this repo.**
> If you are an AI assistant (Devin, Claude, Copilot, etc.), follow `docs/learning-protocol.md` for how to interact with me. Read `docs/chat-history/current-state.md` first for session context (handoff doc), then `CHANGELOG.md` for past errors and fixes, `docs/wiki/` for reference material, and `docs/phase/` for consolidated phase documentation.

## Project

A data engineering learning project that answers:

> **Does crime near a Divvy bike-share station affect ridership?**

Full plan: see `docs/chicago-pipeline-plan.md`.

## Tech Stack

| Layer | Tool | Phase |
|---|---|---|
| Warehouse | Postgres (local) → BigQuery (cloud) | 1 → 4 |
| Batch processing | Spark DataFrames | 1 |
| Streaming | Kafka + Spark Structured Streaming | 2 |
| Transformation | DBT | 1+ |
| Orchestration | Airflow | 1+ |
| Observability | Grafana | 3 |
| Ingestion (cloud) | dlt (data load tool) | 4 |
| Infra (cloud) | Terraform | 4 |
| Containerization | Docker + Docker Compose | 1+ |
| CI/CD | GitHub Actions + GHCR | 5 |

## Phase Gates — ALL COMPLETE

| Phase | Status | Done when |
|---|---|---|
| 1 — Batch | ✅ Complete | `docker compose up` → DAG runs → DBT marts queryable |
| 2 — Stream | ✅ Complete | Divvy live data in Postgres via Kafka + Spark Streaming |
| 3 — Observability | ✅ Complete | Grafana dashboards + DBT tests + Airflow robustness |
| 4 — Cloud | ✅ Complete | Terraform → BigQuery + dlt + correlation analysis + BQML |
| 5 — CI/CD | ✅ Complete | GitHub Actions: branch protection + PR checks + versioned releases |

**All 5 phases are complete.** The driving question is answered.

## Environment

- **OS:** WSL2 (Ubuntu) on Windows 10
- **AI assistant:** Devin (GLM 5.2)
- **Editor:** Whatever Devin uses
- **Docker:** Docker Desktop with WSL2 backend
- **Git:** `prod` (default, protected) + `dev` (protected). All changes via PR.

## Rules for AI Assistants

1. **Follow `docs/learning-protocol.md`** — it defines how you interact with me.
2. **Follow `docs/wiki/conventions/*.md`** — they define engineering standards for each tool.
3. **Never write code for me unless I explicitly say "write the code" or "show me."**
4. **When I hit an error, explain the cause, don't just paste a fix.**
5. **When I ask "how do I do X," ask what I've tried first.**
6. **Don't skip phases. Don't add tools I haven't reached yet.**
7. **When reviewing my code, point out issues but let me fix them.**
8. **If I'm about to do something that will cause a known mistake (see plan's "mistakes to expect" section), let me make it — then help me understand why.**
9. **Read `CHANGELOG.md` before starting work** — it logs past errors and fixes. Check it to avoid repeating mistakes. Update it when we fix a new error.
10. **Read `docs/wiki/` for reference** — it has useful commands, syntax, and explanations, organized by topic (one file per section). Update the relevant file when you learn something worth remembering. See `docs/wiki/index.md` for the section directory.
11. **Read `docs/phase/` for phase documentation** — consolidated phase docs (phase-1.md through phase-5.md) cover what was built, scripts created, errors, fixes, mermaid diagrams, and verification.
12. **Read `docs/chat-history/current-state.md` for session context** — it's the handoff document with current state, active decisions, and next steps. Read `docs/chat-history/YYYY-MM-DD/` folders for detailed conversation history by topic. Update `current-state.md` at the end of a session and create new chunks when context approaches 75% usage.
13. **Keep documentation maintainable as the project grows** — when a `.md` file exceeds ~500 lines or ~20KB, split into a folder with one file per section + an `index.md` for navigation. Append-only logs (`CHANGELOG.md`) stay as a single file but get a Table of Contents with anchor links at the top. Reference files should be organized by topic, not chronology. After splitting, update all references across the repo.
14. **Never delete working state to "force" a refresh** — if something isn't updating after a change, find the proper mechanism to trigger a re-parse/reload (wait for the parse cycle, use the correct CLI command, restart the right service). Do NOT delete database entries, serialized state, or cached data hoping it will be rebuilt. If you delete working state, you own the responsibility of recreating it — and you may not know how. **Diagnose first, touch second.** If you break something by touching it, document the mistake in `CHANGELOG.md` so it's not repeated.

## Repo Structure

```
chicago-data-pipeline/
├── AGENTS.md                 # this file — AI assistant rules + phase gates
├── CHANGELOG.md              # errors, fixes, lessons (read before working)
├── README.md                 # project overview, badges, architecture, structure
├── docker-compose.yml        # 12 services: Postgres, Spark, Airflow, Kafka, Grafana
├── init.sql                  # Postgres init: 3 schemas + airflow DB
├── pyproject.toml            # uv project mode + ruff config
├── .env.example              # env var template
├── .github/                  # Phase 5 — CI/CD workflows
│   ├── workflows/            # ci.yml, build.yml, release.yml
│   └── ci/profiles.yml       # CI-safe dbt profiles
├── airflow/                  # Airflow 3.0 DAGs, Dockerfile, scripts
├── spark/                    # Spark Dockerfile + jobs (batch + streaming)
├── kafka/                    # Kafka producer
├── ingestion/                # Socrata download + dlt S3→BigQuery
├── dbt/                      # DBT models (staging + marts + BQML)
├── grafana/                  # dashboards + datasource provisioning
├── terraform/                # GCP infra as code
└── docs/
    ├── index.md              # navigation entry point
    ├── chicago-pipeline-plan.md  # full phased design
    ├── learning-protocol.md  # AI assistant interaction rules
    ├── phase/                # consolidated phase docs (phase-1.md → phase-5.md)
    ├── wiki/                 # technology reference + commands + conventions
    │   ├── index.md          # wiki navigation
    │   ├── conventions/      # coding standards per tool
    │   └── *.md              # one file per technology topic
    └── chat-history/         # daily conversation logs + current-state.md handoff
```
