# AGENTS.md — Chicago Crime + Divvy Pipeline

> **Read this file before doing anything in this repo.**
> If you are an AI assistant (Devin, Claude, Copilot, etc.), follow `docs/learning-protocol.md` for how to interact with me. Read `chat-history/current-state.md` first for session context (handoff doc), then `changelog.md` for past errors and fixes, `docs/knowledge/` for reference material, and `docs/operations-performed.md` for a record of what has been built and why.

## Project

A data engineering learning project that answers:

> **Does crime near a Divvy bike-share station affect ridership?**

Full plan: see `chicago-pipeline-plan.md` (in repo root).

## Tech Stack

| Layer | Tool | Phase |
|---|---|---|
| Warehouse | Postgres (local) → BigQuery (cloud) | 1 → 4 |
| Batch processing | Spark DataFrames | 1 |
| Streaming | Kafka + Spark Structured Streaming | 2 |
| Transformation | DBT | 1+ |
| Orchestration | Airflow | 1+ |
| Observability | Grafana | 3 |
| Ingestion (cloud) | Airbyte | 4 |
| Infra (cloud) | Terraform | 4 |
| Containerization | Docker + Docker Compose | 1+ |

## Phase Gates — DO NOT SKIP AHEAD

| Phase | Status | Done when |
|---|---|---|
| 1 — Batch | Not started | `docker compose up` → DAG runs → DBT marts queryable |
| 2 — Stream | Locked | Divvy live data in Postgres via Kafka + Spark Streaming |
| 3 — Observability | Locked | Grafana dashboards + DBT tests + Airflow SLAs |
| 4 — Cloud | Locked | Terraform → BigQuery + Airbyte |

**Rule:** Phase N+1 stays locked until Phase N works end-to-end and is verified.
If I ask you to help with Phase 2 while Phase 1 isn't done, refuse and remind me.

## Environment

- **OS:** WSL2 (Ubuntu) on Windows 10
- **AI assistant:** Devin (GLM 5.2)
- **Editor:** Whatever Devin uses
- **Docker:** Docker Desktop with WSL2 backend

## Rules for AI Assistants

1. **Follow `docs/learning-protocol.md`** — it defines how you interact with me.
2. **Follow `docs/conventions/*.md`** — they define engineering standards for each tool.
3. **Never write code for me unless I explicitly say "write the code" or "show me."**
4. **When I hit an error, explain the cause, don't just paste a fix.**
5. **When I ask "how do I do X," ask what I've tried first.**
6. **Don't skip phases. Don't add tools I haven't reached yet.**
7. **When reviewing my code, point out issues but let me fix them.**
8. **If I'm about to do something that will cause a known mistake (see plan's "mistakes to expect" section), let me make it — then help me understand why.**
9. **Read `changelog.md` before starting work** — it logs past errors and fixes. Check it to avoid repeating mistakes. Update it when we fix a new error.
10. **Read `docs/knowledge/` for reference** — it has useful commands, syntax, and explanations, organized by topic (one file per section). Update the relevant file when you learn something worth remembering. See `docs/knowledge/index.md` for the section directory.
11. **Update `docs/operations-performed.md` after structural changes** — record what files/structures were created and why. This is the audit trail of what exists in the repo.
12. **Read `chat-history/current-state.md` for session context** — it's the handoff document with current state, active decisions, and next steps. Read `chat-history/YYYY-MM-DD/` folders for detailed conversation history by topic. Update `current-state.md` at the end of a session and create new chunks when context approaches 75% usage.
13. **Create a phase-completion document after each sub-phase** — when a sub-phase (e.g., 1.1, 1.2) is verified working, create `docs/phases/phase-X.Y.md` using the template in `docs/phases/TEMPLATE.md`. This document captures: what was built, how files connect (with section-by-section mermaid diagrams), what errors were hit, and what's next. See `docs/phases/README.md` for the full process.
14. **Keep documentation maintainable as the project grows** — when a `.md` file exceeds ~500 lines or ~20KB, split it into a folder with one file per section + an `index.md` for navigation. Append-only logs (`changelog.md`, `operations-performed.md`) stay as single files but get a Table of Contents with anchor links at the top. Reference files should be organized by topic, not chronology. After splitting, update all references across the repo.
15. **Never delete working state to "force" a refresh** — if something isn't updating after a change, find the proper mechanism to trigger a re-parse/reload (wait for the parse cycle, use the correct CLI command, restart the right service). Do NOT delete database entries, serialized state, or cached data hoping it will be rebuilt. If you delete working state, you own the responsibility of recreating it — and you may not know how. **Diagnose first, touch second.** If you break something by touching it, document the mistake in `changelog.md` so it's not repeated.

## Repo Structure (target)

See `chicago-pipeline-plan.md` for the full structure. Key directories:
```
├── AGENTS.md
├── changelog.md            ← errors, fixes, lessons (read before working)
├── chat-history/           ← conversation reference (read current-state.md first)
├── docker-compose.yml
├── ingestion/
├── spark/
├── kafka/
├── airflow/
├── dbt/
├── grafana/
├── terraform/             ← Phase 4 only
    ├── knowledge/                ← reference (one file per topic)
    │   ├── index.md              ← section directory with navigation links
    │   ├── wsl.md
    │   ├── uv.md
    │   ├── docker-compose.md
    │   ├── architecture.md       ← how files connect (mermaid diagrams)
    │   ├── postgres.md
    │   ├── dbt.md
    │   ├── spark.md
    │   ├── kafka.md
    │   ├── airflow.md            ← Airflow 2.x vs 3.x comparison
    │   ├── git.md
    │   ├── data-sources.md
    │   └── mermaid-syntax.md
    ├── learning-protocol.md
    ├── operations-performed.md    ← audit trail of what was built and why
    ├── phases/                    ← phase-completion docs (one per sub-phase)
    │   ├── README.md              ← explains the phase-doc system
    │   ├── TEMPLATE.md            ← copy this to start a new phase doc
    │   └── phase-1.1-docker.md    ← Phase 1.1: Docker Compose services
    └── conventions/
        ├── spark.md
        └── airflow.md
```
