# AGENTS.md — Chicago Crime + Divvy Pipeline

> **Read this file before doing anything in this repo.**
> If you are an AI assistant (Devin, Claude, Copilot, etc.), follow `docs/learning-protocol.md` for how to interact with me. Read `changelog.md` for past errors and fixes, `docs/knowledge.md` for reference material, and `docs/operations-performed.md` for a record of what has been built and why.

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
10. **Read `docs/knowledge.md` for reference** — it has useful commands, syntax, and explanations. Update it when you learn something worth remembering.
11. **Update `docs/operations-performed.md` after structural changes** — record what files/structures were created and why. This is the audit trail of what exists in the repo.

## Repo Structure (target)

See `chicago-pipeline-plan.md` for the full structure. Key directories:
```
chicago-data-pipeline/      ← repo root (you are here)
├── AGENTS.md
├── changelog.md            ← errors, fixes, lessons (read before working)
├── docker-compose.yml
├── ingestion/
├── spark/
├── kafka/
├── airflow/
├── dbt/
├── grafana/
├── terraform/             ← Phase 4 only
└── docs/
    ├── knowledge.md               ← reference commands, syntax, explanations
    ├── learning-protocol.md
    ├── operations-performed.md    ← audit trail of what was built and why
    └── conventions/
        ├── docker.md
        ├── dbt.md
        ├── spark.md
        └── airflow.md
```
