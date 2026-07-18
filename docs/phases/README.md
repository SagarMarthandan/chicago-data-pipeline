# Phase Completion Documents

## What This Is

Every sub-phase (1.1, 1.2, 1.3, etc.) gets a **phase-completion document** once it's verified working. This is not a running log (that's `changelog.md`) or a reference (that's `docs/knowledge/`) — it's a **snapshot** of what was built, how it connects, and what it looks like at the moment the sub-phase was declared done.

## When to Create One

Create `docs/phases/phase-X.Y-<short-name>.md` **after** the sub-phase is verified working end-to-end. Not before. Not during. After.

The trigger is: "this sub-phase meets its done-when criteria from the phase gate table in AGENTS.md."

## How to Create One

1. Copy `TEMPLATE.md` to `docs/phases/phase-X.Y-<short-name>.md`
   - Example: `phase-1.1-docker.md`, `phase-1.2-ingestion.md`, `phase-1.3-spark-batch.md`
2. Fill in every section
3. Add section-by-section mermaid diagrams showing how files connect (like the "How Everything Connects" section in `docs/knowledge/architecture.md`)
4. Update `chat-history/current-state.md` to reference the new phase doc

## What Goes In Each Section

| Section | Content | Source |
|---|---|---|
| Summary | 2-3 sentences: what was built, what works | You write this |
| Files Created/Modified | Every file touched, with one-line purpose | `docs/operations-performed.md` for this date |
| Architecture (mermaid) | One high-level "what was built" diagram. Detailed diagrams live in `docs/knowledge/architecture.md` — don't duplicate. | You write this |
| Errors Hit | Every error encountered and its fix | `changelog.md` entries for this date |
| Decisions Made | Key choices and why | Your judgment + chat-history |
| Verification | How you confirmed it works (commands run, output seen) | Actual commands you ran |
| What's Next | The next sub-phase and what it requires | Phase plan in AGENTS.md |

## Relationship to Other Docs

```
docs/phases/phase-1.1-docker.md     ← snapshot of THIS phase (what was built, errors, decisions, verification)
changelog.md                        ← running log of ALL errors ever hit
docs/knowledge/                     ← reference for ALL concepts/commands/architecture diagrams (one file per topic)
docs/operations-performed.md        ← audit trail of ALL structural changes
chat-history/current-state.md       ← handoff for the NEXT session
```

**No duplication.** The phase doc has a single high-level diagram + a pointer to `docs/knowledge/architecture.md` for detailed architecture. The knowledge folder is the permanent reference for how things connect. The phase doc is the historical snapshot of what happened.

## Phase Doc Index

| File | Sub-Phase | Status |
| `phase-1.1-docker.md` | Docker Compose services | Complete |
| `phase-1.2-ingestion.md` | Socrata API ingestion script | Complete |
| `phase-1.3-spark-batch.md` | Spark batch job (Parquet → Postgres) | Complete |
| `phase-1.4-dbt-models.md` | DBT staging + marts | Complete |
| `phase-1.5-airflow-dag.md` | Airflow DAG + dbt Docker image | Complete |
| `phase-1.6-verification.md` | Phase 1 end-to-end verification | Complete |
| `phase-2.1-gbfs-data-source.md` | Divvy GBFS data source exploration | Complete |
| `phase-2.2-kafka.md` | Kafka + Zookeeper Docker services | Complete |
| `phase-2.3-divvy-producer.md` | Kafka producer (GBFS → topic) | Complete |
| `phase-2.4-spark-streaming.md` | Spark Structured Streaming (Kafka → Postgres) | Complete |
| `phase-2.5-dbt-stream-models.md` | DBT models for stream (stg_station_status + fact_station_reads) | Complete |
| `phase-2.6-airflow-stream-dag.md` | Airflow DAG for stream (start/monitor producer + streaming) | Complete |
| `phase-3.1-grafana.md` | Grafana service + datasources + dashboards | Complete |
