# Session 03 — Phase 2.3: Kafka Producer + Documentation Rewrite

**Date:** 2026-07-15
**Session type:** Code implementation + documentation expansion
**Phase:** 2.3

## Summary

Built the Divvy GBFS → Kafka producer (`kafka/producers/divvy_producer.py`), verified it with real data (2,016 messages per poll, 3 partitions), and then rewrote `docs/knowledge/kafka.md` from a command reference into a comprehensive conceptual guide with 8 mermaid diagrams covering all core Kafka concepts.

## What Was Done

### 1. Producer Implementation (`kafka/producers/divvy_producer.py`)
- Polls Divvy GBFS `station_status.json` every 60 seconds
- Publishes each station as a Kafka message:
  - **Key:** `station_id` (string) — same station → same partition → ordered processing
  - **Value:** full station status JSON (all fields from GBFS)
- `acks="all"` — safest delivery guarantee (waits for all in-sync replicas)
- Graceful SIGINT/SIGTERM shutdown with `producer.flush()` before exit
- CLI flags: `--once` (single poll for testing), `--interval N` (custom poll cadence), `--bootstrap` (Kafka broker address)
- `kafka-python` 3.0.8 added to host venv via `uv add kafka-python`

### 2. Airflow Container Preparation (for Phase 2.6)
- Added `kafka-python` to `airflow/requirements.txt` — Airflow image needs rebuild before Phase 2.6
- Added `./kafka:/opt/airflow/kafka` volume mount to Airflow in docker-compose.yml — so the producer script is accessible inside the Airflow container

### 3. Verification
- **Single poll (`--once`):** 2,016 messages produced, all consumed successfully
- **Partition distribution:** 720 / 661 / 635 across partitions 0/1/2 — even distribution via `hash(station_id) % 3`
- **Real data confirmed:** Consumed messages contained real station IDs, bike counts, and timestamps
- **Continuous mode:** Ran multiple poll cycles (3 polls × 2,016 = 6,048 total messages), all consumed
- **Graceful shutdown:** SIGINT triggered clean flush + exit, no message loss

### 4. Documentation Rewrite (`docs/knowledge/kafka.md`)
Rewrote from ~80-line command reference to 425-line comprehensive guide:
- **What is Kafka** — pipeline context, why Kafka vs direct API→Postgres
- **Core Concepts** (each with mermaid diagram):
  1. Cluster — single broker vs production, Zookeeper coordination
  2. Topic — append-only message log
  3. Partition — 3 partitions with key-based distribution
  4. Offset — sequential IDs as consumer bookmarks
  5. Producer — GBFS → parse → send → flush → sleep loop
  6. Consumer — consumer groups, partition assignment
  7. Broker — what the broker stores (logs, metadata, offsets)
  8. Zookeeper — broker registry, leader election, topic config
- **Message flow sequence diagram** — end-to-end journey of a single message
- **Our Setup** — Confluent images, topic config, listeners, single-broker overrides, topic creation lesson
- **Useful Commands** — all kafka-topics, kafka-console-consumer/producer, and producer run commands

### 5. Full Documentation Audit
Verified all docs are updated for Phase 2.3:
- `changelog.md` — Phase 2.3 entry (2 errors, 5 decisions, 3 lessons)
- `docs/operations-performed.md` — Phase 2.3 audit entry
- `docs/phases/phase-2.3-divvy-producer.md` — full phase completion doc
- `docs/phases/README.md` — phase index updated
- `docs/knowledge/kafka.md` — rewritten (see above)
- `docs/knowledge/architecture.md` — added producer details to Kafka section, updated file→container table
- `docs/knowledge/index.md` — updated kafka.md and architecture.md descriptions
- Ran mermaid syntax scanner on all `.md` files — no issues found

## Decisions Made

- **station_id as message key** — same station → same partition → chronological order per station. Critical for time-series analysis.
- **acks="all"** — safest delivery guarantee. Worth the latency for a learning project.
- **Producer runs on host for now** — connects to `localhost:29092`. Phase 2.6 will containerize it inside Airflow.
- **Explicit topic creation (3 partitions)** — `KAFKA_NUM_PARTITIONS` env var doesn't work with Confluent images. Auto-create defaults to 1 partition. Must use `kafka-topics --create --partitions 3`.
- **kafka-python 3.0.8** — pinned version. Note: 3.0.x removed `NoBrokersAvailable` exception.

## Files Created/Modified

- `kafka/producers/divvy_producer.py` — **NEW** — GBFS → Kafka producer
- `pyproject.toml` / `uv.lock` — added `kafka-python` 3.0.8
- `airflow/requirements.txt` — added `kafka-python`
- `docker-compose.yml` — added `./kafka:/opt/airflow/kafka` volume to Airflow
- `docs/knowledge/kafka.md` — **REWRITTEN** — full conceptual guide with 8 mermaid diagrams
- `docs/knowledge/architecture.md` — added producer details, updated file→container table
- `docs/knowledge/index.md` — updated kafka.md + architecture.md descriptions
- `changelog.md` — Phase 2.3 entry
- `docs/operations-performed.md` — Phase 2.3 audit entry
- `docs/phases/phase-2.3-divvy-producer.md` — created phase completion doc
- `docs/phases/README.md` — updated phase index
- `chat-history/current-state.md` — updated handoff for Phase 2.1–2.3

## Errors Encountered

| # | Error | Root Cause | Fix |
|---|---|---|---|
| 1 | `NoBrokersAvailable` not catchable | `kafka-python` 3.0.x removed this exception class | Catch `KafkaError` (base class) instead |
| 2 | Auto-created topic had 1 partition | `KAFKA_NUM_PARTITIONS` env var doesn't work with Confluent images; auto-create uses `server.properties` default (1) | Explicit `kafka-topics --create --partitions 3 --replication-factor 1` |

## Key Context

- The test topic `divvy_station_status` was **deleted** at end of Phase 2.3 testing — clean slate for Phase 2.4. Must be recreated before starting 2.4.
- `kafka-python` is NOT installed in the Airflow container yet — added to `airflow/requirements.txt` but Airflow image needs rebuild (`docker compose build airflow-init`) before Phase 2.6.
- The producer's `key_serializer` and `value_serializer` trigger DeprecationWarnings in kafka-python 3.0.8 but work correctly.
- Total messages tested: 6,048 (3 polls × 2,016 stations) — all consumed successfully.
- `docs/knowledge/kafka.md` is 425 lines / 17KB — under the 500 line / 20KB split threshold.

## User Preferences Learned

- User asked for conceptual Kafka explanations with mermaid diagrams — wants to understand the "why" and "how" of Kafka internals, not just commands
- User wants all documentation kept up to date after each sub-phase
