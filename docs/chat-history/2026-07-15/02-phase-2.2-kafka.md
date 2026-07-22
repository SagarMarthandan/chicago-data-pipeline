# Session 02 — Phase 2.2: Kafka + Zookeeper Docker Services

**Date:** 2026-07-15
**Session type:** Infrastructure setup — Kafka broker
**Phase:** 2.2

## Summary

Added Kafka and Zookeeper services to `docker-compose.yml` using Confluent Platform 7.6.0 images (replacing the no-longer-free Bitnami images). Configured two Kafka listeners (internal Docker network + host), single-broker overrides, and verified the setup with topic creation and message produce/consume round-trips.

## What Was Done

### 1. Image Selection: Confluent over Bitnami
Bitnami moved behind a commercial subscription in 2026. Selected `confluentinc/cp-kafka:7.6.0` and `confluentinc/cp-zookeeper:7.6.0` — free, stable, production-hardened, pinned versions.

### 2. docker-compose.yml Changes
Added two services:
- **zookeeper** — `confluentinc/cp-zookeeper:7.6.0`, port 2181 (internal only), healthcheck via `echo srvr | nc localhost 2181`, two named volumes (`zookeeper_data`, `zookeeper_log`)
- **kafka** — `confluentinc/cp-kafka:7.6.0`, two listeners (9092 internal, 29092 host), depends on zookeeper healthcheck, named volume `kafka_data`, healthcheck via `kafka-broker-api-versions` with 20s start_period

Also added `KAFKA_BOOTSTRAP_SERVERS: kafka:9092` env var to `spark-master` and `spark-worker` (will be needed for Phase 2.4 Spark Streaming).

### 3. Single-Broker Overrides (Critical)
Three Kafka settings default to 3 (production cluster assumption). With a single broker, they MUST be set to 1:
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`
- `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`
- `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1`

Without these, Kafka can't create internal topics (`__consumer_offsets`, `__transaction_state`) and consumers can't commit offsets.

### 4. Two Listeners Configuration
- `PLAINTEXT://kafka:9092` — Docker network internal. Spark Structured Streaming and containerized producer connect here.
- `PLAINTEXT_HOST://localhost:29092` — host machine. For `kafka-console-consumer` testing and the host-based producer (Phase 2.3).

Required env vars:
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092`
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT`
- `KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT`

### 5. Verification
- Both services healthy (`docker compose ps`)
- Created test topic, produced test messages, consumed them back — round-trip confirmed
- Tested from both listeners (internal 9092 and host 29092)

## Decisions Made

- **Confluent Platform 7.6.0** (not Bitnami) — Bitnami no longer free. Confluent images are free, stable, and pinned.
- **Zookeeper mode** (not KRaft) — more educational, traditional setup. Most existing deployments still use ZK. KRaft is newer and less battle-tested.
- **Two listeners** — internal for Docker services, external for host testing. Standard Kafka pattern.
- **Auto-create topics enabled** — dev convenience. But explicit creation needed for custom partition counts (see Phase 2.3 lesson).

## Files Created/Modified

- `docker-compose.yml` — added zookeeper + kafka services, KAFKA_BOOTSTRAP_SERVERS on spark services, 3 named volumes
- `docs/knowledge/kafka.md` — created initial reference (setup details, commands)
- `docs/knowledge/architecture.md` — added section 9 (Kafka + Zookeeper Docker architecture, mermaid diagrams)
- `docs/knowledge/index.md` — added kafka.md to knowledge index
- `changelog.md` — added Phase 2.2 entry
- `docs/operations-performed.md` — added Phase 2.2 audit entry
- `docs/phases/phase-2.2-kafka.md` — created phase completion doc
- `docs/phases/README.md` — updated phase index

## Key Context

- Zookeeper coordinates Kafka brokers: broker registry, leader election, topic config
- Kafka stores messages as append-only logs on disk (`/var/lib/kafka/data`)
- `__consumer_offsets` is an internal Kafka topic that tracks consumer group positions
- Healthcheck `start_period: 20s` on Kafka is needed because Kafka takes ~30-40s to fully initialize
- Named volumes (`kafka_data`, `zookeeper_data`, `zookeeper_log`) persist across `docker compose down` (without `-v`)

## Errors Encountered

None during this sub-phase. The single-broker overrides were identified proactively (from Confluent docs) before they could cause issues.

## User Preferences Learned

- User wanted to understand why Zookeeper exists and what it does — explained broker coordination, leader election, and KRaft alternative
