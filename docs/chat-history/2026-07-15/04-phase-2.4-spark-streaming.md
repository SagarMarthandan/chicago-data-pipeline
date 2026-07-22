# Phase 2.4 — Spark Structured Streaming

**Date:** 2026-07-15
**Session topic:** Built the Spark Structured Streaming consumer that reads from Kafka and writes to Postgres. Phase 2.4 complete.

## What was done

### Prerequisites
- Started Docker services (postgres, spark-master, spark-worker, zookeeper, kafka)
- Recreated Kafka topic `divvy_station_status` (3 partitions, replication factor 1) — was deleted at end of Phase 2.3 testing
- Confirmed `apache/spark:3.5.1` does NOT include Kafka connector JARs — only ships core Spark JARs

### Build
1. **Added 4 Kafka connector JARs to `spark/Dockerfile`:**
   - `spark-sql-kafka-0-10_2.12-3.5.1.jar` — main connector (KafkaSourceProvider)
   - `spark-token-provider-kafka-0-10_2.12-3.5.1.jar` — auth token dependency
   - `kafka-clients-3.5.1.jar` — Kafka protocol client (network I/O)
   - `commons-pool2-2.11.1.jar` — connection pooling (used by token provider)
   - Same pattern as PostgreSQL JDBC driver — baked in, not `--packages` at runtime

2. **Created `spark/jobs/divvy_stream.py`** — Structured Streaming consumer:
   - `readStream.format("kafka")` → subscribes to `divvy_station_status`
   - `from_json()` with typed schema handling all 4 GBFS quirks:
     - station_id as StringType (mixed UUID + numeric)
     - is_* fields as IntegerType → CAST AS BOOLEAN (GBFS uses 0/1, not booleans)
     - Optional scooter fields as nullable
     - last_reported as LongType → from_unixtime → timestamp
   - Filter stale stations: `last_reported > now() - 1 hour` (drops 888/2016 = 44%)
   - `foreachBatch(write_batch)` → JDBC append to `raw.station_status`
   - Checkpoint at `/opt/spark/checkpoints/divvy_stream`
   - 60s trigger (matches producer poll interval)
   - `--once` mode for testing, continuous mode for production

3. **Added `spark_checkpoints` named volume** to `docker-compose.yml` + mounted to spark-master

4. **Created `raw.station_status` table** in Postgres (18 columns: 14 station fields + 3 Kafka metadata + 1 ingest timestamp)

5. **Added checkpoint directory creation** to `spark/Dockerfile`:
   - `RUN mkdir -p /opt/spark/checkpoints && chown spark:spark /opt/spark/checkpoints`
   - Named volumes inherit ownership from image directory on first mount

### Verification
- `--once` mode: 2,016 Kafka messages → 1,128 rows inserted (888 stale filtered)
- Boolean casts verified: is_renting/is_returning/is_installed show true/false correctly
- Optional scooter fields: 1,099/1,128 non-null (tolerated absence correctly)
- Continuous mode (producer + streaming both running): 5 micro-batches in 5 minutes, ~1,128 rows per batch, 5,640 total rows
- Row count grew from 1,128 → 5,640 over 150s — pipeline operates continuously

### Errors hit
1. **Checkpoint mkdir failed** — named volume mounted as root:root, Spark runs as `spark` (UID 185). Fixed with `chown` + Dockerfile `RUN mkdir + chown`.
2. **AQE warning for streaming** — `spark.sql.adaptive.enabled` not supported for streaming DataFrames. Warning only, Spark silently disables it. No action needed.

### Docs updated
- `changelog.md` — Phase 2.4 entry (2 errors, 9 decisions, 5 lessons)
- `docs/operations-performed.md` — Phase 2.4 audit trail
- `docs/knowledge/spark.md` — Kafka connector JARs table, foreachBatch example, checkpointing section, AQE warning
- `docs/knowledge/kafka.md` — Consumer section rewritten (future→present), offset management comparison table, Spark-Kafka config options, checkpoint ownership gotcha, Our Setup table updated, message flow sequence diagram updated, useful commands added
- `docs/knowledge/architecture.md` — Section 2 updated (Kafka JARs + checkpoint), duplicate removed, file→container table updated, new section 10 (Spark Streaming → Kafka → Postgres)
- `docs/knowledge/index.md` — Updated descriptions for architecture.md, spark.md, kafka.md
- `docs/phases/phase-2.4-spark-streaming.md` — New phase completion doc
- `docs/phases/README.md` — Phase 2.4 marked Complete, Phase 2.5 added as next
- `chat-history/current-state.md` — Full handoff update

## Key decisions
- **4 Kafka JARs baked into image** — apache/spark:3.5.1 ships only core JARs
- **foreachBatch bridges streaming→JDBC** — JDBC has no native streaming sink
- **Checkpoint via named volume** — persists Kafka offsets across container restarts
- **Stale station filter at 1 hour** — 44% of stations are stale/dead
- **is_* cast int→boolean in Spark** — GBFS returns 0/1, not booleans
- **station_id stays string** — mixed UUID + numeric format
- **Kafka metadata columns** — partition, offset, timestamp for traceability

## Next session
Phase 2.5 — DBT models for stream: `stg_station_status` (staging view) + `fact_station_reads` (one row per station poll). Will enable querying "avg bikes available at station X over last hour."
