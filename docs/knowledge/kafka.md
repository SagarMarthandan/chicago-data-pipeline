# Kafka

### Our Setup (Phase 2.2)

| Component | Image | Version | Purpose |
|---|---|---|---|
| Zookeeper | `confluentinc/cp-zookeeper:7.6.0` | Confluent 7.6.0 | Kafka coordination (broker metadata, leader election) |
| Kafka | `confluentinc/cp-kafka:7.6.0` | Confluent 7.6.0 | Broker — producers write, consumers read |

- **Topic:** `divvy_station_status` (3 partitions, replication factor 1)
- **Auto-create topics:** enabled (dev convenience — producer creates topic on first message)
- **Listeners:**
  - `PLAINTEXT://kafka:9092` — internal Docker network (Spark, producer)
  - `PLAINTEXT_HOST://localhost:29092` — host machine (console consumer testing)
- **Zookeeper port:** 2181 (internal only, not exposed to host)
- **Data volumes:** `kafka_data`, `zookeeper_data`, `zookeeper_log` (named volumes, persist across restarts)

### Why Confluent images (not Bitnami)?
Bitnami moved behind a commercial subscription in 2026. Confluent Platform images (`confluentinc/cp-*`) are free, stable, and production-hardened. Version 7.6.0 is pinned (not `latest`) for reproducibility.

### Why Zookeeper (not KRaft)?
Kafka 3.x+ can run without Zookeeper using KRaft mode. But learning Zookeeper first is more educational — most existing Kafka deployments still use it, and understanding ZK helps when debugging broker coordination issues.

### Single-broker overrides
Three Kafka settings MUST be overridden for a single-broker setup (defaults assume 3 brokers):
```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1          # default 3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1   # default 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1              # default 2
```
Without these, Kafka fails to create internal topics (__consumer_offsets, __transaction_state) and consumers can't commit offsets.

### Useful Commands
```bash
# All commands run inside the kafka container:
# docker compose exec kafka <command>

# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Create a topic (3 partitions, replication factor 1)
kafka-topics --create --topic divvy_station_status \
  --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Describe a topic (see partitions, leaders, replicas)
kafka-topics --describe --topic divvy_station_status \
  --bootstrap-server localhost:9092

# Delete a topic
kafka-topics --delete --topic divvy_station_status \
  --bootstrap-server localhost:9092

# Consume from a topic (terminal, from beginning)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic divvy_station_status --from-beginning

# Consume with a max message count (useful for testing)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic divvy_station_status --from-beginning --max-messages 5

# Produce to a topic (terminal, type messages + Ctrl-D)
kafka-console-producer --bootstrap-server localhost:9092 \
  --topic divvy_station_status

# Pipe a single JSON message (non-interactive)
echo '{"key":"value"}' | kafka-console-producer \
  --bootstrap-server localhost:9092 --topic divvy_station_status
```

### Key Concepts
- **Topic** — named stream/category (e.g., `divvy_station_status`)
- **Partition** — parallelism unit within a topic. Messages within a partition are ordered. We use 3 partitions — station_id as message key ensures same station goes to same partition (ordered processing per station)
- **Consumer Group** — group of consumers sharing partitions. Spark Structured Streaming uses its own consumer group
- **Offset** — position within a partition; committed by consumer. Checkpointed by Spark in `/checkpoint/divvy`
- **Zookeeper** — coordination service (manages broker metadata, leader election). Kafka 3.x+ can run without it via KRaft, but learning ZK first is more educational
- **Replication factor** — how many copies of each partition. 1 for single-broker (no redundancy). Production: 3
- **Auto-create topics** — if enabled, Kafka creates a topic when the first message arrives. Convenient for dev; in prod, create topics explicitly to control partitions/replication

---
