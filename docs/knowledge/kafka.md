# Kafka

### Useful Commands
```bash
# List topics
kafka-topics --list --bootstrap-server kafka:9092

# Consume from a topic (terminal)
kafka-console-consumer --bootstrap-server kafka:9092 --topic divvy_station_status --from-beginning

# Produce to a topic (terminal)
kafka-console-producer --bootstrap-server kafka:9092 --topic test

# Describe a topic
kafka-topics --describe --bootstrap-server kafka:9092 --topic divvy_station_status
```

### Key Concepts
- **Topic** — named stream/category (e.g., `divvy_station_status`)
- **Partition** — parallelism unit within a topic
- **Consumer Group** — group of consumers sharing partitions
- **Offset** — position within a partition; committed by consumer
- **Zookeeper** — coordination service (Kafka 3.x+ can run without it via KRaft, but learning ZK first is more educational)

---
