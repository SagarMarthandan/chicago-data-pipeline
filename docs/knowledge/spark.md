# Spark

### Docker Image: apache/spark (not bitnami)
We use the official `apache/spark:3.5.1` image. Bitnami moved their images behind a commercial subscription in 2026 — `docker.io/bitnami/*` is no longer free.

| Concept | bitnami/spark (old) | apache/spark (current) |
|---|---|---|
| Start master | `SPARK_MODE=master` env var | `spark-class org.apache.spark.deploy.master.Master` command |
| Start worker | `SPARK_MODE=worker` + `SPARK_MASTER_URL` env vars | `spark-class org.apache.spark.deploy.worker.Worker spark://master:7077` command |
| SPARK_HOME | `/opt/bitnami/spark` | `/opt/spark` |
| JDBC jar path | `/opt/bitnami/spark/jars/` | `/opt/spark/jars/` |
| Non-root user | UID 1001 | `spark` (UID 185) |

`SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` env vars still work in the official image — `spark-class` reads them.

`SPARK_MASTER_HOST=spark-master` is needed in Docker Compose so the master advertises the Docker service name (not a random container hostname) to workers.

### Spark Master Healthcheck in Docker
Spark master binds ports differently:
- **RPC port 7077** → binds to the container's Docker network IP (e.g., `172.18.0.2`), NOT `127.0.0.1`
- **Web UI port 8080** → binds to `0.0.0.0` (all interfaces)

Healthchecks run inside the container. Checking port 7077 on `127.0.0.1` fails because Spark isn't listening there. Always check the Web UI port (8080 inside the container, remapped to 8180 on the host):
```yaml
healthcheck:
  test: ["CMD-SHELL", "python3 -c \"import socket; s=socket.socket(); s.settimeout(2); s.connect(('127.0.0.1', 8080)); s.close()\""]
```

### Useful Commands

> **IMPORTANT:** In the `apache/spark` container, `spark-submit` is NOT on `$PATH`.
> Always use the full path: `/opt/spark/bin/spark-submit`

```bash
# Submit a batch job (inside spark-master container)
/opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/crime_batch.py

# From the host via docker compose exec
docker compose exec spark-master /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/crime_batch.py

# Submit with JDBC dependency (not needed — driver is baked into our image)
spark-submit --packages org.postgresql:postgresql:42.7.3 jobs/crime_batch.py

# Spark shell (PySpark)
/opt/spark/bin/pyspark --master local[*]
```

### JDBC Connection to Postgres
```python
(df.write
  .format("jdbc")
  .option("url", "jdbc:postgresql://postgres:5432/chicago_analytics")
  .option("dbtable", "raw.crime_events")
  .option("user", "chicago")
  .option("password", "changeme")
  .mode("overwrite")
  .save())
```

### Structured Streaming + Kafka
```python
stream = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka:9092")
  .option("subscribe", "divvy_station_status")
  .load())
```

### foreachBatch (streaming → JDBC bridge)
JDBC doesn't have a native streaming sink. Use `foreachBatch` to write each micro-batch:
```python
(df.writeStream
  .foreachBatch(lambda df, epoch: df.write.format("jdbc").option(...).save())
  .start())
```

---
