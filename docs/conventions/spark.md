# Spark Conventions

## Batch Jobs (`spark/jobs/*.py`)

### Structure
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder \
        .appName("crime-batch") \
        .getOrCreate()

    # 1. Read
    df = spark.read.parquet("/data/crime_2023.parquet")

    # 2. Clean
    df = clean(df)

    # 3. Write
    write_to_postgres(df, "raw.crime_events")

    spark.stop()

def clean(df):
    return (df
        .dropDuplicates(["id"])
        .withColumn("date", F.to_timestamp("date"))
        .withColumn("primary_type", F.upper(F.trim("primary_type")))
        .withColumn("community_area", F.col("community_area").cast("int"))
    )

def write_to_postgres(df, table):
    (df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/chicago_analytics")
        .option("dbtable", table)
        .option("user", "chicago")
        .option("password", "changeme")  # from env, not hardcoded
        .option("batchsize", 10000)
        .mode("overwrite")  # Phase 1 only — switch to upsert later
        .save())

if __name__ == "__main__":
    main()
```

### Rules
- **Always use the DataFrame API or SQL functions.** Never RDDs unless you have a specific reason.
- **One job = one `main()` function.** Read → clean → write. No monolithic scripts.
- **Credentials from environment variables, never hardcoded.** Use `os.environ["POSTGRES_PASSWORD"]`.
- **Use `dropDuplicates` early** — dedup before shuffles to reduce data volume.
- **Use `F.upper()` / `F.trim()` on string columns** from APIs — casing is inconsistent in real data.

## Partitioning

### When to Repartition
- **Before a JDBC write:** `df.repartition(8).write...` — controls parallelism into Postgres
- **Before a wide transformation (join/groupBy):** repartition by the join/group key to avoid skew
- **Before writing partitioned Parquet:** `df.repartition("year")` — one file per partition

### When NOT to Repartition
- After a narrow transformation (filter, map) — partitions are already fine
- If the dataset is small (<1GB) — default partitions are fine, don't over-engineer

### Key Config
```python
spark.conf.set("spark.sql.shuffle.partitions", 200)  # default is 200, tune for your data
spark.conf.set("spark.sql.adaptive.enabled", "true")  # AQE — let Spark coalesce partitions
```

## JDBC Writes

### Batch Size
```python
.option("batchsize", 10000)  # rows per batch insert. Default is 1000. Too low = slow.
```

### Mode
- **Phase 1:** `.mode("overwrite")` — simple, replaces the whole table each run
- **Phase 2+:** `.mode("append")` with an upsert pattern (see Idempotency below)

### Parallelism
```python
.option("numPartitions", 8)  # parallel JDBC connections
```
Must match the number of partitions in the DataFrame (use `repartition(8)` before write).

## Structured Streaming (`spark/jobs/divvy_stream.py`)

### Checkpointing
**Always set a checkpoint location.** Without it, the stream can't recover from failure.
```python
.option("checkpointLocation", "/checkpoint/divvy")
```

### foreachBatch for JDBC
JDBC has no native streaming sink. Use `foreachBatch`:
```python
def write_batch(df, batch_id):
    df.write.format("jdbc") \
        .option("url", "...") \
        .option("dbtable", "raw.station_status") \
        .mode("append") \
        .save()

stream.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/checkpoint/divvy") \
    .trigger(processingTime="60 seconds") \
    .start()
```

### Triggers
- `processingTime="60 seconds"` — micro-batch every 60s (good for Divvy's refresh rate)
- `once=True` — process available data and stop (useful for testing)

### Watermarks
Use a watermark to drop stale data and manage state size:
```python
.withWatermark("reported_at", "1 hour")
```

## Idempotency

**Every write must be safe to re-run.**

- **Batch (Phase 1):** `overwrite` mode is idempotent (replaces table)
- **Batch (Phase 2+):** Use upsert via a temp table + `MERGE INTO`:
  ```sql
  -- write to temp table first, then merge
  MERGE INTO raw.crime_events t
  USING temp.crime_events_new n
  ON t.id = n.id
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ...
  ```
- **Streaming:** `append` + deduplication in DBT staging (`dropDuplicates` or SQL `DISTINCT`)

## Memory Configuration

### Symptoms of OOM
- `java.lang.OutOfMemoryError: Java heap space`
- `Container killed by YARN for exceeding memory limits` (not applicable in local mode, but good to know)

### Fixes
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

- Start with defaults, increase only when OOM occurs
- `spark.sql.adaptive.enabled=true` lets Spark auto-tune partition counts (AQE)

## Common Mistakes to Expect

1. **OOM on full dataset** → reduce `shuffle.partitions`, increase `driver.memory`, or filter early
2. **JDBC write is slow** → increase `batchsize` (default 1000 → 10000), use `numPartitions`
3. **Stream dies silently** → add `awaitTermination()` and a query listener for errors
4. **Duplicate rows after re-run** → using `append` mode without deduplication
5. **`localhost` connection refused** → use Docker service name (`postgres`), not `localhost`
6. **JDBC driver not found** → JAR isn't in `/opt/spark/jars/`. Mount it in the Dockerfile.
7. **Schema mismatch from API** → Socrata returns strings for numeric fields. Cast in Spark, not in DBT.
