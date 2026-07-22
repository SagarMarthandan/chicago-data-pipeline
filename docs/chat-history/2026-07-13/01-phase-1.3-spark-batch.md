# 2026-07-13 — Phase 1.3: Spark Batch Job

## Topic
Spark batch ETL: read Parquet → clean → write to Postgres `raw.crime_events` via JDBC.

## What was done
- Created `spark/jobs/crime_batch.py` — reads `data/raw/crime/crime_2023.parquet` (263,393 rows), cleans (cast id to long, parse dates to timestamp, uppercase primary_type, cast community_area to int, dedup on id, drop null ids), writes to Postgres `raw.crime_events` via JDBC with `overwrite` mode. Includes built-in verification step (reads back from Postgres and compares row counts).
- Updated `docker-compose.yml` — added Postgres env vars (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`) to both `spark-master` and `spark-worker` services.

## Errors hit
1. `spark-submit` not on PATH in apache/spark container → use full path `/opt/spark/bin/spark-submit`
2. Duplicate `environment:` block for spark-worker from bad edit → cleaned up
3. `spark-worker:` service key dropped during edit → re-inserted
4. `raw.crime_events` table missing after WSL restart → re-ran batch job (idempotent via `overwrite` mode)

## Verification
- 263,393 rows read from Parquet, 0 dropped, 263,393 written to Postgres
- Row count match verified: Spark 263,393 == Postgres 263,393
- Column types verified via `information_schema.columns`

## Key decisions
- `mode("overwrite")` for Phase 1 idempotency — replaces whole table each run
- JDBC batchsize=10000, numPartitions=8 for parallel writes
- Credentials from environment variables, never hardcoded
- Keep null lat/long as-is (don't drop — too many rows)
