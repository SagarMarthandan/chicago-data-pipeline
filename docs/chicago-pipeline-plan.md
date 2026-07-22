# Chicago Crime + Divvy — Data Engineering Learning Project

## The Question That Drives Everything

> **Does crime near a Divvy bike-share station affect ridership?**

Every component in this pipeline exists to answer (or enable answering) that question. When you're unsure whether something is in scope, ask: *does this help answer the question?*

---

## Architecture Overview

```
                         ┌─────────────────────────────────────────────┐
                         │               SOURCES                        │
                         │                                              │
                         │  Chicago Crime (batch)    Divvy (stream)     │
                         │  Socrata API / CSV        GBFS REST API       │
                         │  ~8M rows, daily drop     ~60s refresh        │
                         └──────────┬────────────────────┬──────────────┘
                                    │                    │
                                    │                    ▼
                                    │              ┌──────────┐
                                    │              │  Kafka   │
                                    │              │  topic   │
                                    │              └────┬─────┘
                                    │                   │
                                    ▼                   ▼
                         ┌──────────────┐    ┌────────────────────┐
                         │    Spark     │    │  Spark Structured  │
                         │  (batch ETL) │    │    Streaming       │
                         └──────┬───────┘    └─────────┬──────────┘
                                │                      │
                                ▼                      ▼
                         ┌─────────────────────────────────────────┐
                         │            Postgres (raw)               │
                         │   raw.crime_events  raw.station_status  │
                         └─────────────────────┬───────────────────┘
                                               │
                                               ▼
                         ┌─────────────────────────────────────────┐
                         │              DBT (transform)            │
                         │  staging → intermediate → marts         │
                         └─────────────────────┬───────────────────┘
                                               │
                                               ▼
                         ┌─────────────────────────────────────────┐
                         │         Postgres (marts)                │
                         │  fact_crime_events, fact_station_reads, │
                         │  dim_community_area, dim_date, ...      │
                         └─────────────────────┬───────────────────┘
                                               │
                            ┌──────────────────┼──────────────────┐
                            ▼                  ▼                  ▼
                       ┌─────────┐      ┌────────────┐     ┌───────────┐
                       │ Grafana │      │  Analysis  │     │  Airflow  │
                       │   dash  │      │  / BI / ML │     │  monitors │
                       └─────────┘      └────────────┘     └───────────┘
```

**Orchestration:** Airflow sits above the batch path, triggering each step.
**Phase 4 (cloud):** Postgres → BigQuery/Snowflake, ingestion → Airbyte, infra → Terraform.

---

## Repo Structure

```
chicago-data-pipeline/
├── docker-compose.yml          # all services
├── .env.example                # connection strings, API keys
├── Makefile                    # convenience commands
│
├── ingestion/                  # Phase 1: pull raw data
│   ├── download_crime.py       # Socrata API → local CSV/parquet
│   └── requirements.txt
│
├── spark/
│   ├── Dockerfile
│   ├── jobs/
│   │   ├── crime_batch.py      # Phase 1: CSV → Postgres raw
│   │   └── divvy_stream.py     # Phase 2: Kafka → Postgres raw
│   └── requirements.txt
│
├── kafka/
│   ├── Dockerfile
│   └── producers/
│       └── divvy_producer.py   # Phase 2: GBFS API → Kafka topic
│
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   ├── crime_batch_dag.py  # Phase 1
│   │   └── divvy_stream_dag.py # Phase 2 (start/stop producer)
│   └── requirements.txt
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            # Postgres connection
│   ├── macros/
│   │   └── try_cast.sql        # cross-warehouse safe-cast macro
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_crime_events.sql
│   │   │   ├── stg_station_status.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   ├── int_crime_by_community_area.sql
│   │   │   └── int_station_activity_by_hour.sql
│   │   └── marts/
│   │       ├── fact_crime_events.sql
│   │       ├── fact_station_reads.sql
│   │       ├── dim_date.sql
│   │       ├── dim_community_area.sql
│   │       ├── dim_crime_type.sql
│   │       └── schema.yml
│   ├── tests/
│   │   └── assert_crime_in_chicago_bounds.sql
│   └── seeds/
│       └── community_areas.csv  # static reference data
│
├── grafana/
│   ├── dashboards/
│   │   ├── pipeline_health.json
│   │   └── crime_divvy_analysis.json
│   └── provisioning/
│
└── terraform/                  # Phase 4 only
    ├── main.tf
    ├── variables.tf
    └── modules/
        ├── bigquery/
        └── gcs/
```

---

## Phase 1 — Batch Foundation

**Goal:** A reproducible `docker compose up` that loads Chicago crime data into Postgres and builds a DBT mart. No cloud, no streaming.

### 1.1 Docker Compose services

```yaml
# docker-compose.yml (Phase 1 scope)
services:
  postgres:        # warehouse (raw + marts)
  airflow-webserver:
  airflow-scheduler:
  spark-master:
  spark-worker:
  # Zookeeper + Kafka added in Phase 2
  # Grafana added in Phase 3
```

**Postgres init:** Create two schemas on startup — `raw` (landing zone, raw ingested data) and `mart` (DBT output). Use an init script mounted into `/docker-entrypoint-initdb.d/`.

```sql
-- init.sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;
```

### 1.2 Ingestion script (`ingestion/download_crime.py`)

- **Dataset:** "Crimes - 2001 to Present" on Chicago Data Portal
- **API:** Socrata API — `https://data.cityofchicago.org/resource/ijzp-q4t2.json`
- **App token:** Register a free account at data.cityofchicago.org to get an app token (raises rate limits)
- **Size:** ~8M rows. For learning, start with a single year (e.g. `?$where=year=2023`) → ~80K rows. Scale up later.
- **Update cadence:** Daily, ~1 day lag. This matters for incremental loading later.

**Key columns:**

| Column | Type | Notes |
|---|---|---|
| `id` | int | unique case ID — primary key |
| `case_number` | string | agency-specific, not unique |
| `date` | timestamp | occurrence date, string in API |
| `primary_type` | string | e.g. THEFT, BATTERY, NARCOTICS |
| `description` | string | sub-category |
| `location_description` | string | e.g. STREET, APARTMENT |
| `arrest` | bool | whether arrest was made |
| `domestic` | bool | domestic-related |
| `community_area` | int | FK to community area (0 = not assigned) |
| `district` | int | police district |
| `ward` | int | aldermanic ward |
| `latitude` / `longitude` | float | can be null |
| `year` | int | partition column |

**Ingestion approach:**

- Uses `requests` + Socrata API with app token
- Paginates with `$limit=50000&$offset=...`
- Writes to local Parquet (not CSV — Parquet preserves types, is columnar, Spark-friendly)
- **First mistake to make and learn from:** try loading the full 8M rows immediately. Watch it be slow. Then learn about partitioning, predicate pushdown, and incremental loads. This is the lesson.

### 1.3 Spark batch job (`spark/jobs/crime_batch.py`)

```
Read Parquet → clean → write to Postgres raw.crime_events
```

**Cleaning steps (this is where the learning is):**
1. Parse `date` string → timestamp (Socrata returns ISO 8601 strings)
2. Drop rows where `id` is null (data quality)
3. Normalize `primary_type` casing (THEFT vs theft vs Theft — yes, this happens)
4. Handle null lat/long (don't drop — too many; keep as null, flag)
5. Cast `community_area` from string to int (API returns it as string)
6. Write to `raw.crime_events` using JDBC

**Spark-Postgres connection:** Use the `postgresql` JDBC driver. Mount the JAR into the Spark container. This will be your first Docker volume mount headache — embrace it.

### 1.4 DBT models

**Staging (`stg_crime_events.sql`):** Light cleaning on top of raw. Rename columns to snake_case, cast types, deduplicate on `id`.

**The `try_cast` macro (`macros/try_cast.sql`):** Postgres has no `TRY_CAST` (that's Snowflake/DuckDB syntax), and BigQuery uses `SAFE_CAST`. This macro dispatches per-warehouse so your models stay portable across Phase 1 (Postgres) and Phase 4 (BigQuery).

```sql
-- macros/try_cast.sql
{% macro try_cast(column, target_type) %}
  {% if adapter.type() == 'postgres' %}
    {{ column }}::{{ target_type }}
  {% elif adapter.type() == 'bigquery' %}
    SAFE_CAST({{ column }} AS {{ target_type }})
  {% else %}
    {{ column }}::{{ target_type }}
  {% endif %}
{% endmacro %}
```

> **Design note:** the Postgres branch does a *plain* cast, not a guarded one. This is deliberate — if bad data reaches DBT, you want the model to fail loudly so you catch it at the source (Spark/Kafka should have cleaned it upstream). A silent null hides upstream bugs. BigQuery's `SAFE_CAST` is used because BigQuery doesn't raise on cast failure — it returns null — so the two branches converge on the same "null on bad input" behavior, just via different mechanisms.


```sql
-- stg_crime_events.sql
SELECT
    id::bigint          AS crime_id,
    case_number,
    {{ try_cast('date', 'timestamp') }} AS occurred_at,
    primary_type::varchar,
    description::varchar,
    location_description::varchar,
    arrest::boolean,
    domestic::boolean,
    {{ try_cast('community_area', 'int') }} AS community_area_id,
    {{ try_cast('district', 'int') }}       AS district_id,
    {{ try_cast('ward', 'int') }}           AS ward_id,
    latitude::double precision,
    longitude::double precision,
    year::int
FROM {{ source('raw', 'crime_events') }}
```

**Marts:**

- `dim_date` — standard date dimension from min to max crime date
- `dim_community_area` — seed from `seeds/community_areas.csv` (Chicago has 77 community areas; this is static reference data you'll grab from the portal)
- `dim_crime_type` — distinct `primary_type` + `description` combinations
- `fact_crime_events` — the main fact table, joined to dims

```sql
-- fact_crime_events.sql
SELECT
    c.crime_id,
    c.occurred_at,
    c.occurred_at::date AS date_key,
    c.community_area_id,
    c.primary_type || '|' || c.description AS crime_type_key,
    c.arrest,
    c.domestic,
    c.latitude,
    c.longitude
FROM {{ ref('stg_crime_events') }} c
WHERE c.crime_id IS NOT NULL
```

**DBT tests (`schema.yml`):**
```yaml
models:
  - name: fact_crime_events
    columns:
      - name: crime_id
        tests: [unique, not_null]
      - name: community_area_id
        tests:
          - relationships:
              to: ref('dim_community_area')
              field: community_area_id
              # note: 0 is a valid "unassigned" value — handle in model
```

### 1.5 Airflow DAG (`crime_batch_dag.py`)

```
download_crime (BashOperator/PythonOperator)
    → spark_crime_batch (SparkSubmitOperator or DockerOperator)
    → dbt_run (BashOperator: dbt run --select staging marts)
    → dbt_test (BashOperator: dbt test)
```

- Schedule: `@daily` (but start with `@manual` while debugging)
- Use Airflow's `DockerOperator` to run the Spark job in the spark container, OR `SparkSubmitOperator` pointing at spark-master. Pick one — learn the tradeoff.

### 1.6 Phase 1 deliverable & verification

**Done when:**
- `docker compose up` starts Postgres + Airflow + Spark
- Triggering the DAG downloads crime data, runs Spark, runs DBT, runs tests — all green
- `SELECT COUNT(*) FROM mart.fact_crime_events` returns rows
- `dbt test` passes
- You can run a query like "top 5 community areas by crime count in 2023" and get a real answer

**Learning checkpoints (mistakes you should make):**
- [ ] Spark OOMs on full dataset → learn about `spark.sql.shuffle.partitions`, driver vs executor memory
- [ ] JDBC write is slow → learn about `repartition`, batch size, `batchsize` JDBC param
- [ ] DBT model fails on type mismatch → learn why Postgres has no `TRY_CAST` and how the `try_cast` macro dispatches per-warehouse
- [ ] Airflow task can't reach Postgres → learn Docker networking, service names as hostnames
- [ ] Re-running the DAG duplicates rows → learn idempotency, `merge`/upsert patterns

---

## Phase 2 — Add the Stream

**Goal:** Live Divvy station data flows through Kafka into Postgres via Spark Structured Streaming.

### 2.1 Data source — Divvy GBFS

Divvy publishes a **GBFS (General Bikeshare Feed Specification)** feed — a real, live, REST API. This is genuine streaming, not faked.

- **Base URL:** `https://gbfs.divvybikes.com/gbfs/gbfs.json` (discovery endpoint listing all feeds)
- **station_status:** `https://gbfs.divvybikes.com/gbfs/en/station_status.json`
  - Returns array of stations with: `station_id`, `num_bikes_available`, `num_docks_available`, `is_renting`, `is_returning`, `last_reported` (unix timestamp)
- **station_information:** `https://gbfs.divvybikes.com/gbfs/en/station_information.json`
  - Static-ish: `station_id`, `name`, `lat`, `lon`, `capacity`, `region_id`
- **Refresh:** ~60 seconds

### 2.2 New Docker services

```yaml
# added to docker-compose.yml
zookeeper:
  image: confluentinc/cp-zookeeper:latest
kafka:
  image: confluentinc/cp-kafka:latest
  # use KRaft mode (no Zookeeper) if you want to simplify — but
  # learning Zookeeper first is more educational
```

### 2.3 Kafka producer (`kafka/producers/divvy_producer.py`)

- Polls `station_status.json` every 60 seconds
- For each station in the response, publishes a JSON message to topic `divvy_station_status`
- Message key: `station_id` (ensures same station goes to same partition — important for ordered processing)
- Message value: full station status JSON

```python
# skeleton
while True:
    resp = requests.get(STATION_STATUS_URL)
    stations = resp.json()["data"]["stations"]
    for station in stations:
        producer.send(
            "divvy_station_status",
            key=str(station["station_id"]).encode(),
            value=json.dumps(station).encode("utf-8"),
        )
    time.sleep(60)
```

### 2.4 Spark Structured Streaming (`spark/jobs/divvy_stream.py`)

```python
# read from Kafka
stream = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "divvy_station_status")
    .option("startingOffsets", "latest")
    .load())

# parse JSON value
parsed = (stream
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .select(
        from_json("json_str", schema).alias("data"),
        col("kafka_ts"),
    )
    .select("data.*", "kafka_ts"))

# write to Postgres — use foreachBatch for JDBC sinks
(parsed
    .writeStream
    .foreachBatch(lambda df, batch_id: write_to_postgres(df, batch_id))
    .option("checkpointLocation", "/checkpoint/divvy")
    .trigger(processingTime="60 seconds")
    .start())
```

**Key lesson:** `foreachBatch` is the bridge between Structured Streaming and JDBC (which doesn't have a native streaming sink). Learn why.

### 2.5 DBT models for stream data

```sql
-- stg_station_status.sql
SELECT
    station_id::bigint,
    num_bikes_available::int,
    num_docks_available::int,
    is_renting::boolean,
    is_returning::boolean,
    {{ try_cast('last_reported', 'bigint') }} AS last_reported_epoch,
    {{ try_cast('last_reported', 'timestamp') }} AS reported_at,
    kafka_ts AS ingested_at
FROM {{ source('raw', 'station_status') }}
```

```sql
-- fact_station_reads.sql
-- one row per station poll — the "fact" of a station reading
SELECT
    station_id,
    reported_at,
    reported_at::date AS date_key,
    num_bikes_available,
    num_docks_available,
    is_renting,
    is_returning,
    ingested_at
FROM {{ ref('stg_station_status') }}
WHERE station_id IS NOT NULL
```

### 2.6 Airflow DAG for the stream

The stream runs continuously, so Airflow's role is different here:
- A DAG that **starts** the producer + streaming job (DockerOperator or KubernetesPodOperator)
- A DAG that **monitors** — checks for lag, checks last ingested timestamp, alerts if stale
- Learn the pattern: Airflow orchestrates batch; long-running streams are managed separately

### 2.7 Phase 2 deliverable & verification

**Done when:**
- `docker compose up` includes Kafka + Zookeeper
- Producer is running and `kafka-console-consumer` shows messages arriving
- Spark streaming job writes to `raw.station_status`
- `SELECT COUNT(*) FROM raw.station_status` grows over time
- DBT builds `fact_station_reads` from the stream
- You can query "average bikes available at station X over the last hour"

**Learning checkpoints:**
- [ ] Kafka topic has no messages → learn `auto.create.topics.enable`, producer acks
- [ ] Spark streaming job won't start → learn checkpointing, watermarks
- [ ] Duplicate rows in Postgres → learn idempotent sinks, upsert patterns
- [ ] Stream dies silently → learnStructured Streaming query listeners, `awaitTermination`
- [ ] Backpressure / lag → learn Kafka consumer group settings, max offsets per trigger

---

## Phase 3 — Observability & Data Quality

**Goal:** The pipeline fails loudly and visibly. You can see it working (or breaking) in Grafana.

### 3.1 Grafana

- Connect Grafana to Postgres as a data source
- **Pipeline health dashboard:**
  - Row count over time (raw vs mart) — detects load failures
  - Latest `ingested_at` timestamp on stream data — detects stale stream
  - DBT test pass/fail counts
  - Airflow DAG run status (via Airflow's statsd → Grafana, or a simple Postgres table Airflow writes to)
- **Analysis dashboard:**
  - Crime count by community area (map visualization — Grafana has geospatial panels)
  - Station availability heatmap (station × hour)
  - The actual crime-vs-ridership question (once both sources are in)

### 3.2 DBT tests (expand)

Add custom singular tests:
```sql
-- tests/assert_crime_in_chicago_bounds.sql
SELECT *
FROM {{ ref('fact_crime_events') }}
WHERE latitude IS NOT NULL
  AND (latitude NOT BETWEEN 41.6 AND 42.1
       OR longitude NOT BETWEEN -87.9 AND -87.5)
```

Add tests on the stream:
```yaml
- name: fact_station_reads
  columns:
    - name: station_id
      tests: [not_null]
    - name: reported_at
      tests: [not_null]
```

### 3.3 Airflow robustness

- Add `retries=3`, `retry_delay=timedelta(minutes=5)` to tasks
- Add SLAs: `sla=timedelta(hours=2)` on the dbt_run task
- Add a sensor that checks `raw.station_status` freshness before downstream tasks
- Learn: `on_failure_callback`, email/Slack alerts (even if just logging)

### 3.4 Phase 3 deliverable & verification

**Done when:**
- Grafana shows live row counts and stream freshness
- Breaking the pipeline (stop the producer) shows up as a Grafana alert within minutes
- DBT tests catch a deliberately introduced data quality issue
- Airflow retries a deliberately failing task and alerts on SLA miss

---

## Phase 4 — Go Cloud

**Goal:** Same pipeline, now on cloud infrastructure provisioned with Terraform, with Airbyte replacing hand-rolled ingestion. **Then answer the driving question** — does crime near a Divvy station affect ridership? — using full historical data (years of overlap) + BigQuery performance tuning (partitioning/clustering) + correlation analysis + (stretch) BigQuery ML.

### 4.1 Choose a warehouse

| Option | Cost | Learning value | Notes |
|---|---|---|---|
| **BigQuery** | $0 (free tier is generous) | High | Serverless, scales automatically, DBT support is excellent. Best for learning. |
| **Snowflake** | ~$25-40/mo | High | Industry standard. Free trial. More moving parts (warehouses, credits). |
| **Redshift** | ~$0.25/hr | Medium | Requires provisioning. Less elegant for small data. |

**Recommendation: BigQuery.** Free tier covers this project entirely, DBT integration is first-class, and serverless means no warehouse management.

### 4.2 Terraform

```hcl
# terraform/main.tf (sketch)
provider "google" { project = var.project_id }

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = "US"
}

resource "google_bigquery_dataset" "mart" {
  dataset_id = "mart"
  location   = "US"
}

resource "google_storage_bucket" "data_lake" {
  name     = "${var.project_id}-data-lake"
  location = "US"
}
```

**Lesson:** Terraform state. Learn why `terraform destroy` is satisfying and terrifying. Use a local backend first, then migrate to GCS backend.

### 4.3 Architecture change

```
Spark → GCS (Parquet, data lake) → BigQuery (raw) → DBT → BigQuery (mart)
                                              ↑
                                          Airbyte (ingestion from Socrata API directly)
```

- Spark writes to GCS instead of Postgres
- Airbyte replaces `download_crime.py` — configure a Socrata source → BigQuery destination
- DBT `profiles.yml` switches from Postgres to BigQuery
- The streaming path can stay on Postgres (or move to BigQuery via `foreachBatch` to GCS → BigQuery load job)

### 4.4 Phase 4 deliverable & verification

**Done when:**
- `terraform apply` creates BigQuery datasets + GCS bucket
- `terraform destroy` cleans them up
- Airbyte ingests crime data into BigQuery
- DBT runs against BigQuery and produces the same marts
- Grafana connects to BigQuery (or you keep a Postgres mirror for dashboards)

---

### 4.5 Partitioning & clustering (performance tuning)

**Why:** On local Postgres (263K rows), partitioning was irrelevant — full scan = 100ms. On BigQuery (8M+ crime rows, 50M+ Divvy trips), partition pruning + clustering reduce bytes scanned and query cost. This is the "performance tuning that matters" bullet.

**Implementation:**
- DBT config on `fact_crime_events`: `materialized='table'`, `partition_by={"field":"date","data_type":"date"}`, `cluster_by=["community_area","primary_type"]`
- DBT config on Divvy trip fact: `partition_by={"field":"started_at","data_type":"timestamp"}`, `cluster_by=["start_station_id"]`
- Partitioning requires `materialized='table'` (not `view`) — BigQuery only partitions physical tables
- Clustering columns: low-cardinality columns you filter/group by frequently

**Lesson:** Partition pruning. Run the same query with and without a partition filter (`WHERE date BETWEEN ...`). Compare bytes scanned via `EXPLAIN` or query history in BigQuery console. Show real cost reduction. Over-partitioning (too many small partitions) hurts — learn the tradeoff.

### 4.6 Ingest full history (the data that answers the question)

**Why:** The driving question was unanswerable in Phases 1-3 because crime data was 2023 only and Divvy was 2026 live — no temporal overlap. Full history gives 5+ years of overlap.

**Data sources (verified 2026-07-20):**
- **Crime history:** `bigquery-public-data.chicago_crime` — official BigQuery public dataset, Chicago PD crime incidents 2001-present (~8M rows). Reference directly in DBT as an external source — no ingestion needed. This replaces the 2023-only Socrata extract from Phase 1.
- **Divvy trip history:** NOT in BigQuery public datasets (verified — `bigquery-public-data.chicago_divvy_trips` does not exist). Raw data is monthly CSVs on AWS S3: `https://divvy-tripdata.s3.amazonaws.com/index.html` (2013-present, ~50M rows total). Must be ingested into BigQuery.

**Implementation:**
- **Crime (2001-present, ~8M rows):** Reference `bigquery-public-data.chicago_crime` directly in DBT (`sources.yml`). No ingestion cost. Filter to 2018-present in staging to keep marts focused.
- **Divvy trips (2013-present, ~50M rows):** Two paths — pick based on time/scope:
  - **Path A (recommended): Airbyte S3 source → BigQuery.** Configure Airbyte S3 connector pointing at `divvy-tripdata.s3.amazonaws.com`, incremental load by month. Shows Airbyte skill on a non-trivial source (S3 CSV → BigQuery). Slower to set up but demonstrates real ingestion engineering.
  - **Path B (faster fallback): `bq load` from S3 via a script.** Google Cloud Storage Transfer Service or a one-time `gsutil cp` + `bq load` script. Less Airbyte skill shown, faster to insight.
  - **Recommendation:** Path A for a 1-month sample (shows the skill), then `bq load` for the full history if Airbyte proves too slow for 50M rows. Pragmatic hybrid.

**Lesson:** Verify data sources before committing to a plan. `bigquery-public-data` has Chicago crime and taxi trips but NOT Divvy — a common assumption that's wrong. Real-world DE: don't assume a dataset exists; check the catalog. When data lives on S3 as files, ingestion is your job — there's no shortcut.

### 4.7 Analytics mart — answer the driving question

**Why:** This is the project's payoff. Every phase before this was plumbing. This phase produces an actual finding.

**Implementation:**
- New fact table: `mart.fact_station_day` — one row per station per day:
  - `station_id`, `date`, `trip_count` (from Divvy trips), `crime_count_within_quarter_mile` (from crime events)
- Geospatial join in BigQuery:
  ```sql
  ST_DISTANCE(
    ST_GEOGPOINT(station_lon, station_lat),
    ST_GEOGPOINT(crime_lon, crime_lat)
  ) <= 402  -- 0.25 mile in meters
  ```
- Time window: crimes on same day or prior 24h, within 0.25 mile of station
- New mart: `mart.crime_ridership_correlation` — `CORR(trip_count, crime_count_within_quarter_mile)` across station-days, grouped by station / month / community_area
- Grafana panel: scatter plot (trip_count vs crime_count), correlation coefficient gauge per station
- Write a "Findings" section in README with the actual correlation coefficient + interpretation

**Lesson:** Geospatial joins are expensive without partitioning/clustering — this is why 4.5 matters. A null result ("no correlation at station-day grain") is still a valid finding. The question being answered is the resume differentiator.

### 4.8 BigQuery ML (stretch goal — only if 4.5-4.7 go smoothly)

**Why:** "I trained a model" is a stronger resume bullet than "I ran a correlation." BigQuery ML is pure SQL — fits the DE story, no Python/ML engineering needed.

**Implementation:**
```sql
CREATE OR REPLACE MODEL mart.crime_ridership_model
OPTIONS(model_type='linear_reg', input_label_cols=['trip_count']) AS
SELECT
  trip_count,
  crime_count_within_quarter_mile,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
  EXTRACT(MONTH FROM date) AS month,
  station_id,
  community_area
FROM mart.fact_station_day
WHERE date BETWEEN '2018-01-01' AND '2023-12-31';
```
- `ML.EVALUATE` — R², MAE, RMSE
- `ML.WEIGHTS` — feature importance (does crime_count have a significant weight?)
- `ML.PREDICT` — predicted ridership given crime count

**Lesson:** BigQuery ML trades flexibility for convenience. Fine for linear regression on structured features. Not for anything complex. Know its limits.

### 4.9 Phase 4 deliverable & verification

**Done when:**
- `terraform apply` creates BigQuery datasets + GCS bucket
- `terraform destroy` cleans them up
- Crime history sourced from `bigquery-public-data.chicago_crime` (referenced in DBT, no ingestion) OR Airbyte Socrata source → BigQuery
- Divvy trip history ingested into BigQuery (Airbyte S3 source for sample, `bq load` for full history)
- DBT runs against BigQuery and produces partitioned marts
- Partition pruning verified: query with filter scans fewer bytes than without
- `fact_station_day` mart built with geospatial crime-to-station join
- `crime_ridership_correlation` mart produces a correlation coefficient
- README "Findings" section states the correlation + interpretation
- Grafana shows scatter plot + correlation gauge (or BigQuery console screenshots)
- (Stretch) BigQuery ML model trained, `ML.EVALUATE` run, weights interpreted
- **The driving question is answered.**

---

## Phase 5 — GitHub CI/CD Integration

**Goal:** Every change to the pipeline goes through a pull request, runs automated checks, and gets versioned before it lands on `prod`. No more direct commits to production.

### 5.1 Branch strategy

```
feature/* ──PR──▶ dev ──PR──▶ prod
                   │
                   └── CI builds + tests on every push

prod ──tag──▶ v1.0.0, v1.1.0, v1.2.0 (semantic versioning)
```

| Branch | Purpose | Who commits | Rules |
|---|---|---|---|
| `prod` | Production. What's deployed. Stable. | Merges only (no direct push) | Requires PR from `dev`, all CI checks pass, 1 review |
| `dev` | Development integration. Where features land. | Merges from feature branches | Requires PR, CI checks pass |
| `feature/*` | Short-lived work. One per task/phase. | You | Branch off `dev`, PR back to `dev` |

**Transition from current state:** Your `main` branch becomes `prod`. Create `dev` from `prod`. All future work branches off `dev`.

```bash
# One-time setup
git branch -m main prod          # rename main → prod
git push origin prod             # push prod
git checkout -b dev prod         # create dev from prod
git push origin dev              # push dev
# Set prod as default branch in GitHub Settings → Branches
```

### 5.2 Versioning

**Semantic versioning:** `v{MAJOR}.{MINOR}.{PATCH}`

| Bump | When | Example |
|---|---|---|
| MAJOR | Breaking change (schema redesign, tool swap) | `v1.0.0` → `v2.0.0` |
| MINOR | New feature (new DAG, new DBT model, new dashboard) | `v1.0.0` → `v1.1.0` |
| PATCH | Bug fix, doc update, config tweak | `v1.1.0` → `v1.1.1` |

**How versions are created:**
1. PR merged from `dev` → `prod`
2. GitHub Actions workflow tags the merge commit with the next version
3. GitHub Release is created with auto-generated changelog

**Your current v1–v17 informal tags become `v1.0.0` baseline.** First prod merge after Phase 5 setup = `v1.1.0` (Phase 3 observability).

### 5.3 GitHub Actions workflows

Three workflows, each triggered by a different event:

#### Workflow 1: CI checks (on every PR)

```yaml
# .github/workflows/ci.yml
# Triggers: pull_request to dev or prod
name: CI Checks
on:
  pull_request:
    branches: [dev, prod]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install ruff
      - run: ruff check airflow/ spark/ kafka/ ingestion/

  validate-dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install dbt-postgres
      - working-directory: dbt
        run: dbt parse --profiles-dir .

  validate-compose:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose config -q   # validate YAML syntax

  build-images:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose build airflow-init spark-master dbt-build
```

**What this catches:**
- Python syntax errors, unused imports, style violations (ruff)
- DBT model parse errors, broken refs, invalid Jinja (dbt parse)
- docker-compose.yml syntax errors (compose config)
- Dockerfile build failures (compose build)

#### Workflow 2: Build & push images (on merge to dev)

```yaml
# .github/workflows/build.yml
# Triggers: push to dev
name: Build & Push
on:
  push:
    branches: [dev]

jobs:
  push-images:
    runs-on: ubuntu-latest
    permissions:
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - run: |
          docker compose build \
            --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
            --build-arg GIT_SHA=${{ github.sha }} \
            --build-arg VERSION=dev-${{ github.sha }} \
            airflow-init spark-master dbt-build
      - run: |
          docker tag chicago-data-pipeline-airflow-init ghcr.io/${{ github.repository }}/airflow:dev
          docker tag chicago-data-pipeline-spark-master ghcr.io/${{ github.repository }}/spark:dev
          docker tag chicago-data-pipeline-dbt-build ghcr.io/${{ github.repository }}/dbt:dev
          docker push ghcr.io/${{ github.repository }}/airflow:dev
          docker push ghcr.io/${{ github.repository }}/spark:dev
          docker push ghcr.io/${{ github.repository }}/dbt:dev
```

**Why:** Images are available in GitHub Container Registry (GHCR) for any environment to pull. No local builds needed on deploy.

#### Workflow 3: Release (on merge to prod)

```yaml
# .github/workflows/release.yml
# Triggers: push to prod (i.e., PR merged from dev → prod)
name: Release
on:
  push:
    branches: [prod]

jobs:
  tag-and-release:
    runs-on: ubuntu-latest
    permissions:
      contents: write
      packages: write
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0    # need full history for changelog
      - name: Determine next version
        id: version
        run: |
          LATEST=$(git describe --tags --abbrev=0 2>/dev/null || echo "v1.0.0")
          # Bump minor by default; use commit message [major] or [patch] to override
          if git log --format=%B $LATEST..HEAD | grep -q "\[major\]"; then
            NEXT=$(echo $LATEST | awk -F. '{print "v"($1+1)".0.0"}')
          elif git log --format=%B $LATEST..HEAD | grep -q "\[patch\]"; then
            NEXT=$(echo $LATEST | awk -F. '{print $1"."$2"."($3+1)}')
          else
            NEXT=$(echo $LATEST | awk -F. '{print $1"."($2+1)".0"}')
          fi
          echo "version=$NEXT" >> $GITHUB_OUTPUT
      - name: Tag and release
        run: |
          git tag ${{ steps.version.outputs.version }}
          git push origin ${{ steps.version.outputs.version }}
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - run: |
          V=${{ steps.version.outputs.version }}
          docker compose build \
            --build-arg VERSION=$V \
            airflow-init spark-master dbt-build
          docker tag chicago-data-pipeline-airflow-init ghcr.io/${{ github.repository }}/airflow:$V
          docker tag chicago-data-pipeline-spark-master ghcr.io/${{ github.repository }}/spark:$V
          docker tag chicago-data-pipeline-dbt-build ghcr.io/${{ github.repository }}/dbt:$V
          docker push ghcr.io/${{ github.repository }}/airflow:$V
          docker push ghcr.io/${{ github.repository }}/spark:$V
          docker push ghcr.io/${{ github.repository }}/dbt:$V
      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          tag_name: ${{ steps.version.outputs.version }}
          generate_release_notes: true
```

**Version bump control:** Default is minor bump. Put `[patch]` or `[major]` in a commit message to override.

### 5.4 Branch protection rules

Configure in GitHub: **Settings → Branches → Branch protection rules**

| Rule | `prod` | `dev` |
|---|---|---|
| Require pull request before merging | Yes | Yes |
| Required approvals | 1 | 1 (can relax to 0 for solo dev) |
| Require status checks to pass | Yes | Yes |
| Require branches to be up to date | Yes | Yes |
| Restrict direct pushes | Yes | Yes |
| Allow force pushes | No | No |

**For solo development:** Set dev approvals to 0 (you approve your own PRs). Keep prod at 1 — even if it's you, the PR view forces you to review your own diff before it lands.

### 5.5 Files created in Phase 5

```
.github/
├── workflows/
│   ├── ci.yml           # PR checks: ruff, dbt parse, compose validate, build
│   ├── build.yml        # dev push: build + push images to GHCR
│   └── release.yml      # prod push: tag version + push images + GitHub Release
```

No changes to existing pipeline code. This phase is pure DevOps — it wraps the existing codebase in automated quality gates.

### 5.6 Phase 5 deliverable & verification

**Done when:**
- `prod` and `dev` branches exist, `main` is retired
- Direct push to `prod` is rejected by GitHub
- PR to `dev` triggers CI checks (ruff, dbt parse, compose validate, build)
- Merge to `dev` pushes images to GHCR
- Merge to `prod` creates a git tag + GitHub Release with changelog
- `docker compose pull` works from GHCR images (no local build needed)

**Learning checkpoints:**
- [ ] CI fails on a bad DBT model → learn that `dbt parse` catches syntax errors without a database
- [ ] Branch protection blocks your direct push → learn why this is good even solo
- [ ] GHCR image doesn't match local → learn build args, image tagging, layer caching
- [ ] Release workflow tags wrong version → learn semantic versioning, commit message conventions
- [ ] Force push to prod is rejected → learn why history rewriting on prod is dangerous

---

## The Analytical Payoff

Once Phase 3 is done, you can answer the driving question:

```sql
-- For each community area, monthly crime rate vs Divvy ridership proxy
WITH monthly_crime AS (
    SELECT
        community_area_id,
        DATE_TRUNC('month', date_key) AS month,
        COUNT(*) AS crime_count,
        SUM(CASE WHEN primary_type IN ('THEFT','BATTERY','ROBBERY')
                 THEN 1 ELSE 0 END) AS violent_crime_count
    FROM {{ ref('fact_crime_events') }}
    GROUP BY 1, 2
),
monthly_station_activity AS (
    SELECT
        s.community_area_id,  -- join via station location → community area
        DATE_TRUNC('month', reported_at) AS month,
        AVG(num_bikes_available) AS avg_bikes_available,
        COUNT(*) AS poll_count
    FROM {{ ref('fact_station_reads') }} r
    JOIN {{ ref('dim_station') }} s USING (station_id)
    GROUP BY 1, 2
)
SELECT
    c.community_area_id,
    c.month,
    c.crime_count,
    c.violent_crime_count,
    s.avg_bikes_available,
    -- correlation, lag analysis, etc.
FROM monthly_crime c
FULL OUTER JOIN monthly_station_activity s
    ON c.community_area_id = s.community_area_id
   AND c.month = s.month
ORDER BY c.community_area_id, c.month
```

This is the query that makes the whole project a portfolio piece, not a tutorial exercise.
| Tool | What you should be able to explain after this project |
|---|---|
| **Docker** | Multi-service compose, networking by service name, volume mounts, init scripts, building images |
| **Postgres** | Schemas, roles, JDBC connectivity, performance with analytical workloads (indexes, explain plans) |
| **Spark** | DataFrame API, partitioning, shuffling, JDBC sinks, Structured Streaming, checkpointing |
| **Kafka** | Topics, partitions, consumer groups, offsets, producers, key-based ordering |
| **DBT** | Sources, staging, marts, tests, refs, incremental models, materializations |
| **Airflow** | DAGs, operators, XCom, sensors, retries, SLAs, scheduling, idempotency |
| **Grafana** | Data sources, dashboards, alerts, variables, geospatial panels |
| **Terraform** | Resources, state, providers, variables, modules, destroy safety |
| **Airbyte** | Connectors, sources, destinations, sync modes (full refresh vs incremental) |
| **GitHub Actions** | Workflows, triggers, runners, secrets, CI/CD pipelines, branch protection |

---

## Common Mistakes to Expect (and learn from)

1. **Building everything at once** — Don't. Follow the phases. Each phase is a working system.
2. **Using the full 8M-row dataset on day 1** — Start with one year. Scale later.
3. **Ignoring idempotency** — Your DAG will double-insert. Learn upserts early.
4. **Treating Docker as magic** — Read the compose file. Understand what each service does and how they talk.
5. **Skipping DBT tests** — Tests are the difference between "data pipeline" and "reliable data pipeline."
6. **Not reading the logs** — When something breaks, the container logs have the answer. `docker logs <service>` is your best friend.
7. **Hardcoding credentials** — Use `.env` files and environment variables from the start. Unlearning bad habits is harder than learning good ones.
8. **Never breaking it on purpose** — In Phase 3, stop the Kafka producer. Delete a row. Introduce a null. Watch your observability catch it. If it doesn't, your observability isn't good enough yet.

---

## Suggested Pace

Don't rush. The point is learning, not finishing.

- **Week 1-2:** Phase 1 (batch). Expect Docker networking pain. It's normal.
- **Week 3-4:** Phase 2 (stream). Expect Kafka/Spark integration pain. It's normal.
- **Week 5:** Phase 3 (observability). This is where it starts feeling real.
- **Week 6+:** Phase 4 (cloud). Optional but high-value for portfolio.
- **Week 7:** Phase 5 (CI/CD). Wraps everything in automated quality gates.

When you're stuck, ask me specific questions — "my Spark job OOMs when writing to Postgres, here's the error" is a great question. "How do I do Spark" is not. The learning is in the debugging.
