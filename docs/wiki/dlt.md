# dlt (data load tool) Reference

## Why We Switched from Airbyte to dlt

The original plan (`chicago-pipeline-plan.md` sections 4.4–4.6) specified **Airbyte** for Divvy trip history ingestion from S3 into BigQuery. We switched to **dlt** instead. Here's the full reasoning.

### The Problem with Airbyte on WSL2

Airbyte is a full-featured ELT platform with a web UI, 350+ source connectors, and built-in scheduling. It's excellent for enterprise environments. But for this project on WSL2 with an i7-7700HQ (4 cores, 8GB RAM allocated to WSL), it was the wrong tool:

| Issue | Airbyte | Impact on WSL2 |
|---|---|---|
| **Container footprint** | 5-6 Docker containers (server, scheduler, worker, temp-worker, db, pod-scheduler) | We already run 12 containers (Postgres, Spark×2, Airflow×3, Kafka, Zookeeper, Grafana). Adding 5-6 more would push us to 17-18 containers on a 4-core machine. |
| **RAM usage** | 2-4 GB idle | WSL2 has 8 GB allocated. 12 existing services already use ~4 GB. Airbyte would consume 25-50% of remaining RAM — risk of OOM kills. |
| **Setup complexity** | Docker Compose config + UI walkthrough + connector configuration + connection scheduling | 30+ minutes of setup for a one-time bulk load of 75 monthly CSV files. Overkill. |
| **Skill demonstration** | Shows you can configure a UI | dlt shows you can write Python ingestion code — more relevant for DE interviews. |
| **Maintenance** | Another service to keep running, update, and debug | One more failure point in the stack. |

### Why dlt Was the Right Choice

dlt (data load tool) is a Python library — not a platform. You `pip install` it, write a Python script, and run it. No containers, no UI, no scheduler. It does one thing: move data from A to B.

| Advantage | What it means for us |
|---|---|
| **Zero containers** | Runs inside the existing Airflow container. No new Docker services. |
| **~100 MB RAM** | Negligible memory overhead. No OOM risk. |
| **5-minute setup** | `pip install "dlt[bigquery]"` + write a 100-line script. Done. |
| **Native BigQuery** | `dlt.destinations.bigquery(...)` — first-class support, same credentials as google-cloud-bigquery. |
| **Schema inference** | dlt reads the CSV, infers column types, creates the BigQuery table automatically. No pre-define schema. |
| **Append mode** | Each monthly file loaded once, never overwritten. Idempotent at the application level (skip months already loaded). |
| **Generator-based** | Yields rows one at a time — loads 34.8M rows without holding everything in memory. |
| **Orchestrated by Airflow** | dlt is just a Python script. Airflow's BashOperator runs it. No separate scheduler. |
| **Code, not config** | The ingestion logic is in `ingestion/load_divvy_trips.py` — readable, version-controlled, testable. Not a UI config blob in a database. |

### The Decision

| Factor | Airbyte | dlt | Winner |
|---|---|---|---|
| Footprint | 5-6 containers | 1 pip install | **dlt** |
| RAM | 2-4 GB | ~100 MB | **dlt** |
| Setup time | 30+ min | 5 min | **dlt** |
| BigQuery support | Native | Native | Tie |
| UI | Web UI | None (code) | Airbyte (but not needed here) |
| Scheduling | Built-in | External (Airflow) | Tie (we already have Airflow) |
| Skill demo | Config | Code | **dlt** (better for interviews) |
| Best for | Enterprise, many connectors | Script-based, lightweight | Context-dependent |

**Verdict:** dlt for this project. Airbyte would be the right choice in a production environment with many data sources, a team of analysts configuring connectors via UI, and infrastructure that can spare 4 GB RAM. On WSL2 with a learning project, dlt is the pragmatic choice.

### What We Did NOT Lose by Switching

- **Ingestion skill:** dlt IS an ingestion tool. We still demonstrate EL/T — just with a different tool. The S3 → BigQuery path is the same; the tool is lighter.
- **Airbyte knowledge:** The plan still references Airbyte. We know what it is, why we didn't use it, and when we would. That's the important part.
- **Scalability:** dlt handles 34.8M rows fine. For larger volumes, dlt has incremental loading, merge mode, and resource limits. Airbyte's advantage is many connectors, not scale.

---

## dlt Concepts

### What is dlt?

dlt is a Python library for loading data from sources into destinations (BigQuery, Postgres, Snowflake, etc.). It's an ELT tool — Extract + Load, no Transform (that's DBT's job). Think of it as a lightweight Airbyte replacement that runs as a Python script, no extra containers needed.

### Installation

```bash
pip install "dlt[bigquery]"
```

In Airflow: add to `airflow/requirements.txt`, rebuild image.

### BigQuery Configuration

dlt uses `GOOGLE_APPLICATION_CREDENTIALS` env var (same as google-cloud-bigquery). No separate dlt config file needed.

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="divvy_trips",
    destination=dlt.destinations.bigquery(
        project_id=os.environ["GCP_PROJECT_ID"],
        location=os.environ.get("BIGQUERY_LOCATION", "US"),
    ),
    dataset_name="raw",  # BigQuery dataset
)
```

### Loading Data

```python
# From a list of dicts
data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
pipeline.run(data, table_name="users")

# From a generator (streaming, memory-efficient)
def trip_generator():
    for row in huge_csv:
        yield {"ride_id": row[0], "started_at": row[1], ...}
pipeline.run(trip_generator(), table_name="divvy_trips")
```

### Append vs Replace

```python
# Default: append (adds rows, keeps existing)
pipeline.run(data, table_name="divvy_trips")

# Replace: drops and recreates
pipeline.run(data, table_name="divvy_trips", write_disposition="replace")
```

For Divvy trips: **append mode** — each monthly file is loaded once, never overwritten. Idempotency is handled at the application level (skip months already loaded).

### Schema Evolution

dlt automatically infers schema from the data. If a new column appears in a later load, dlt adds it to the BigQuery table. This is useful for evolving schemas (Divvy CSVs changed format over the years).

### Key Concepts

| Concept | Description |
|---|---|
| `pipeline` | The ETL pipeline object (source → destination) |
| `destination` | Where data lands (BigQuery, Postgres, etc.) |
| `dataset_name` | BigQuery dataset (schema) name |
| `resource` | A data source within a pipeline (table, API, file) |
| `source` | A collection of resources (e.g., a dlt source package) |
| `write_disposition` | `append` (default), `replace`, `merge` |
| `load_id` | Unique ID per pipeline run (for tracking) |

### Our Usage (Phase 4.4)

- **Script:** `ingestion/load_divvy_trips.py`
- **Source:** Divvy S3 bucket (`divvy-tripdata.s3.amazonaws.com`) — monthly ZIP files containing CSV
- **Destination:** BigQuery `raw.divvy_trips`
- **Mode:** Append (each month loaded once)
- **CLI:** `--month YYYYMM`, `--from/--to YYYYMM`, `--all`, `--dry-run`
- **Volume:** 34.8M rows across 75 months (2020-04 to 2026-06)
- **dlt version:** 1.29.0

### Gotchas

- **dlt creates tables automatically** — no need to pre-create the BigQuery table. dlt infers schema from the data.
- **`dlt[bigquery]` extras** — must install with the `[bigquery]` extra, not just `dlt`. Without it, `dlt.destinations.bigquery` is not available.
- **Credentials** — dlt reads `GOOGLE_APPLICATION_CREDENTIALS` env var. Same as google-cloud-bigquery. No separate dlt auth.
- **Memory** — for large files, use a generator (yield rows one at a time) instead of loading all rows into memory. dlt handles batching internally.
- **Schema inference** — dlt infers types from the first batch. If later batches have different types, dlt may cast or error. For Divvy CSVs, we pre-cast all columns to strings in the extraction step, then let DBT handle type casting in staging.

---

**← Previous:** [terraform](terraform.md) | **Next:** [bigquery-ml](bigquery-ml.md) →
