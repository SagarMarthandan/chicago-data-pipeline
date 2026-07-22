# Wiki — Technology Reference

Reference material, useful commands, and explanations accumulated throughout the project. Not a tutorial — a quick lookup for things you've already learned but might forget.

## Sections

| File | Topic | What's inside |
|---|---|---|
| [wsl.md](wsl.md) | WSL | Useful commands, why WSL filesystem is faster, Devin IDE sync |
| [uv.md](uv.md) | uv (Python Package Manager) | What is uv, uv sync vs pip install, uv in Docker, common commands |
| [docker-compose.md](docker-compose.md) | Docker Compose | Project names, env vars, depends_on, volumes, healthchecks, YAML anchors |
| [architecture.md](architecture.md) | How Everything Connects | 10 mermaid diagrams: uv→Docker, Spark→Docker (JDBC + Kafka JARs), Airflow→Docker, init.sql, .env, docker.sock, startup order, file→container map, Kafka+Zookeeper, Spark Streaming→Kafka→Postgres |
| [postgres.md](postgres.md) | Postgres | Useful commands, schemas, init.sql, two databases pattern |
| [dbt.md](dbt.md) | DBT | Key Jinja variables, common commands, model naming, dbt docs |
| [spark.md](spark.md) | Spark | apache/spark image, spark-class commands, healthcheck, Kafka connector JARs, Structured Streaming + Kafka, foreachBatch→JDBC, checkpointing, useful commands |
| [kafka.md](kafka.md) | Kafka | What is Kafka, core concepts (cluster, topic, partition, offset, producer, consumer, broker, Zookeeper) with mermaid diagrams, Spark Structured Streaming consumer, checkpointing, our setup, single-broker overrides, useful commands |
| [airflow.md](airflow.md) | Airflow | Airflow 2.x vs 3.x comprehensive comparison (9 subsections), commands, SimpleAuthManager, **3.0 breaking changes** (SLA removed, SqlSensor row-not-cursor, try_number, on_failure_callback timing, DAG bundle refresh, dags delete TTY), sensors (reschedule vs poke), AIRFLOW_CONN_ env var pattern, cross-DAG dependencies |
| [git.md](git.md) | Git | Useful commands |
| [data-sources.md](data-sources.md) | Data Sources | Chicago Crime Socrata API reference, Divvy GBFS API reference, Divvy S3 trip history, current data inventory |
| [mermaid-syntax.md](mermaid-syntax.md) | Mermaid Syntax | Quoting rules for special characters, scanner script |
| [grafana.md](grafana.md) | Grafana | Core concepts, our setup, file-based provisioning, env var interpolation gotcha, two-datasource pattern, dashboard inventory, 10 common mistakes, 8 mermaid diagrams |
| [gcp.md](gcp.md) | Google Cloud Platform | GCP auth model, setup process, WSL vs Windows/PowerShell command differences, pitfalls, useful commands |
| [terraform.md](terraform.md) | Terraform | Concepts, three-command workflow, file structure, resources managed, key decisions, errors, useful commands |
| [dlt.md](dlt.md) | dlt (data load tool) | Why we switched from Airbyte to dlt, dlt vs Airbyte decision matrix, concepts, installation, BigQuery config, our Phase 4.4 usage |
| [bigquery-ml.md](bigquery-ml.md) | BigQuery ML | BQML concepts, dbt integration via post_hook, gotchas, our Phase 4.8 usage (linear_reg, R²=0.434, crime coefficient +1.45) |

## Coding Conventions

| File | Topic |
|---|---|
| [conventions/airflow.md](conventions/airflow.md) | Airflow DAG conventions |
| [conventions/dbt.md](conventions/dbt.md) | DBT model conventions |
| [conventions/docker.md](conventions/docker.md) | Docker Compose conventions |
| [conventions/spark.md](conventions/spark.md) | Spark job conventions |

## How to Use

- **Looking for a command?** Find the topic in the table above, open that file.
- **Adding new knowledge?** Create a new `.md` file in this folder, add it to the table above.
- **Cross-referencing?** Use relative links: `[see Airflow comparison](airflow.md)`

## Relationship to Other Docs

```
docs/wiki/               ← THIS FOLDER — reference material (looked up by topic)
docs/phase/              ← consolidated phase docs (what was built, errors, verification)
docs/chat-history/       ← daily conversation logs + handoff doc
CHANGELOG.md             ← running log of ALL errors ever hit (root)
docs/chicago-pipeline-plan.md ← full phased design
```
