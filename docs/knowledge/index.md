# Knowledge Base

Reference material, useful commands, and explanations accumulated throughout the project. Not a tutorial — a quick lookup for things you've already learned but might forget.

## Sections

| File | Topic | What's inside |
|---|---|---|
| [wsl.md](wsl.md) | WSL | Useful commands, why WSL filesystem is faster, Devin IDE sync |
| [uv.md](uv.md) | uv (Python Package Manager) | What is uv, uv sync vs pip install, uv in Docker, common commands |
| [docker-compose.md](docker-compose.md) | Docker Compose | Project names, env vars, depends_on, volumes, healthchecks, YAML anchors |
| [architecture.md](architecture.md) | How Everything Connects | 10 mermaid diagrams: uv→Docker, Spark→Docker (JDBC + Kafka JARs), Airflow→Docker, init.sql, .env, docker.sock, startup order, file→container map, Kafka+Zookeeper, Spark Streaming→Kafka→Postgres |
| [postgres.md](postgres.md) | Postgres | Useful commands, schemas, init.sql, two databases pattern |
| [dbt.md](dbt.md) | DBT | Key Jinja variables, common commands, model naming |
| [spark.md](spark.md) | Spark | apache/spark image, spark-class commands, healthcheck, Kafka connector JARs, Structured Streaming + Kafka, foreachBatch→JDBC, checkpointing, useful commands |
| [kafka.md](kafka.md) | Kafka | What is Kafka, core concepts (cluster, topic, partition, offset, producer, consumer, broker, Zookeeper) with mermaid diagrams, Spark Structured Streaming consumer, checkpointing, our setup, single-broker overrides, useful commands |
| [airflow.md](airflow.md) | Airflow | Airflow 2.x vs 3.x comprehensive comparison (9 subsections), commands, SimpleAuthManager, **3.0 breaking changes** (SLA removed, SqlSensor row-not-cursor, try_number, on_failure_callback timing, DAG bundle refresh, dags delete TTY), sensors (reschedule vs poke), AIRFLOW_CONN_ env var pattern, cross-DAG dependencies (sensors vs dbt selectors) |
| [git.md](git.md) | Git | Useful commands |
| [data-sources.md](data-sources.md) | Data Sources | Chicago Crime Socrata API reference, Divvy GBFS API reference |
| [mermaid-syntax.md](mermaid-syntax.md) | Mermaid Syntax | Quoting rules for special characters, scanner script |
| [grafana.md](grafana.md) | Grafana | Core concepts (datasource, dashboard, panel, query, provisioning), our setup, file-based provisioning, env var interpolation gotcha (`$VAR` not `{{.VAR}}`), `jsonData.database` deep dive (browser vs API code paths), two-datasource pattern for separate Postgres databases, dashboard inventory (11 panels), DAG run order (sensor fixes race), useful commands, 10 common mistakes, 8 mermaid diagrams, **Phase 3.2–3.4** (DBT test panel, failed-tasks panel, panel thresholds as alerts, verification approach) |
| [gcp.md](gcp.md) | Google Cloud Platform | GCP auth model (two layers: human Gmail vs service account), setup process (project, billing, APIs, service account, roles, key), WSL vs Windows/PowerShell command differences (6 pitfalls including separate WSL auth state + least-privilege SA permissions), pitfalls/risks/cautions, our Phase 4.1 setup, pointer to terraform.md, useful commands, gsutil deprecation note |
| [terraform.md](terraform.md) | Terraform | Concepts (IaC, state, providers), three-command workflow, file structure, resources managed (2 BigQuery datasets + 1 GCS bucket), key decisions (delete_contents_on_destroy, lifecycle rules, version pinning), how Terraform auths to GCP (SA key, not gcloud), 3 errors hit during Phase 4.2 (WSL account switch, `~` not expanded, least-privilege SA can't list APIs), verification, useful commands |

## How to Use

- **Looking for a command?** Find the topic in the table above, open that file.
- **Adding new knowledge?** Create a new `.md` file in this folder, add it to the table above.
- **Cross-referencing?** Use relative links: `[see Airflow comparison](airflow.md)`

## Relationship to Other Docs

```
docs/knowledge/          ← THIS FOLDER — reference material (looked up by topic)
docs/conventions/        ← coding standards (how to write code for each tool)
docs/phases/             ← phase-completion snapshots (what was built, errors, verification)
changelog.md             ← running log of ALL errors ever hit (append at bottom)
docs/operations-performed.md ← audit trail of ALL structural changes (append at bottom)
chat-history/current-state.md ← handoff doc for new sessions
```
