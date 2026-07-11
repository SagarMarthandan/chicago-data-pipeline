# Knowledge Base

Reference material, useful commands, and explanations accumulated throughout the project. Not a tutorial — a quick lookup for things you've already learned but might forget.

## Sections

| File | Topic | What's inside |
|---|---|---|
| [wsl.md](wsl.md) | WSL | Useful commands, why WSL filesystem is faster, Devin IDE sync |
| [uv.md](uv.md) | uv (Python Package Manager) | What is uv, uv sync vs pip install, uv in Docker, common commands |
| [docker-compose.md](docker-compose.md) | Docker Compose | Project names, env vars, depends_on, volumes, healthchecks, YAML anchors |
| [architecture.md](architecture.md) | How Everything Connects | 9 mermaid diagrams: uv→Docker, Spark→Docker, Airflow→Docker, init.sql, .env, docker.sock, startup order, file→container map |
| [postgres.md](postgres.md) | Postgres | Useful commands, schemas, init.sql, two databases pattern |
| [dbt.md](dbt.md) | DBT | Key Jinja variables, common commands, model naming |
| [spark.md](spark.md) | Spark | apache/spark image, spark-class commands, healthcheck, useful commands |
| [kafka.md](kafka.md) | Kafka | Useful commands (Phase 2 reference) |
| [airflow.md](airflow.md) | Airflow | Airflow 2.x vs 3.x comprehensive comparison (9 subsections), commands, SimpleAuthManager |
| [git.md](git.md) | Git | Useful commands |
| [data-sources.md](data-sources.md) | Data Sources | Chicago Crime Socrata API reference, Divvy GBFS API reference |
| [mermaid-syntax.md](mermaid-syntax.md) | Mermaid Syntax | Quoting rules for special characters, scanner script |

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
