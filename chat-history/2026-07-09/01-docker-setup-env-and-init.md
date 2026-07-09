# Docker Setup — .env.example + init.sql

## Summary
Created environment variable template (`.env.example`) and Postgres init script (`init.sql`). Established the two-database-in-one-container architecture and the 3-schema layering (raw, staging, mart).

## Decisions Made
- **Two databases in one Postgres container** — `chicago_analytics` (warehouse) + `airflow_metadata` (Airflow internal). Avoids second container. `init.sql` creates both.
- **LocalExecutor over CeleryExecutor** — parallelism without Redis/RabbitMQ containers
- **3 schemas: raw, staging, mart** — traditional DBT layering, skipped `intermediate` to keep simpler
- **Hardcoded values in init.sql** — SQL files can't read `.env`. Values match `.env.example`.
- **`DO $$ ... $$` block for user creation** — Postgres has no `CREATE USER IF NOT EXISTS`
- **`\gexec` for database creation** — `CREATE DATABASE` can't run inside a transaction
- **Image names in docker-compose.yml, not .env** — they're not secrets or environment-specific config

## Files Created/Modified
- `.env.example` — Postgres creds, Airflow metadata DB creds, LocalExecutor, COMPOSE_PROJECT_NAME, Socrata token placeholder
- `init.sql` — 3 schemas (raw, staging, mart), airflow user, airflow_metadata database

## Key Context
- Init scripts run only on first startup (empty volume). Changing `init.sql` after first run requires `docker compose down -v` (destroys data).
- Postgres schemas (physical namespaces) ≠ DBT layers (logical transformation stages). We map 3 DBT layers to 3 Postgres schemas.
- Airflow needs its own DB — pointing it at analytics DB pollutes it with `task_instance`, `dag_run`, etc.

## Errors Encountered
None in this chunk.
