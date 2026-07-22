# Postgres

### Useful Commands
```bash
# Connect via psql
psql -h localhost -p 5432 -U chicago -d chicago_analytics

# Inside psql
\dt raw.*          # list tables in raw schema
\dt mart.*         # list tables in mart schema
\d raw.crime_events  # describe table structure
\dn                # list schemas
\q                 # quit
```

### Schemas
- `raw` — landing zone, untransformed data from Spark/Kafka
- `staging` — DBT staging layer: light cleaning, renaming, type casting (1:1 with source tables)
- `mart` — DBT final output: facts + dimensions, analytics-ready

### Schema vs DBT Layer
Postgres schemas are **physical namespaces** in the database. DBT layers are **logical transformation stages** (folders in your dbt project). They're different concepts:
- You can have 3 DBT model layers mapped to 3 Postgres schemas (this project's approach)
- Or all DBT output in one schema (simpler, less separation)
- Schema-per-layer gives clearer separation and finer-grained access control (e.g., grant analysts access to `mart` only)

### Init Scripts (`/docker-entrypoint-initdb.d/`)
- Scripts in this directory run **once** on first container startup (when the data volume is empty)
- Supported formats: `.sql` (run as SQL), `.sh` (run as shell script), `.sql.gz` (decompressed then run)
- Run in alphabetical order as the `POSTGRES_USER` connected to `POSTGRES_DB`
- **Changing init.sql after first run does nothing** — must destroy the volume: `docker compose down -v`
- SQL files **cannot read `.env` variables** — only `docker-compose.yml` can interpolate `${VAR}`. Use a `.sh` script if you need env vars in init logic.

### Postgres "If Not Exists" Workarounds
Postgres lacks `CREATE USER IF NOT EXISTS` and `CREATE DATABASE IF NOT EXISTS`. Workarounds:

```sql
-- User: use DO block with pg_roles check
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'myuser') THEN
        CREATE ROLE myuser WITH LOGIN PASSWORD 'mypass';
    END IF;
END
$$;

-- Database: use \gexec (psql meta-command)
-- CREATE DATABASE can't run inside a transaction, so IF NOT EXISTS patterns don't work
SELECT 'CREATE DATABASE mydb OWNER myuser'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mydb')\gexec
```

- `$$` are dollar quotes — tell Postgres "treat everything between as a string body"
- `\gexec` takes the query result (a SQL string) and executes it as a new command

### Cast Syntax
```sql
-- Postgres: use :: for casting
SELECT '2024-01-15'::date;
SELECT '123'::integer;

-- Postgres does NOT have TRY_CAST (that's Snowflake/DuckDB)
-- Use CASE/REGEXP guards or clean data upstream in Spark

-- EXTRACT fields: year, month, day, hour, minute, second, dow, epoch
-- 'date' is NOT a valid EXTRACT field
SELECT EXTRACT(year FROM occurred_at);  -- valid
SELECT occurred_at::date;               -- use this for date casting
```

---

---

**← Previous:** [docker-compose](docker-compose.md) | **Next:** [data-sources](data-sources.md) →
