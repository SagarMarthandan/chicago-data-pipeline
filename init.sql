-- ============================================================
-- Postgres Init Script — runs once on first container startup
-- ============================================================
-- This script is mounted into /docker-entrypoint-initdb.d/ by
-- docker-compose.yml. Postgres executes it automatically when
-- the data volume is empty (first run only).
--
-- If you change this file after the first run, you MUST destroy
-- the volume and recreate:
--   docker compose down -v   # WARNING: destroys all data
--   docker compose up -d
--
-- Context: runs as POSTGRES_USER (superuser) connected to
-- POSTGRES_DB (chicago_analytics).
-- ============================================================


-- ============ Analytics Warehouse Schemas ============
-- raw     — landing zone for untransformed data from Spark/Kafka
-- staging — DBT staging layer: light cleaning, renaming, type casting (1:1 with source tables)
-- mart    — DBT final output: facts + dimensions, analytics-ready
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- Grant full access to the chicago user (your warehouse user)
GRANT ALL PRIVILEGES ON SCHEMA raw TO chicago;
GRANT ALL PRIVILEGES ON SCHEMA staging TO chicago;
GRANT ALL PRIVILEGES ON SCHEMA mart TO chicago;


-- ============ Airflow Metadata Database ============
-- Airflow needs its own database to track DAG runs, task states,
-- scheduling info, and logs. This is separate from your analytics
-- warehouse to avoid polluting it with Airflow's internal tables.
--
-- Values must match .env:
--   AIRFLOW_DB_USER     = airflow
--   AIRFLOW_DB_PASSWORD = airflow_pass
--   AIRFLOW_DB_NAME     = airflow_metadata

-- Create the Airflow user (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
        CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow_pass';
    END IF;
END
$$;

-- Create the Airflow metadata database (if not exists)
-- Note: CREATE DATABASE cannot run inside a transaction block,
-- so we can't use IF NOT EXISTS here. The DO block above handles
-- the user, and we check for the database separately.
SELECT 'CREATE DATABASE airflow_metadata OWNER airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow_metadata')\gexec

-- Grant Airflow user full privileges on its database
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow;
