#!/usr/bin/env python3
"""
Record dbt test results into Postgres for Grafana observability (Phase 3.2).

dbt writes `target/run_results.json` after every `dbt build`/`dbt test`. That
file is the source of truth for which tests ran and their outcomes (pass/fail/
warn/skip/error), but it is a *file* — not queryable by Grafana. This script
parses it and upserts one row per test into `observability.dbt_test_results`,
which the Grafana "DBT tests" panel queries.

Why a custom recorder instead of a package (e.g. re-data/dbt_artifacts)?
- No new dbt dependency (the project deliberately keeps packages.yml small).
- The artifact we care about is tiny (test outcomes only) — a 40-line script
  is clearer than pulling in a package that writes 10+ tables.
- It runs from the Airflow container (which already has psycopg2 via the
  postgres provider), as a BashOperator after `dbt build`.

Run by both crime_batch and divvy_stream DAGs:
    python /opt/airflow/scripts/record_dbt_results.py

Environment (set in the Airflow container by docker-compose):
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
Postgres host is the Docker service name `postgres` (hardcoded below to match
dbt/profiles.yml and the rest of the project — POSTGRES_HOST is not exported
to the Airflow container).

Idempotency: rows are keyed by (invocation_id, test_name). Re-running for the
same dbt invocation (e.g. a retried DAG task) DELETEs then re-inserts that
invocation's rows — no duplicates, latest status wins.
"""

import json
import os
import sys
from pathlib import Path

import psycopg2

RUN_RESULTS_PATH = Path("/opt/airflow/dbt/target/run_results.json")

# Postgres host — Docker service name (matches dbt/profiles.yml + airflow
# dbt_profiles). Not taken from an env var because POSTGRES_HOST is not
# exported to the Airflow container; the connection string in docker-compose
# hardcodes `postgres` too.
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432


def main() -> int:
    if not RUN_RESULTS_PATH.exists():
        print(
            f"ERROR: dbt run_results.json not found at {RUN_RESULTS_PATH}. "
            "Did `dbt build` run first?",
            file=sys.stderr,
        )
        return 1

    data = json.loads(RUN_RESULTS_PATH.read_text())
    metadata = data.get("metadata", {})
    invocation_id = metadata.get("invocation_id")
    generated_at = metadata.get("generated_at")

    if not invocation_id or not generated_at:
        print("ERROR: run_results.json missing invocation_id/generated_at", file=sys.stderr)
        return 1

    # `dbt build` records seeds, models, tests, and snapshots in results.
    # Keep only tests — that is what the Grafana panel reports on.
    #
    # dbt 1.11's run_results.json does NOT populate `resource_type` (it is
    # None for every entry). Tests are identified by `unique_id` starting
    # with "test.". The `name` field is also None — the human-readable name
    # is embedded in `unique_id` as `test.chicago_crime.<name>.<hash>`.
    tests = [r for r in data.get("results", []) if r.get("unique_id", "").startswith("test.")]

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with conn:  # transactional — commit on success, rollback on error
            with conn.cursor() as cur:
                # Observability metadata lives in its own schema, separate
                # from the analytics mart schema. Created idempotently so no
                # init.sql change or volume wipe is needed.
                cur.execute("CREATE SCHEMA IF NOT EXISTS observability")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS observability.dbt_test_results (
                        invocation_id   text        NOT NULL,
                        generated_at    timestamptz NOT NULL,
                        test_name       text        NOT NULL,
                        status          text        NOT NULL,
                        failures        integer,
                        execution_time  double precision,
                        recorded_at     timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (invocation_id, test_name)
                    )
                    """
                )
                # Replace this invocation's rows so a re-run reflects the
                # latest outcome rather than accumulating duplicates.
                cur.execute(
                    "DELETE FROM observability.dbt_test_results WHERE invocation_id = %s",
                    (invocation_id,),
                )
                for t in tests:
                    # unique_id format: "test.chicago_crime.<test_name>.<hash>"
                    # Strip the prefix and trailing hash for a readable name.
                    uid = t.get("unique_id", "<unknown>")
                    parts = uid.split(".")
                    # Drop "test", "chicago_crime" (first 2) and the hash (last)
                    name = ".".join(parts[2:-1]) if len(parts) > 3 else uid
                    cur.execute(
                        """
                        INSERT INTO observability.dbt_test_results
                            (invocation_id, generated_at, test_name, status, failures, execution_time)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            invocation_id,
                            generated_at,
                            name,
                            t.get("status"),
                            t.get("failures"),
                            t.get("execution_time"),
                        ),
                    )
    finally:
        conn.close()

    counts = {}
    for t in tests:
        counts[t.get("status")] = counts.get(t.get("status"), 0) + 1
    summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    print(
        f"Recorded {len(tests)} dbt test results for invocation {invocation_id} "
        f"({summary}) into observability.dbt_test_results"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
