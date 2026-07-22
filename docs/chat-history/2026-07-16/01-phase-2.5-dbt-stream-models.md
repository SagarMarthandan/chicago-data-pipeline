# Phase 2.5 — DBT Stream Models

## Summary

Built DBT staging and mart models for the Divvy station status streaming data. Created `stg_station_status` (staging view on `raw.station_status`) and `fact_station_reads` (mart table, one row per station poll). Updated `dim_date` to span both crime (2023) and station read (2026) dates. All 59 DBT tests passed on first run with zero errors. The Phase 2 gate query ("avg bikes available at station X") returns correct results.

## Decisions Made

- **Dedup on Kafka coordinates (partition + offset)** — streaming equivalent of crime's `DISTINCT ON (id)`. Kafka coordinates are the system-of-record unique identifier for streaming data.
- **Column renames: `last_reported`→`reported_at`, `ingest_timestamp`→`ingested_at`** — clearer naming matching the `occurred_at`/`updated_at` pattern from crime staging. `reported_at` = when station reported to GBFS; `ingested_at` = when pipeline received it via Kafka.
- **Fact grain = one row per station poll** — most granular level, supports any aggregation without pre-aggregation.
- **Derived `total_vehicles_available` = bikes + ebikes + COALESCE(scooters, 0)** — convenience column. COALESCE needed because scooter fields are nullable (not all stations have scooters).
- **dim_date spans all fact sources via UNION ALL** — single date dimension serves both `fact_crime_events` and `fact_station_reads`. Without this, 2026 station dates would fail the FK relationship test.
- **No unique test on `station_id`** — station_id repeats across polls (unlike `crime_id` which is unique). The grain is station + `reported_at`, not station alone.
- **No `dim_station` dimension yet** — station metadata (name, location, capacity) comes from `station_information.json` which hasn't been ingested. Will be needed for the crime+nearby-ridership analysis but is out of scope for Phase 2.5.

## Files Created/Modified

- `dbt/models/staging/stg_station_status.sql` — Created. Staging view on `raw.station_status`. Renames columns, deduplicates on Kafka coordinates, casts all types explicitly.
- `dbt/models/marts/fact_station_reads.sql` — Created. Mart table: one row per station poll. Includes `date_key` FK, `reported_at`/`ingested_at` timestamps, all availability counts, boolean status flags, derived `total_vehicles_available`, Kafka traceability columns. Filters null station_id/reported_at.
- `dbt/models/marts/dim_date.sql` — Modified. Now uses UNION ALL of min/max from `stg_crime_events` + `stg_station_status`, then `date_bounds` CTE for overall min/max. Generates 1,292 rows (2023-01-01 through 2026-07-15).
- `dbt/models/staging/schema.yml` — Modified. Added `station_status` to `raw` source with column documentation. Added `stg_station_status` model with tests (not_null, expect_between 0-100 on bikes/docks).
- `dbt/models/marts/schema.yml` — Modified. Updated `dim_date` description + year test bounds (2023–2026). Added `fact_station_reads` model with tests (not_null, expect_between, relationships to dim_date).
- `changelog.md` — Added Phase 2.5 entry (no errors, all tests passed first run).
- `docs/operations-performed.md` — Added Phase 2.5 entry with files, decisions, verification.
- `docs/phases/phase-2.5-dbt-stream-models.md` — Created. Phase completion doc with mermaid diagram, decisions, verification.
- `docs/phases/README.md` — Updated index: Phase 2.5 Complete, Phase 2.6 next.
- `chat-history/current-state.md` — Updated handoff for Phase 2.5 completion.

## Key Context

- **Container name is `chicago-data-pipeline-postgres-1`** not `postgres`. Use `docker compose exec postgres` or the full container name. This was discovered at the start of this session.
- **DBT build command from host:** `docker run --rm --network chicago-data-pipeline_default -v ./dbt:/opt/airflow/dbt -v ./airflow/dbt_profiles:/opt/airflow/dbt_profiles -e POSTGRES_USER=chicago -e POSTGRES_PASSWORD=chicago1234 -e POSTGRES_DB=chicago_analytics chicago-data-pipeline-dbt:latest dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt_profiles`
- **`raw.station_status` has 5,640 rows** from Phase 2.4 verification (5 micro-batches × 1,128 stations). This data is from 2026-07-15 only.
- **`fact_station_reads` has 5,640 rows** — same count as raw because dedup didn't remove any rows (no duplicate Kafka messages in the test data).
- **`dim_date` now has 1,292 rows** — spans 2023-01-01 (earliest crime) through 2026-07-15 (latest station read). The gap between 2023 and 2026 is filled with dates that have no fact rows — this is correct for a date dimension.
- **Phase 2 gate is almost met** — all components work individually (Kafka, producer, Spark streaming, DBT models). Phase 2.6 (Airflow DAG) will tie them together for automated end-to-end orchestration.

## Errors Encountered

None. All 59 DBT tests passed on the first `dbt build` run. The only issue was the container name mismatch (`postgres` vs `chicago-data-pipeline-postgres-1`) which was resolved quickly.

## User Preferences Learned

- User confirmed AI-writes-code mode is still active ("yes. start" — no request for Socratic guidance).
- User prefers to start new sessions fresh — relies on `current-state.md` handoff.
