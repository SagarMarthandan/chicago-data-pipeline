# Phase 2.6 — Airflow Stream DAG

**Date:** 2026-07-16
**Session topic:** Building the Airflow DAG for the streaming pipeline (Phase 2.6), completing Phase 2.

## What was done

- Built `airflow/dags/divvy_stream_dag.py` — 7-task DAG: create_topic → start_producer → start_stream → wait_for_data → dbt_build → stop_stream → stop_producer
- Fixed 9 errors across 3 infrastructure issues:
  1. kafka-python not installable in Airflow image (uv fails, pip as root refused, volume mount shadows package)
  2. Spark checkpoint permissions (named volume mounts as root, entrypoint.sh + gosu fix)
  3. Airflow BashOperator kills background processes (switched producer to --once mode)
- Verified end-to-end: all 7 tasks succeed, 2,001 rows in fact_station_reads, analytics queries return correct results
- Phase 2 gate met: full pipeline works end-to-end

## Key decisions

- Producer uses `--once` mode (single poll) instead of continuous — Airflow BashOperator kills background processes
- Spark stream started as background process via `docker exec nohup ... &`, killed by stop_stream task
- `wait_for_data` uses delta logic (CURRENT > INITIAL) to detect new rows
- Cleanup tasks use `trigger_rule=ALL_DONE` — no orphaned processes
- Airflow Dockerfile switched from `uv pip install --system` to `pip install` as airflow user
- Spark Dockerfile + entrypoint.sh for checkpoint volume permissions
- Kafka mount renamed from `/opt/airflow/kafka` to `/opt/airflow/kafka_scripts` to avoid package shadowing

## Errors hit (9 total)

1. kafka-python not installed — image never rebuilt after Phase 2.3
2. uv pip install fails on kafka-python — permission denied creating kafka dir
3. pip as root refused — apache/airflow image guard
4. Volume mount shadows kafka-python package — namespace package collision
5. Spark checkpoint mkdir fails — named volume root ownership
6. start_producer fails — nohup process dies, head can't find log
7. stop_producer fails — kill with && short-circuits cleanup
8. wait_for_data times out — producer died, no new Kafka messages
9. DAG stuck in queued — orphaned task instances from failed runs

## DAG ordering issue discovered

- crime_batch must run before divvy_stream on cold start
- dim_date.sql UNION ALLs both sources — both DAGs run full dbt build
- crime_batch's dbt_build fails on stg_station_status (expected, non-blocking)
- divvy_stream's dbt_build succeeds (both raw tables exist)
- Fix for Phase 3: split dbt build by selector per DAG

## Files changed

- `airflow/dags/divvy_stream_dag.py` — created
- `airflow/Dockerfile` — pip install as airflow user
- `spark/Dockerfile` — entrypoint + USER root
- `spark/entrypoint.sh` — created
- `docker-compose.yml` — renamed kafka mount
- `changelog.md` — Phase 2.6 entry
- `docs/operations-performed.md` — Phase 2.6 entry
- `docs/phases/phase-2.6-airflow-stream-dag.md` — created
- `docs/phases/README.md` — index updated
- `chat-history/current-state.md` — Phase 2 COMPLETE, Phase 3 next
