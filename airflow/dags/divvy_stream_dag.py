"""
Phase 2.6 — Divvy Streaming Pipeline DAG

Orchestrates the full streaming lifecycle:
  0. create_topic    — create Kafka topic with 3 partitions (idempotent)
  1. start_producer  — start Kafka producer (continuous, background process)
  2. start_stream    — start Spark Structured Streaming (continuous, background)
  3. wait_for_data   — poll Postgres until new rows arrive in raw.station_status
  4. dbt_build       — refresh staging + marts (stg_station_status, fact_station_reads)
  5. stop_stream     — stop Spark streaming job (cleanup, runs even on failure)
  6. stop_producer   — stop Kafka producer (cleanup, runs even on failure)

Why background processes instead of run-to-completion tasks?
  Streaming jobs are long-running — they don't "complete" like batch jobs.
  Airflow tasks are designed for run-to-completion work. So we start the
  producer and Spark stream as detached background processes, wait for data
  to flow through, run DBT to refresh marts, then clean up.

  This demonstrates the full streaming lifecycle: start → monitor → transform → stop.
  For truly continuous 24/7 streaming, run the producer and Spark job outside
  Airflow (e.g., as Docker services or supervisor-managed processes) and use
  a separate monitoring DAG with sensors (Phase 3 territory).

Producer runs inside the Airflow container:
  - Script at /opt/airflow/kafka/producers/divvy_producer.py (bind-mounted)
  - kafka-python installed in Airflow image (airflow/requirements.txt)
  - Reaches Kafka at kafka:9092 (Docker network)

Spark stream runs inside spark-master container:
  - docker exec spark-master → spark-submit divvy_stream.py
  - KAFKA_BOOTSTRAP_SERVERS=kafka:9092 set in docker-compose.yml
  - Checkpoint in spark_checkpoints named volume (persists Kafka offsets)

DBT runs in a separate container:
  - dbt-core 1.11's protobuf >=6.0 conflicts with Airflow 3.0's protobuf 4.x
  - --volumes-from shares Airflow's bind mounts (./dbt, ./dbt_profiles, ./data)
  - --network connects to compose network so dbt can reach Postgres

Cleanup guarantee:
  stop_stream and stop_producer use trigger_rule=ALL_DONE — they run even if
  upstream tasks fail. This ensures no orphaned background processes remain
  after a DAG run, regardless of where the pipeline failed.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from callbacks import on_failure_callback

# ============================================================
# Configuration
# ============================================================

# Spark container — resolved at runtime by name pattern.
# Avoids hardcoding the project prefix (chicago-data-pipeline-spark-master-1)
# which breaks if COMPOSE_PROJECT_NAME changes.
SPARK_CONTAINER_CMD = "docker ps -qf name=spark-master --filter status=running | head -1"

# Paths inside the Airflow container
PRODUCER_SCRIPT = "/opt/airflow/kafka_scripts/producers/divvy_producer.py"
DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

# Kafka bootstrap — Docker service name (inside the network)
KAFKA_BOOTSTRAP = "kafka:9092"

# PID files for background process management
PRODUCER_PID_FILE = "/tmp/divvy_producer.pid"
STREAM_PID_FILE = "/tmp/divvy_stream.pid"

# Log files for background processes (useful for debugging)
PRODUCER_LOG = "/tmp/divvy_producer.log"
STREAM_LOG = "/tmp/divvy_stream.log"

# DBT image — separate from Airflow (protobuf conflict, see crime_batch_dag)
DBT_IMAGE = "chicago-data-pipeline-dbt:latest"

# ============================================================
# Default args
# ============================================================

default_args = {
    "owner": "chicago-pipeline",
    "retries": 3,  # Phase 3.3 — transient failures (Kafka blips, Spark startup) benefit from retries
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,  # no email server; on_failure_callback logs instead
    "depends_on_past": False,
    "on_failure_callback": on_failure_callback,  # Phase 3.3 — structured failure logging
}

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="divvy_stream",
    default_args=default_args,
    schedule=None,  # manual trigger — streaming lifecycle, not a scheduled job
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["divvy", "stream", "phase-2"],
    max_active_runs=1,  # don't overlap — one streaming lifecycle at a time
    doc_md=__doc__,
) as dag:

    # 0. Create Kafka topic — divvy_station_status with 3 partitions.
    #    Auto-create (KAFKA_AUTO_CREATE_TOPICS_ENABLE=true) gives only 1
    #    partition. We need 3 so station_id key partitioning spreads load
    #    (same station_id → same partition → ordered processing per station).
    #    --if-not-exists makes this idempotent — safe to re-run.
    create_topic = BashOperator(
        task_id="create_topic",
        bash_command=(
            'docker exec chicago-data-pipeline-kafka-1 '
            'kafka-topics --bootstrap-server kafka:9092 '
            '--create --if-not-exists '
            '--topic divvy_station_status '
            '--partitions 3 '
            '--replication-factor 1 && '
            'docker exec chicago-data-pipeline-kafka-1 '
            'kafka-topics --bootstrap-server kafka:9092 '
            '--describe --topic divvy_station_status'
        ),
    )

    # 1. Run Kafka producer once — polls GBFS, publishes ~2,016 station
    #    status messages to the divvy_station_status topic, then exits.
    #
    #    We use --once mode (single poll) instead of continuous mode because
    #    Airflow's BashOperator kills background processes when the task's
    #    shell exits. nohup + disown don't reliably survive — the producer
    #    dies before its second poll. With --once, the producer runs in the
    #    foreground, publishes one batch, and exits cleanly. The Spark stream
    #    (started next) consumes these messages on its 60s trigger.
    #
    #    For truly continuous 24/7 streaming, run the producer as a separate
    #    Docker service or supervisor-managed process (Phase 3 territory).
    start_producer = BashOperator(
        task_id="start_producer",
        bash_command=(
            f'python {PRODUCER_SCRIPT} '
            f'--bootstrap {KAFKA_BOOTSTRAP} --once'
        ),
    )

    # 2. Start Spark Structured Streaming — consumes from Kafka, cleans data,
    #    writes to Postgres raw.station_status via foreachBatch.
    #
    #    Runs as a detached background process inside the spark-master container.
    #    The checkpoint (spark_checkpoints volume) stores Kafka offsets — on
    #    restart, the stream resumes from the last committed offset and only
    #    processes NEW messages.
    #
    #    The 60s trigger means the first micro-batch fires ~60s after start.
    #    We sleep 3s here just to confirm Spark initialized without crashing.
    start_stream = BashOperator(
        task_id="start_stream",
        bash_command=(
            'SPARK_CID=$(' + SPARK_CONTAINER_CMD + ') && '
            'if [ -z "$SPARK_CID" ]; then echo "ERROR: spark-master container not running"; exit 1; fi && '
            # Check if stream is already running from a previous run
            f'docker exec "$SPARK_CID" bash -c '
            f'"if [ -f {STREAM_PID_FILE} ] && kill -0 \\$(cat {STREAM_PID_FILE}) 2>/dev/null; then '
            f'  echo \\"Stream already running — skipping\\"; exit 0; fi" && '
            # Start Spark streaming job in background inside spark-master
            f'docker exec "$SPARK_CID" bash -c '
            f'"nohup /opt/spark/bin/spark-submit --master local[*] '
            f'/opt/spark/jobs/divvy_stream.py --bootstrap {KAFKA_BOOTSTRAP} '
            f'> {STREAM_LOG} 2>&1 & echo \\$! > {STREAM_PID_FILE}" && '
            f'echo "Spark stream started" && '
            # Give Spark time to initialize (SparkSession + Kafka reader setup)
            f'sleep 5 && '
            # Show startup logs for debugging (|| true so missing log doesn't fail task)
            f'echo "--- Spark stream logs ---" && '
            f'(docker exec "$SPARK_CID" head -10 {STREAM_LOG} || true)'
        ),
    )

    # 3. Wait for data — poll Postgres until NEW rows appear in raw.station_status.
    #
    #    The Spark stream's 60s trigger means the first micro-batch fires ~60s
    #    after start_stream. We poll every 15s with a 5-minute timeout (20 polls).
    #
    #    We capture the initial row count, then poll until CURRENT > INITIAL.
    #    This ensures we wait for the stream to process the NEW messages from
    #    the producer's --once poll, not just detect pre-existing data.
    #
    #    This is the "monitor" step: we confirm data is actually flowing through
    #    the pipeline before running DBT. If no new data arrives in 5 minutes,
    #    something is wrong (producer didn't poll, stream didn't start, Kafka
    #    connection failed, etc.) and the DAG fails — but cleanup still runs.
    wait_for_data = BashOperator(
        task_id="wait_for_data",
        bash_command=(
            # Table may not exist yet on first run — treat as 0 rows.
            'INITIAL=$(docker exec chicago-data-pipeline-postgres-1 '
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A '
            '-c "SELECT COUNT(*) FROM raw.station_status" 2>/dev/null || echo 0) && '
            'echo "Initial row count: $INITIAL" && '
            'for i in $(seq 1 20); do '
            '  sleep 15 && '
            '  CURRENT=$(docker exec chicago-data-pipeline-postgres-1 '
            '  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A '
            '  -c "SELECT COUNT(*) FROM raw.station_status" 2>/dev/null || echo 0) && '
            '  echo "[$i/20] Current: $CURRENT rows" && '
            '  if [ "$CURRENT" -gt "$INITIAL" ]; then '
            '    echo "New data detected: +$((CURRENT - INITIAL)) rows" && exit 0; '
            '  fi; '
            'done && '
            'echo "ERROR: No new data after 5 minutes" && exit 1'
        ),
    )
    #    All 52 tests run as part of dbt build.
    #    execution_timeout: 30 minutes (Phase 3.3). Airflow 3.0 removed the
    #    sla= parameter (deprecation warning, no-op). execution_timeout actually
    #    fails the task if it exceeds the limit. dbt build typically <10min.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            'docker run --rm '
            '--network chicago-data-pipeline_default '
            f'--volumes-from $HOSTNAME '
            '-e POSTGRES_USER '
            '-e POSTGRES_PASSWORD '
            '-e POSTGRES_DB '
            f'{DBT_IMAGE} '
            f'dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
        ),
        execution_timeout=timedelta(minutes=30),  # Phase 3.3 — replaces removed sla=
    )
    # 4b. Record dbt test results into Postgres for Grafana observability
    #     (Phase 3.2). Same recorder as crime_batch_dag — parses the
    #     run_results.json written by dbt_build and upserts rows into
    #     observability.dbt_test_results. Default trigger_rule (ALL_SUCCESS):
    #     only records when dbt_build completes; cleanup tasks still run via
    #     ALL_DONE downstream.
    record_dbt_results = BashOperator(
        task_id="record_dbt_results",
        bash_command="python /opt/airflow/scripts/record_dbt_results.py",
    )

    # 5. Stop Spark streaming job — cleanup.
    stop_stream = BashOperator(
        task_id="stop_stream",
        bash_command=(
            'SPARK_CID=$(' + SPARK_CONTAINER_CMD + ') && '
            'if [ -n "$SPARK_CID" ]; then '
            f'  docker exec "$SPARK_CID" bash -c '
            f'  "kill $(cat {STREAM_PID_FILE}) 2>/dev/null; rm -f {STREAM_PID_FILE}"; '
            '  echo "Spark stream stopped"; '
            'else '
            '  echo "spark-master not running — nothing to stop"; '
            'fi'
        ),
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,  # cleanup — don't retry. If kill fails, the process is already gone.
    )

    # 6. Stop Kafka producer — cleanup.
    #
    #    In --once mode the producer exits on its own after one poll, so there's
    #    no background process to kill. This task is a no-op kept for structural
    #    symmetry with stop_stream and as a safety net if we switch back to
    #    continuous mode later.
    stop_producer = BashOperator(
        task_id="stop_producer",
        bash_command=(
            f'if [ -f {PRODUCER_PID_FILE} ]; then '
            f'  kill $(cat {PRODUCER_PID_FILE}) 2>/dev/null; '
            f'  rm -f {PRODUCER_PID_FILE}; '
            '  echo "Producer stopped"; '
            'else '
            '  echo "Producer already exited (--once mode)"; '
            'fi'
        ),
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,  # cleanup — don't retry. No-op in --once mode anyway.
    )

    # Task dependencies — lifecycle pipeline:
    #   create topic → start → monitor → transform → cleanup
    #
    # Cleanup tasks (stop_stream, stop_producer) run regardless of upstream
    # success/failure via trigger_rule=ALL_DONE. This guarantees no orphaned
    # background processes remain after a DAG run.
    create_topic >> start_producer >> start_stream >> wait_for_data >> dbt_build
    dbt_build >> record_dbt_results >> stop_stream >> stop_producer
