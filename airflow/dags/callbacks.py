"""
Shared Airflow callbacks for the Chicago Crime + Divvy pipeline (Phase 3.3).

on_failure_callback is a Python callable that Airflow invokes when a task
fails (after all retries are exhausted). It receives a context dict with
the task instance, DAG run, exception, and other metadata.

This callback logs the failure with full context to the Airflow task logs
so it's visible in the UI alongside the task's own output. In a production
setup, this is where you'd send a Slack/email/PagerDuty alert — for this
learning project, structured logging is the observable artifact.

Used by both crime_batch and divvy_stream DAGs via default_args:
    default_args = {
        ...
        "on_failure_callback": on_failure_callback,
    }
"""

import logging

logger = logging.getLogger(__name__)


def on_failure_callback(context):
    """Log a structured failure message when a task exhausts its retries.

    Called by Airflow in the scheduler process after a task's final retry
    fails. The context dict contains:
        - context["task_instance"]: the TaskInstance object
        - context["dag_run"]: the DagRun object
        - context["exception"]: the exception that caused the failure
        - context["ti"]: shorthand for task_instance
    """
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = ti.dag_id if ti else "<unknown>"
    task_id = ti.task_id if ti else "<unknown>"
    run_id = dag_run.run_id if dag_run else "<unknown>"
    try_number = ti.try_number if ti else 0

    # Log with full context — this appears in the Airflow task logs (UI + file)
    logger.error(
        "TASK FAILED — dag=%s task=%s run=%s try=%s exception=%s",
        dag_id,
        task_id,
        run_id,
        try_number,
        repr(exception),
    )

    # Also print so it's visible in the scheduler logs too
    print(
        f"[on_failure_callback] TASK FAILED: dag={dag_id} task={task_id} "
        f"run={run_id} try={try_number} exception={repr(exception)}"
    )
