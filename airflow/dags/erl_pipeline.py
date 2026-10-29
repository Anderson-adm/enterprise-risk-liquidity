"""Case 01 - Enterprise Risk & Liquidity pipeline (manual trigger, one reference date per run).

Each task runs one step of the project CLI (`erl step <name>`); all business logic lives in
the `erl` package, not here. The run id is created once and shared through XCom, so a retry
of any task re-enters the same run (Bronze keys, run state and publication are run-scoped and
idempotent).

Retries are proportional to the failure type:
  * extraction / collection / ingestion: transient infrastructure errors -> 2 retries
  * transform / load / publish: 1 retry (deterministic; publication is idempotent)
  * quality gate: never retried (a data problem does not fix itself)
"""

from __future__ import annotations

import os
import shlex
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

REPO = Path(__file__).resolve().parents[2]
ERL = os.environ.get("ERL_BIN", str(REPO / ".venv" / "bin" / "erl"))
RUN_ID = "{{ ti.xcom_pull(task_ids='new_run_id') }}"
REF = "{{ params.reference_date }}"


def notify_failure(context) -> None:
    """Alert with the exact failed step (SNS in aws mode, structured log locally)."""
    ti = context["task_instance"]
    run_id = ti.xcom_pull(task_ids="new_run_id") or "unknown"
    cmd = [
        ERL,
        "notify",
        "--step",
        ti.task_id,
        "--run-id",
        run_id,
        "--date",
        str(context["params"]["reference_date"]),
        "--airflow-run",
        context["run_id"],
    ]
    subprocess.run(cmd, check=False, timeout=60)  # noqa: S603 - fixed argv


def step(name: str, retries: int, timeout_min: int = 15) -> BashOperator:
    return BashOperator(
        task_id=name,
        bash_command=f"{shlex.quote(ERL)} step {name} --date {REF} --run-id {RUN_ID}",
        retries=retries,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=timeout_min),
        on_failure_callback=notify_failure,
    )


with DAG(
    dag_id="erl_risk_liquidity_pipeline",
    description="Seven sources -> Bronze -> dbt Silver/Gold -> quality gate -> SQL Server publication",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "reference_date": Param(
            "2026-10-26", type="string", format="date", description="Business reference date (YYYY-MM-DD)"
        )
    },
    default_args={"owner": "data-platform", "depends_on_past": False},
    tags=["case01", "risk", "liquidity"],
) as dag:
    new_run_id = BashOperator(
        task_id="new_run_id", bash_command=f"{shlex.quote(ERL)} new-run-id", do_xcom_push=True, retries=0
    )
    validate = step("validate", retries=0, timeout_min=2)
    extract = step("extract", retries=2)
    collect = step("collect", retries=2, timeout_min=5)
    ingest = step("ingest", retries=2)
    transform = step("transform", retries=1, timeout_min=30)
    gate = step("gate", retries=0, timeout_min=2)
    load = step("load", retries=1)
    publish = step("publish", retries=1)
    report = BashOperator(
        task_id="report",
        bash_command=f"{shlex.quote(ERL)} step report --date {REF} --run-id {RUN_ID}",
        trigger_rule="all_done",  # evidence is produced for failed runs too
        retries=0,
    )

    steps = [validate, extract, collect, ingest, transform, gate, load, publish]
    # `report` always runs (all_done), so it cannot decide the DagRun state. This watcher is a
    # leaf that only runs - and fails - when an upstream step failed, marking the run failed.
    watcher = BashOperator(
        task_id="fail_run_if_any_step_failed",
        bash_command="echo 'a pipeline step failed; see the step log, run report and alert' && exit 1",
        trigger_rule="one_failed",
        retries=0,
    )

    new_run_id >> validate >> extract >> collect >> ingest >> transform >> gate >> load >> publish >> report
    [new_run_id, *steps] >> watcher
