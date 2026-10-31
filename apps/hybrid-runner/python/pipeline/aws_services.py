"""Hybrid AWS mode: the local pipeline drives AWS services with temporary SSO credentials.

Sources stay local. The pipeline uploads landing objects to S3 (via ``S3Store``), then:

* invokes the ingestion Lambdas (market data, internal XML, external XML) once per file;
* starts ``extract-liquidez`` / ``extract-enquadramento`` (GB / TB profiles) for database snapshots;
* runs dbt on Athena (Iceberg Silver/Gold) — see ``pipeline.step_transform``;
* starts the Glue ``export-risk-analytics`` job, which writes the publication package to S3;
* the local publisher then loads SQL Server from that package (AWS never connects to localhost).

Sessions come from ``connect.aws`` (explicit SSO/runner profile, sa-east-1 only).

Status: implemented; one Lambda invocation ran on AWS on 2026-10-30 and failed on the managed
layer's Parquet codec (fixed locally since, not re-run). Glue/Athena never ran. See
docs/AWS_VALIDATION.md.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date

from erl_protocol import structured_log
from erl_protocol.contracts import bronze_entity
from erl_protocol.errors import InfrastructureError
from erl_protocol.layout import partition
from erl_protocol.objects import S3Store
from pipeline.config import Settings

logger = logging.getLogger(__name__)

LAMBDA_FOR_SOURCE = {
    "market_data": "ingest-market-data",
    "internal_xml": "ingest-internal-xml",
    "external_xml": "ingest-external-xml",
}
GLUE_FOR_PATTERN = {"glue_db_gb": "extract-liquidez", "glue_db_tb": "extract-enquadramento"}
GLUE_POLL_SECONDS = 15
GLUE_TIMEOUT_SECONDS = 20 * 60


def ingest_on_aws(
    settings: Settings, ref: date, run_id: str, landing_db: list[str], landing_files: list[str]
) -> list[dict]:
    session = settings.aws_session()
    manifests: list[dict] = []
    lam = session.client("lambda")
    for key in landing_files:
        source = key.split("/")[2]
        fn = f"{settings.name_prefix}-{LAMBDA_FOR_SOURCE[source]}"
        payload = {
            "bucket": settings.s3_bucket,
            "key": key,
            "run_id": run_id,
            "reference_date": ref.isoformat(),
        }
        resp = lam.invoke(
            FunctionName=fn, InvocationType="RequestResponse", Payload=json.dumps(payload).encode()
        )
        body = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            raise InfrastructureError(f"Lambda {fn} failed for {key}: {body}")
        manifests.append(body["manifest"])
        structured_log.event(
            logger,
            "lambda_ingestion",
            run_id=run_id,
            function=fn,
            object=key,
            status=body["manifest"]["status"],
        )

    by_pattern: dict[str, list[str]] = {}
    for key in landing_db:
        source, entity = key.split("/")[2:4]
        by_pattern.setdefault(bronze_entity(source, entity).ingestion_pattern, []).append(key)
    glue = session.client("glue")
    for pattern, keys in sorted(by_pattern.items()):
        job = f"{settings.name_prefix}-{GLUE_FOR_PATTERN[pattern]}"
        run = glue.start_job_run(
            JobName=job,
            Arguments={
                "--bucket": settings.s3_bucket,
                "--run_id": run_id,
                "--landing_manifests": ",".join(keys),
            },
        )
        _wait_glue(glue, job, run["JobRunId"], run_id)
    # Glue writes ingestion manifests to S3; read them back for the run report
    store = S3Store(settings.s3_bucket, client=session.client("s3"))
    for key in landing_db:
        source, entity = key.split("/")[2:4]
        prefix = f"manifests/ingestion/{source}/{entity}/{partition(ref)}/{run_id}__"
        manifests += [json.loads(store.get(k)) for k in store.list(prefix)]
    return manifests


def _wait_glue(glue, job: str, job_run_id: str, run_id: str) -> None:
    deadline = time.monotonic() + GLUE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        state = glue.get_job_run(JobName=job, RunId=job_run_id)["JobRun"]
        status = state["JobRunState"]
        if status == "SUCCEEDED":
            structured_log.event(
                logger,
                "glue_job_succeeded",
                run_id=run_id,
                job=job,
                job_run_id=job_run_id,
                execution_time=state.get("ExecutionTime"),
            )
            return
        if status in {"FAILED", "ERROR", "TIMEOUT", "STOPPED"}:
            raise InfrastructureError(
                f"Glue job {job} run {job_run_id} {status}: {state.get('ErrorMessage')}"
            )
        time.sleep(GLUE_POLL_SECONDS)
    raise InfrastructureError(f"Glue job {job} run {job_run_id} did not finish in {GLUE_TIMEOUT_SECONDS}s")


def run_gold_export(settings: Settings, ref: date, run_id: str, gate: dict) -> str:
    glue = settings.aws_session().client("glue")
    job = f"{settings.name_prefix}-export-risk-analytics"
    run = glue.start_job_run(
        JobName=job,
        Arguments={
            "--bucket": settings.s3_bucket,
            "--run_id": run_id,
            "--reference_date": ref.isoformat(),
            "--quality_gate": json.dumps(gate),
        },
    )
    _wait_glue(glue, job, run["JobRunId"], run_id)
    return f"publish/reference_date={ref.isoformat()}/{run_id}/manifest.json"
