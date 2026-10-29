"""Hybrid AWS mode: the local pipeline drives AWS services with temporary SSO credentials.

Sources stay local. The pipeline uploads landing objects to S3 (via ``S3Store``), then:

* invokes the ingestion Lambdas (market data, internal XML, external XML) once per file;
* starts the Glue ``db_ingest`` jobs (GB / TB patterns) for the database snapshots;
* runs dbt on Athena (Iceberg Silver/Gold) — see ``pipeline.step_transform``;
* starts the Glue ``gold_load`` job, which writes the publication package to S3;
* the local publisher then loads SQL Server from that package (AWS never connects to localhost).

Status: implemented, NOT validated against AWS yet (requires an authorised apply; see
docs/AWS_VALIDATION.md).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date

import boto3

from erl import log
from erl.config import Settings
from erl.contracts import bronze_entity
from erl.errors import ConfigurationError, InfrastructureError

logger = logging.getLogger(__name__)

LAMBDA_FOR_SOURCE = {
    "market_data": "ingest-market-data",
    "internal_xml": "ingest-internal-xml",
    "external_xml": "ingest-external-xml",
}
GLUE_POLL_SECONDS = 15
GLUE_TIMEOUT_SECONDS = 20 * 60


def _session(settings: Settings) -> boto3.Session:
    profile = os.environ.get("AWS_PROFILE")
    if not profile or profile == "default":
        raise ConfigurationError("AWS_PROFILE must explicitly select a non-default SSO/runner profile")
    if settings.aws_region != "sa-east-1":
        raise ConfigurationError("AWS execution is restricted to sa-east-1")
    session = boto3.Session(profile_name=profile, region_name=settings.aws_region)
    credentials = session.get_credentials()
    if credentials is None or credentials.method not in {"sso", "assume-role"}:
        raise ConfigurationError("AWS credentials must come from SSO or the SSO-backed runner role")
    return session


def assert_expected_account(settings: Settings) -> dict:
    """Refuse to run against any account other than the configured data-dev account."""
    if not settings.aws_account_id:
        raise ConfigurationError("ERL_AWS_ACCOUNT_ID must be set in aws mode")
    identity = _session(settings).client("sts").get_caller_identity()
    if identity["Account"] != settings.aws_account_id:
        raise ConfigurationError(
            f"AWS credentials belong to account {identity['Account']}, expected {settings.aws_account_id}"
        )
    return identity


def ingest_on_aws(
    settings: Settings, ref: date, run_id: str, landing_db: list[str], landing_files: list[str]
) -> list[dict]:
    session = _session(settings)
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
        log.event(
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
        job = f"{settings.name_prefix}-{pattern.replace('glue_', '').replace('_', '-')}"
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
    from erl.storage import S3Store

    store = S3Store(settings.s3_bucket, client=session.client("s3"))
    for key in landing_db:
        source, entity = key.split("/")[2:4]
        prefix = f"manifests/ingestion/{source}/{entity}/bronze_reference_date={ref.isoformat()}/{run_id}__"
        manifests += [json.loads(store.get(k)) for k in store.list(prefix)]
    return manifests


def _wait_glue(glue, job: str, job_run_id: str, run_id: str) -> None:
    deadline = time.monotonic() + GLUE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        state = glue.get_job_run(JobName=job, RunId=job_run_id)["JobRun"]
        status = state["JobRunState"]
        if status == "SUCCEEDED":
            log.event(
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


def run_gold_load(settings: Settings, ref: date, run_id: str, gate: dict) -> str:
    glue = _session(settings).client("glue")
    job = f"{settings.name_prefix}-gold-load"
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
