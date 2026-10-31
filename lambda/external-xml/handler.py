"""external-xml Lambda entry point (handler.handler).

Own parser and pinned native dependencies; the versioned integrity protocol accepts/commits
its result. Public event: bucket, landing key, run_id, reference_date. No remote call on import.
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict
from datetime import date

import boto3
from external_xml import parse

from erl_protocol import file_acceptance as files
from erl_protocol import structured_log
from erl_protocol.contracts import parquet_compression
from erl_protocol.objects import S3Store
from erl_protocol.run_ids import validate_run_id

logger = logging.getLogger(__name__)


def _handle(source_system: str, event: dict, context) -> dict:
    structured_log.configure(os.environ.get("LOG_LEVEL", "INFO"))
    bucket, key = event["bucket"], event["key"]
    if not key.startswith(f"landing/files/{source_system}/"):
        raise ValueError(f"{key} is not a {source_system} landing object")
    if bucket != os.environ["ERL_S3_BUCKET"]:
        raise ValueError("bucket outside this function's scope")
    run_id = validate_run_id(event["run_id"])
    store = S3Store(bucket, client=boto3.client("s3"))
    manifest = files.ingest_file(
        store,
        source_system=source_system,
        object_name=key.rsplit("/", 1)[-1],
        data=store.get(key),
        run_id=run_id,
        run_reference_date=date.fromisoformat(event["reference_date"]),
        component=f"lambda:{getattr(context, 'function_name', 'unknown')}",
        compression=parquet_compression(),
        parse=parse,
        entity_name="transfer_in_position",
    )
    return {"manifest": asdict(manifest)}


def handler(event: dict, context) -> dict:
    return _handle("external_xml", event, context)
