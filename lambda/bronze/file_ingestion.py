"""AWS Lambda entry points for the file-delivered sources (one function per source). Thin:
validate the event, build the S3 store, call ``ingestion.files.ingest_file`` - the same code
the local pipeline runs. Only the object store differs.

Event (sent by the pipeline with lambda:InvokeFunction, RequestResponse)::

    {"bucket": "...", "key": "landing/files/<source>/delivery_date=<D>/<run_id>/<name>",
     "run_id": "...", "reference_date": "YYYY-MM-DD"}

Package: scripts/build/build_artifacts.py -> dist/lambda/bronze/file_ingestion.zip, with this
module at the archive root (handler ``file_ingestion.<source>_handler``), the project packages
and PyArrow/lxml pinned to uv.lock. No Lambda layer: the AWS-managed pandas layer's PyArrow
could not write the contract's Parquet codec (AWS attempt of 2026-10-30).
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict
from datetime import date

import boto3

from ingestion import files
from storage.objects import S3Store
from utils import structured_log
from utils.contracts import parquet_compression
from utils.run_ids import validate_run_id

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
    )
    return {"manifest": asdict(manifest)}


def market_data_handler(event: dict, context) -> dict:
    return _handle("market_data", event, context)


def internal_xml_handler(event: dict, context) -> dict:
    return _handle("internal_xml", event, context)


def external_xml_handler(event: dict, context) -> dict:
    return _handle("external_xml", event, context)
