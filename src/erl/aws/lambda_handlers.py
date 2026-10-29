"""AWS Lambda entry points for the file-delivered sources (one function per source).

Thin wrappers: the validation and Bronze logic is ``erl.ingestion.files.ingest_file``, the
same code path exercised locally and in tests. Only the object store differs (S3).

Event (sent by the pipeline with lambda:InvokeFunction, RequestResponse)::

    {"bucket": "...", "key": "landing/files/<source>/delivery_date=<D>/<run_id>/<name>",
     "run_id": "...", "reference_date": "YYYY-MM-DD"}

Packaging: function code = ``erl`` package + lxml; pyarrow comes from the AWS-managed
"AWS SDK for pandas" layer (see infra/environments/dev/lambda.tf).
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict
from datetime import date

from erl import log
from erl.ingestion import bronze, files
from erl.storage import S3Store

log.configure(os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def _handle(source_system: str, event: dict, context) -> dict:
    bucket, key = event["bucket"], event["key"]
    expected_prefix = f"landing/files/{source_system}/"
    if not key.startswith(expected_prefix):
        raise ValueError(f"{key} is not a {source_system} landing object")
    if bucket != os.environ["ERL_S3_BUCKET"]:
        raise ValueError("bucket outside this function's scope")
    run_id = bronze.validate_run_id(event["run_id"])
    store = S3Store(bucket)
    manifest = files.ingest_file(
        store,
        source_system=source_system,
        object_name=key.rsplit("/", 1)[-1],
        data=store.get(key),
        run_id=run_id,
        run_reference_date=date.fromisoformat(event["reference_date"]),
        component=f"lambda:{getattr(context, 'function_name', 'unknown')}",
    )
    return {"manifest": asdict(manifest)}


def market_data_handler(event: dict, context) -> dict:
    return _handle("market_data", event, context)


def internal_xml_handler(event: dict, context) -> dict:
    return _handle("internal_xml", event, context)


def external_xml_handler(event: dict, context) -> dict:
    return _handle("external_xml", event, context)
