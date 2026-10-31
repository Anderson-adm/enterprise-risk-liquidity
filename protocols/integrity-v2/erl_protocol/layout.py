"""Object key layout of the lake (identical for the local directory store and S3).

    landing/db/<source>/<entity>/reference_date=<D>/<run_id>.parquet|.json
    landing/files/<source>/delivery_date=<D>/<run_id>/<file name>
    bronze/raw/<source>/bronze_reference_date=<D>/<run_id>/<original file name>
    bronze/structured/<source>/<entity>/bronze_reference_date=<D>/<run_id>[suffix].parquet
    manifests/ingestion/<source>/<entity>/bronze_reference_date=<D>/<run_id>__<object>.json
    quarantine/<source>/bronze_reference_date=<D>/<run_id>/<file>  (+ <file>.rejection.json)

Every key contains the run id, so a re-run writes new objects instead of replacing old ones.
"""

from __future__ import annotations

import re
from datetime import date

from erl_protocol.contracts import PARTITION_COLUMN, BronzeEntity


def partition(d: date) -> str:
    return f"{PARTITION_COLUMN}={d.isoformat()}"


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)[:150]


def landing_db_prefix(source: str, entity: str, ref: date) -> str:
    return f"landing/db/{source}/{entity}/reference_date={ref.isoformat()}/"


def landing_file_key(source: str, ref: date, run_id: str, name: str) -> str:
    return f"landing/files/{source}/delivery_date={ref.isoformat()}/{run_id}/{name}"


def raw_key(source: str, ref: date, run_id: str, name: str) -> str:
    return f"bronze/raw/{source}/{partition(ref)}/{run_id}/{safe_name(name)}"


def bronze_key(entity: BronzeEntity, ref: date, run_id: str, suffix: str = "") -> str:
    return (
        f"bronze/structured/{entity.source_system}/{entity.entity}/{partition(ref)}/{run_id}{suffix}.parquet"
    )


def bronze_prefix(source: str, entity: str, ref: date) -> str:
    return f"bronze/structured/{source}/{entity}/{partition(ref)}/"


def quarantine_key(source: str, ref: date, run_id: str, name: str) -> str:
    return f"quarantine/{source}/{partition(ref)}/{run_id}/{safe_name(name)}"


def manifest_key(source: str, entity: str, ref: date, run_id: str, input_object: str) -> str:
    obj = safe_name(input_object.rsplit("/", 1)[-1])
    return f"manifests/ingestion/{source}/{entity}/{partition(ref)}/{run_id}__{obj}.json"
