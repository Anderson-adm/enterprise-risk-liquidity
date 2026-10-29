"""Glue 5.0 (Spark) job: database extraction snapshots (landing) -> Bronze.

Design boxes "Glue job - Volume em GB" and "Glue job - Volume em TB": the same script, two
job definitions (``--volume_profile gb|tb``). The TB profile spreads the write over several
part files; checksums are verified from S3's stored SHA-256 instead of re-reading the object.

Contract (identical to the local ``erl.ingestion.database.ingest_snapshot``):
  * verify the landing object against its extraction manifest;
  * skip snapshots whose content hash was already ACCEPTED (DUPLICATE manifest);
  * append metadata columns, write Bronze under a run-scoped key, never overwrite;
  * write an ingestion manifest per snapshot.

Status: implemented, NOT executed on AWS yet (see docs/AWS_VALIDATION.md).

Arguments: --bucket --run_id --landing_manifests (comma separated keys) --volume_profile
           --contract_s3_key
"""

import base64
import json
import re
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import functions as F

ARGS = getResolvedOptions(
    sys.argv, ["JOB_NAME", "bucket", "run_id", "landing_manifests", "volume_profile", "contract_s3_key"]
)
BUCKET = ARGS["bucket"]
s3 = boto3.client("s3")
spark = GlueContext(SparkContext.getOrCreate()).spark_session
ROWS_PER_PART = 1_000_000  # TB profile: target rows per Bronze part file


def log(event, **fields):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "event": event, "job": ARGS["JOB_NAME"], **fields}))


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def put_json_if_absent(key, payload):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(payload, indent=2).encode(), IfNoneMatch="*")


def exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def safe_name(name):
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)[:150]


def manifest_key(m):
    obj = safe_name(m["input_object"].rsplit("/", 1)[-1])
    return (f"manifests/ingestion/{m['source_system']}/{m['entity']}/"
            f"bronze_reference_date={m['reference_date']}/{m['run_id']}__{obj}.json")


def s3_sha256_hex(key):
    head = s3.head_object(Bucket=BUCKET, Key=key, ChecksumMode="ENABLED")
    checksum = head.get("ChecksumSHA256")
    if not checksum or "-" in checksum:  # absent or multipart composite: fall back to reading
        import hashlib

        return hashlib.sha256(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()).hexdigest()
    return base64.b64decode(checksum).hex()


def accepted_duplicate(source, entity, ref, content_sha):
    prefix = f"manifests/ingestion/{source}/{entity}/bronze_reference_date={ref}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            m = get_json(obj["Key"])
            if m["status"] == "ACCEPTED" and m["input_sha256"] == content_sha:
                return m
    return None


def ingest(landing_manifest_key, contract):
    lm = get_json(landing_manifest_key)
    source, entity, ref, run_id = lm["source_system"], lm["entity"], lm["reference_date"], lm["run_id"]
    spec = contract["entities"][f"{source}__{entity}"]
    base = {
        "run_id": run_id, "source_system": source, "entity": entity, "reference_date": ref,
        "component": f"glue:{ARGS['JOB_NAME']}", "input_object": lm["object"], "input_sha256": lm["content_sha256"],
        "records_read": 0, "records_written": 0, "records_rejected": 0, "bronze_object": None,
        "raw_object": lm["object"], "late_arrival": False, "duplicate_of_run": None,
        "rejection_reason": None, "rejection_detail": None,
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }
    if s3_sha256_hex(lm["object"]) != lm["object_sha256"]:
        raise RuntimeError(f"landing object {lm['object']} does not match its manifest checksum")

    dup = accepted_duplicate(source, entity, ref, lm["content_sha256"])
    if dup is not None:
        m = dict(base, status="DUPLICATE", records_read=lm["rows"], duplicate_of_run=dup["run_id"])
        put_json_if_absent(manifest_key(m), m)
        log("snapshot_duplicate_skipped", run_id=run_id, source_system=source, entity=entity, reference_date=ref)
        return m

    df = spark.read.parquet(f"s3://{BUCKET}/{lm['object']}")
    expected = spec["columns"] + ["meta_row_number", "meta_row_hash"]
    if df.columns != expected:
        raise RuntimeError(f"{source}__{entity}: landing columns {df.columns} != {expected}")

    meta = {
        "meta_source_system": source, "meta_entity": entity, "meta_run_id": run_id,
        "meta_ingested_at": base["ingested_at"], "meta_component": base["component"],
        "meta_source_object": lm["object"], "meta_source_sha256": lm["object_sha256"],
        "meta_snapshot_key": f"{source}/{entity}/{ref}",
    }
    for name, value in meta.items():
        df = df.withColumn(name, F.lit(value).cast("string"))
    order = spec["columns"] + [c["name"] for c in contract["metadata_columns"]]
    df = df.select(*[F.col(c).cast("bigint" if c == "meta_row_number" else "string").alias(c) for c in order])

    rows = lm["rows"]
    parts = 1 if ARGS["volume_profile"] == "gb" else max(1, -(-rows // ROWS_PER_PART))
    tmp_prefix = f"tmp/glue/{run_id}/{source}/{entity}/"
    df.repartition(parts).write.mode("errorifexists").parquet(f"s3://{BUCKET}/{tmp_prefix}")

    part_keys = sorted(
        o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=tmp_prefix).get("Contents", [])
        if o["Key"].endswith(".parquet")
    )
    target_dir = f"bronze/structured/{source}/{entity}/bronze_reference_date={ref}/"
    targets = []
    for i, key in enumerate(part_keys):
        target = target_dir + (f"{run_id}.parquet" if len(part_keys) == 1 else f"{run_id}-part-{i:05d}.parquet")
        if exists(target):  # append-only: a run never replaces Bronze objects
            raise RuntimeError(f"refusing to overwrite s3://{BUCKET}/{target}")
        s3.copy_object(Bucket=BUCKET, Key=target, CopySource={"Bucket": BUCKET, "Key": key})
        targets.append(target)
    for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=tmp_prefix).get("Contents", []):
        s3.delete_object(Bucket=BUCKET, Key=o["Key"])

    m = dict(base, status="ACCEPTED", records_read=rows, records_written=rows, bronze_object=",".join(targets))
    put_json_if_absent(manifest_key(m), m)
    log("snapshot_ingested", run_id=run_id, source_system=source, entity=entity, reference_date=ref,
        records_written=rows, parts=len(targets))
    return m


def main():
    contract = get_json(ARGS["contract_s3_key"])
    keys = [k for k in ARGS["landing_manifests"].split(",") if k]
    for key in keys:
        if f"/{ARGS['run_id']}.json" not in key:
            raise RuntimeError(f"{key} does not belong to run {ARGS['run_id']}")
        ingest(key, contract)


main()
