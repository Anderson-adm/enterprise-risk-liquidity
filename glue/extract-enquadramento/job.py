"""Glue 5.0 extract-enquadramento: landing snapshots -> committed Bronze.

Own Spark implementation in spark/unit_spark.py; pinned integrity implementation in this
unit's lib.zip. Unit-local SDK requirements. Implemented; no real Spark/Glue execution claimed.
"""

from __future__ import annotations

import sys

ROWS_PER_PART = 1_000_000  # TB profile: target rows per Bronze part file


def parts_for(volume_profile: str, rows: int) -> int:
    if volume_profile not in {"gb", "tb"}:
        raise ValueError(f"unknown volume profile {volume_profile!r}")
    return 1 if volume_profile == "gb" else max(1, -(-rows // ROWS_PER_PART))


def ingest(spark, store, landing_manifest_key: str, component: str, volume_profile: str):
    # Spark/Glue imports stay local so this module imports (and is unit-tested) without a JVM.
    from unit_spark import read_parquet, select_in_order, single_row, with_constant_columns, write_parquet

    from erl_protocol import layout
    from erl_protocol.clock import utc_now_iso
    from erl_protocol.contracts import bronze_entity, metadata_columns
    from erl_protocol.errors import ContractError
    from erl_protocol.snapshot_manifests import (
        LandingSnapshot,
        bronze_metadata,
        completed_snapshot,
        landing_object_sha256,
        record_accepted,
        record_duplicate,
        verify_landing_checksum,
    )

    snapshot = LandingSnapshot.read(store, landing_manifest_key)
    entity = bronze_entity(snapshot.source_system, snapshot.entity)
    if completed := completed_snapshot(store, snapshot):
        return completed
    verify_landing_checksum(snapshot, landing_object_sha256(store, snapshot))
    if duplicate := record_duplicate(store, snapshot, component, snapshot.rows):
        return duplicate

    df = read_parquet(spark, store.uri(snapshot.object))
    expected = [*entity.columns, "meta_row_number", "meta_row_hash"]
    if df.columns != expected:
        raise ContractError(f"{entity.table}: landing columns {df.columns} != {expected}")
    if snapshot.rows == 0:  # empty-snapshot marker, as in the local writer
        df = single_row(spark, df.schema, [None] * len(entity.columns) + [0, None])
    df = with_constant_columns(df, bronze_metadata(snapshot, component, utc_now_iso()))
    df = select_in_order(df, [*entity.columns, *metadata_columns()], frozenset({"meta_row_number"}))

    tmp = f"tmp/glue/{snapshot.run_id}/{snapshot.source_system}/{snapshot.entity}/"
    write_parquet(df, store.uri(tmp), files=parts_for(volume_profile, snapshot.rows), shuffle=True)
    part_keys = [k for k in store.list(tmp) if k.endswith(".parquet")]
    target_dir = layout.bronze_prefix(snapshot.source_system, snapshot.entity, snapshot.reference_date)
    targets = []
    for i, key in enumerate(part_keys):
        name = (
            f"{snapshot.run_id}.parquet" if len(part_keys) == 1 else f"{snapshot.run_id}-part-{i:05d}.parquet"
        )
        targets.append(target_dir + name)
        store.copy_if_absent(key, target_dir + name)
    store.delete_prefix(tmp)
    return record_accepted(store, snapshot, component, targets, snapshot.rows)


def main(argv: list[str]) -> None:
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    from erl_protocol import structured_log
    from erl_protocol.objects import S3Store

    args = getResolvedOptions(argv, ["JOB_NAME", "bucket", "run_id", "landing_manifests", "volume_profile"])
    structured_log.configure("INFO")
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    store = S3Store(args["bucket"], client=boto3.client("s3"))
    for key in [k for k in args["landing_manifests"].split(",") if k]:
        if f"/{args['run_id']}.json" not in key:
            raise ValueError(f"{key} does not belong to run {args['run_id']}")
        ingest(spark, store, key, f"glue:{args['JOB_NAME']}", args["volume_profile"])


if __name__ == "__main__":
    main(sys.argv)
