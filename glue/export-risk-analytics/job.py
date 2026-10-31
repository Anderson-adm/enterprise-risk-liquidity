"""Glue 5.0 export-risk-analytics: Iceberg Gold -> committed publication package.

Own Spark implementation in spark/unit_spark.py; pinned integrity implementation in this
unit's lib.zip. Unit-local SDK requirements. Implemented; no real Spark/Glue execution claimed.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date


def export_product(
    spark, store, table: str, columns: list[str], ref: date, run_id: str, max_rows: int, key: str
):
    # Spark imports stay local so this module imports (and is unit-tested) without a JVM.
    from unit_spark import read_table_rows, write_parquet

    from erl_protocol.errors import PublicationError

    df = read_table_rows(spark, table, {"reference_date": ref, "pipeline_run_id": run_id}, columns)
    rows = df.count()
    if rows > max_rows:
        raise PublicationError(f"{table}: {rows} rows exceed --max_product_rows={max_rows}")
    tmp = f"tmp/glue/{run_id}/publish/{key.rsplit('/', 1)[-1].removesuffix('.parquet')}/"
    write_parquet(df.orderBy("portfolio_id"), store.uri(tmp), files=1, shuffle=False)  # bounded above
    part = next(k for k in store.list(tmp) if k.endswith(".parquet"))
    store.copy_if_absent(part, key)
    store.delete_prefix(tmp)
    return {"object": key, "sha256": store.sha256_hex(key), "rows": rows}


def main(argv: list[str]) -> None:
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    from erl_protocol import publication as package_manifest
    from erl_protocol import structured_log
    from erl_protocol.contracts import consumer_contract
    from erl_protocol.objects import S3Store

    args = getResolvedOptions(
        argv,
        [
            "JOB_NAME",
            "bucket",
            "run_id",
            "reference_date",
            "quality_gate",
            "gold_database",
            "max_product_rows",
        ],
    )
    structured_log.configure("INFO")
    gate = json.loads(args["quality_gate"])
    package_manifest.require_passed_gate(gate)
    ref, run_id = date.fromisoformat(args["reference_date"]), args["run_id"]
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    store = S3Store(args["bucket"], client=boto3.client("s3"))
    products = {}
    for name, spec in consumer_contract()["products"].items():
        products[name] = export_product(
            spark,
            store,
            f"glue_catalog.{args['gold_database']}.{spec['gold_model']}",
            [c["name"] for c in spec["columns"]],
            ref,
            run_id,
            int(args["max_product_rows"]),
            package_manifest.product_key(ref, run_id, name),
        )
    manifest = package_manifest.build_manifest(ref, run_id, f"glue:{args['JOB_NAME']}", gate, products)
    key = package_manifest.manifest_key(ref, run_id)
    store.put(key, json.dumps(manifest, indent=2).encode())  # written last = commit marker
    structured_log.event(
        logging.getLogger(__name__),
        "publication_package_built",
        run_id=run_id,
        reference_date=ref.isoformat(),
        rows={k: v["rows"] for k, v in products.items()},
    )


if __name__ == "__main__":
    main(sys.argv)
