"""Glue 5.0 (Spark + Iceberg) job: Gold (Iceberg) -> publication package on S3. Thin entry point.

Design box "Glue job - Load". In an enterprise network this job would write to SQL Server
through a Glue JDBC connection inside a VPC. This lab deliberately has no network path from
AWS to the local SQL Server (no VPN/NAT, nothing exposed), so the job stops at the same
publication-package contract the local Load step produces (``load.package_manifest``); the
local publisher then performs the transactional SQL Server load (``load.publisher``).

Only rows written by this pipeline run are exported, and only if the quality gate passed. Each
consumer product is one Parquet object: products hold one row per portfolio and date, and the
job refuses more than ``--max_product_rows`` rows before writing a single file.

Code: this script + the library ZIP (``--extra-py-files``), built by scripts/build. Status:
implemented, NOT executed on AWS and not executed with Spark locally (no JVM).

Arguments: --bucket --run_id --reference_date --quality_gate (JSON) --gold_database
           --max_product_rows
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
    from readers.tables import read_table_rows
    from utils.errors import PublicationError
    from writers.parquet import write_parquet

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

    from load import package_manifest
    from storage.objects import S3Store
    from utils import structured_log
    from utils.contracts import consumer_contract

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
