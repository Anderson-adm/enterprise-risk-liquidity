"""Glue 5.0 (Spark + Iceberg) job: Gold (Iceberg) -> publication package on S3.

Design box "Glue job - Load". In an enterprise network this job would write to SQL Server
through a Glue JDBC connection inside a VPC. This lab deliberately has no network path from
AWS to the local SQL Server (no VPN/NAT, nothing exposed), so the job stops at the same
publication-package contract the local Load step produces; the local publisher then performs
the transactional SQL Server load (erl.publish.publisher).

Only rows written by this pipeline run are exported, and only if the quality gate passed.

Status: implemented, NOT executed on AWS yet (see docs/AWS_VALIDATION.md).

Arguments: --bucket --run_id --reference_date --quality_gate (JSON) --gold_database
           --consumer_contract_s3_key
"""

import hashlib
import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

ARGS = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket", "run_id", "reference_date", "quality_gate", "gold_database", "consumer_contract_s3_key"],
)
BUCKET = ARGS["bucket"]
s3 = boto3.client("s3")
spark = GlueContext(SparkContext.getOrCreate()).spark_session


def log(event, **fields):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "event": event, "job": ARGS["JOB_NAME"], **fields}))


def main():
    gate = json.loads(ARGS["quality_gate"])
    if gate.get("status") != "PASSED":
        raise RuntimeError(f"quality gate is {gate.get('status')}; refusing to export Gold")
    contract = json.loads(s3.get_object(Bucket=BUCKET, Key=ARGS["consumer_contract_s3_key"])["Body"].read())
    ref, run_id = ARGS["reference_date"], ARGS["run_id"]
    prefix = f"publish/reference_date={ref}/{run_id}/"
    products = {}
    for name, spec in contract["products"].items():
        cols = [c["name"] for c in spec["columns"]]
        df = (
            spark.table(f"glue_catalog.{ARGS['gold_database']}.{spec['gold_model']}")
            .where((F.col("reference_date") == F.lit(ref).cast("date")) & (F.col("pipeline_run_id") == run_id))
            .select(*cols)
            .orderBy("portfolio_id")
        )
        tmp = f"tmp/glue/{run_id}/publish/{name}/"
        df.coalesce(1).write.mode("errorifexists").parquet(f"s3://{BUCKET}/{tmp}")
        part = next(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=tmp)["Contents"]
                    if o["Key"].endswith(".parquet"))
        data = s3.get_object(Bucket=BUCKET, Key=part)["Body"].read()
        key = prefix + f"{name}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=data, IfNoneMatch="*")
        products[name] = {"object": key, "sha256": hashlib.sha256(data).hexdigest(), "rows": df.count()}
        for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=tmp).get("Contents", []):
            s3.delete_object(Bucket=BUCKET, Key=o["Key"])

    manifest = {
        "pipeline_run_id": run_id,
        "reference_date": ref,
        "producer": f"glue:{ARGS['JOB_NAME']}",
        "quality_gate": gate,
        "products": products,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
    }
    s3.put_object(Bucket=BUCKET, Key=prefix + "manifest.json", Body=json.dumps(manifest, indent=2).encode(),
                  IfNoneMatch="*")
    log("publication_package_built", run_id=run_id, reference_date=ref, rows={k: v["rows"] for k, v in products.items()})


main()
