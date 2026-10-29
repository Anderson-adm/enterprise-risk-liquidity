#!/usr/bin/env python3
"""Prepare the Case 01 AWS lab for `terraform destroy` (dry-run by default).

What it removes (only with --execute and a typed confirmation):
  * every object version in the two lab buckets (data lake + Athena results), because
    Terraform deliberately does not force-destroy non-empty buckets;
  * the Silver/Gold tables that dbt created in the lab's Glue databases (dbt owns them).

What it never touches: the Terraform state bucket (infra/bootstrap), any bucket or database
whose name/tags do not match the lab, any other account or region. It does NOT run
`terraform destroy`; it prints the command for a human to review and run.

Usage:
    AWS_PROFILE=data-dev python scripts/teardown_aws.py --account-id 123456789012
    AWS_PROFILE=data-dev python scripts/teardown_aws.py --account-id 123456789012 --execute
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import subprocess
import sys
from pathlib import Path

from botocore.exceptions import ClientError

from erl import config
from erl.aws.runtime import _session

ROOT = Path(__file__).resolve().parents[1]
DEV = ROOT / "infra" / "environments" / "dev"
PROJECT_TAG = "enterprise-risk-liquidity"


def terraform_outputs() -> dict:
    out = subprocess.run(  # noqa: S603
        ["terraform", f"-chdir={DEV}", "output", "-json"],  # noqa: S607 - terraform from PATH
        capture_output=True,
        text=True,
        check=True,
    )
    return {k: v["value"] for k, v in json.loads(out.stdout).items()}


def bucket_in_scope(s3, bucket: str, account_id: str, prefix: str) -> None:
    if not bucket.startswith(f"{prefix}-") or not bucket.endswith(account_id):
        sys.exit(f"refusing: bucket {bucket!r} does not match {prefix}-*-{account_id}")
    tags = {t["Key"]: t["Value"] for t in s3.get_bucket_tagging(Bucket=bucket)["TagSet"]}
    if tags.get("Project") != PROJECT_TAG or tags.get("ManagedBy") != "terraform":
        sys.exit(f"refusing: bucket {bucket!r} is not tagged Project={PROJECT_TAG}, ManagedBy=terraform")
    if tags.get("Environment") != "dev" or tags.get("Purpose") != "case01-validation-lab":
        sys.exit(f"refusing: bucket {bucket!r} is not a dev lab bucket (state is always excluded)")


def count_versions(s3, bucket: str) -> int:
    n = 0
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        n += len(page.get("Versions", [])) + len(page.get("DeleteMarkers", []))
    return n


def empty_bucket(s3, bucket: str) -> int:
    deleted = 0
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        items = [
            {"Key": v["Key"], "VersionId": v["VersionId"]}
            for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
        ]
        for i in range(0, len(items), 1000):
            result = s3.delete_objects(Bucket=bucket, Delete={"Objects": items[i : i + 1000], "Quiet": True})
            if result.get("Errors"):
                raise RuntimeError(f"S3 deletion failed: {result['Errors']}")
            deleted += len(items[i : i + 1000])
    return deleted


def dbt_tables(glue, database: str) -> list[str]:
    try:
        pages = glue.get_paginator("get_tables").paginate(DatabaseName=database)
        return [t["Name"] for p in pages for t in p["TableList"]]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            return []
        raise


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account-id", required=True, help="expected data-dev account id")
    ap.add_argument("--region", default="sa-east-1")
    ap.add_argument("--name-prefix", default="erl-dev")
    ap.add_argument("--execute", action="store_true", help="actually delete (default: dry-run)")
    args = ap.parse_args()

    session = _session(dataclasses.replace(config.load(), aws_region=args.region))
    identity = session.client("sts").get_caller_identity()
    if identity["Account"] != args.account_id:
        sys.exit(f"refusing: credentials are for account {identity['Account']}, expected {args.account_id}")
    print(f"account {identity['Account']} | principal {identity['Arn']} | region {args.region}")

    outputs = terraform_outputs()
    s3, glue = session.client("s3"), session.client("glue")
    buckets = [outputs["data_bucket"], outputs["athena_results_s3"].removeprefix("s3://").split("/")[0]]
    for b in buckets:
        bucket_in_scope(s3, b, args.account_id, args.name_prefix)
    dbs = [outputs["glue_databases"][k] for k in ("silver", "gold")]
    expected_dbs = [f"{args.name_prefix.replace('-', '_')}_{layer}" for layer in ("silver", "gold")]
    if dbs != expected_dbs:
        sys.exit(f"refusing: databases {dbs} do not match lab databases {expected_dbs}")

    plan = {b: count_versions(s3, b) for b in buckets}
    tables = {db: dbt_tables(glue, db) for db in dbs}
    print(json.dumps({"object_versions_to_delete": plan, "dbt_tables_to_delete": tables}, indent=2))

    if not args.execute:
        print("\ndry-run only. Re-run with --execute to empty the buckets and drop dbt tables.")
        return 0
    if input(f"type the account id {args.account_id} to confirm deletion: ").strip() != args.account_id:
        sys.exit("confirmation mismatch; nothing deleted")
    for b in buckets:
        print(f"emptied {b}: {empty_bucket(s3, b)} versions deleted")
    for db, names in tables.items():
        for i in range(0, len(names), 100):
            result = glue.batch_delete_table(DatabaseName=db, TablesToDelete=names[i : i + 100])
            if result.get("Errors"):
                raise RuntimeError(f"Glue deletion failed: {result['Errors']}")
        print(f"dropped {len(names)} dbt tables from {db}")
    print(
        "\nNext (human-reviewed):\n"
        f"  terraform -chdir={DEV.relative_to(ROOT)} plan -destroy -out=destroy.tfplan\n"
        f"  terraform -chdir={DEV.relative_to(ROOT)} apply destroy.tfplan\n"
        "The state bucket (infra/bootstrap) is intentionally kept."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
