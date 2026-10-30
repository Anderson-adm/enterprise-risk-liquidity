#!/usr/bin/env python3
"""Smoke-test the BUILT Lambda ZIP (not the source tree) in an isolated interpreter.

    scripts/build_lambda.sh && uv run python scripts/smoke_lambda_package.py

The archive is extracted and placed first on ``sys.path`` (like /var/task in the Lambda
runtime) of a ``python -I -S`` child process. The child proves that pyarrow and lxml are
loaded from the package, round-trips the Bronze contract's Parquet codec with the contract's
value types (DECIMAL, timestamp, date, strings with NULL) and invokes the three handlers
against an in-process S3 mock (moto). boto3/moto come from the development environment,
appended *after* the package (Lambda provides boto3 itself).

This is an emulated package test on this machine's Linux/x86_64 + CPython 3.12. It is not a
Lambda execution: the managed runtime, its Amazon Linux image and IAM are not exercised.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import sysconfig
import tempfile
import zipfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CHILD = r"""
import importlib, io, json, os, sys
pkg, extra, inputs_path, handler_module, codec, out = sys.argv[1:7]
sys.path.insert(0, pkg)
sys.path.append(extra)
from datetime import date, datetime
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq
import lxml
report = {"python": sys.version.split()[0], "codec": codec}
for name, mod in {"pyarrow": pa, "lxml": lxml}.items():
    origin = os.path.realpath(mod.__file__)
    if not origin.startswith(os.path.realpath(pkg)):
        raise SystemExit(f"{name} was not loaded from the package: {origin}")
    report[name] = mod.__version__

schema = pa.schema([
    pa.field("quantity", pa.decimal128(24, 6)), pa.field("market_value", pa.decimal128(20, 2)),
    pa.field("processed_at", pa.timestamp("us")), pa.field("reference_date", pa.date32()),
    pa.field("text", pa.string()), pa.field("meta_row_number", pa.int64()),
])
table = pa.Table.from_pylist([
    {"quantity": Decimal("999999999999999999.999999"), "market_value": Decimal("-999999999999999999.99"),
     "processed_at": datetime(2026, 10, 27, 19, 0, 0, 123456), "reference_date": date(2026, 10, 26),
     "text": "SYN-EQ-005", "meta_row_number": 1},
    {"quantity": Decimal("0.000001"), "market_value": Decimal("0.01"), "processed_at": None,
     "reference_date": None, "text": None, "meta_row_number": 2},
], schema=schema)
buf = io.BytesIO()
pq.write_table(table, buf, compression=codec)
restored = pq.read_table(io.BytesIO(buf.getvalue()))
assert restored.equals(table), "round-trip changed the data"
written = pq.ParquetFile(io.BytesIO(buf.getvalue())).metadata.row_group(0).column(0).compression
report["roundtrip"] = {"rows": restored.num_rows, "column_compression": written}

os.environ.update(AWS_DEFAULT_REGION="sa-east-1", AWS_ACCESS_KEY_ID="testing",
                  AWS_SECRET_ACCESS_KEY="testing", AWS_CONFIG_FILE="/dev/null",
                  AWS_SHARED_CREDENTIALS_FILE="/dev/null", ERL_S3_BUCKET="erl-smoke")
os.environ.pop("AWS_PROFILE", None)
import boto3
from moto import mock_aws
cases = json.load(open(inputs_path))
report["invocations"] = []
with mock_aws():
    s3 = boto3.client("s3", region_name="sa-east-1")
    s3.create_bucket(Bucket="erl-smoke", CreateBucketConfiguration={"LocationConstraint": "sa-east-1"})
    handlers = importlib.import_module(handler_module)
    if not os.path.realpath(handlers.__file__).startswith(os.path.realpath(pkg)):
        raise SystemExit("handler module not loaded from the package")
    for case in cases:
        s3.put_object(Bucket="erl-smoke", Key=case["key"], Body=open(case["path"], "rb").read())
        ctx = type("Ctx", (), {"function_name": case["function"]})()
        event = {"bucket": "erl-smoke", "key": case["key"], "run_id": case["run_id"],
                 "reference_date": case["reference_date"]}
        manifest = getattr(handlers, case["handler"])(event, ctx)["manifest"]
        entry = {"handler": case["handler"], "object": case["key"].rsplit("/", 1)[-1],
                 "status": manifest["status"], "expected": case["expected"]}
        if manifest["status"] != case["expected"]:
            raise SystemExit(f"unexpected status: {entry} {manifest.get('rejection_reason')}")
        if manifest["bronze_object"]:
            body = s3.get_object(Bucket="erl-smoke", Key=manifest["bronze_object"])["Body"].read()
            pf = pq.ParquetFile(io.BytesIO(body))
            entry["bronze_rows"] = pf.metadata.num_rows
            entry["bronze_compression"] = pf.metadata.row_group(0).column(0).compression
            assert entry["bronze_rows"] == manifest["records_written"]
        else:
            entry["rejection_reason"] = manifest["rejection_reason"]
        report["invocations"].append(entry)
json.dump(report, open(out, "w"), indent=2)
print(json.dumps(report, indent=2))
"""


def _cases(tmp: Path) -> list[dict]:
    from erl.ingestion import bronze
    from erl.sources import synthetic

    d1, d2 = synthetic.DEFAULT_DATES[:2]
    handlers = {
        "market_data": ("market_data_handler", "erl-dev-ingest-market-data"),
        "internal_xml": ("internal_xml_handler", "erl-dev-ingest-internal-xml"),
        "external_xml": ("external_xml_handler", "erl-dev-ingest-external-xml"),
    }
    wanted = [
        ("market_data", d1, "market_data_20261026.csv", "ACCEPTED"),
        ("internal_xml", d1, "statement_PRT-0006_20261026.xml", "ACCEPTED"),
        ("external_xml", d1, "transfer_in_ADM77_EXT77-0001_20261026.xml", "ACCEPTED"),
        ("external_xml", d2, "transfer_in_ADM77_EXT77-0002_20261027.xml", "REJECTED"),  # XSD failure
    ]
    files = {(s, d, n): b for s, d, n, b in synthetic.generate().files}
    run_id = bronze.new_run_id()
    cases = []
    for source, ref, name, expected in wanted:
        path = tmp / name
        path.write_bytes(files[(source, ref, name)])
        handler, function = handlers[source]
        cases.append(
            {
                "key": f"landing/files/{source}/delivery_date={ref}/{run_id}/{name}",
                "path": str(path),
                "run_id": run_id,
                "reference_date": date.isoformat(ref),
                "handler": handler,
                "function": function,
                "expected": expected,
            }
        )
    return cases


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", type=Path, default=ROOT / "dist/lambda/bronze/file_ingestion.zip")
    ap.add_argument("--handler-module", default="erl.aws.lambda_handlers")
    ap.add_argument("--report", type=Path, default=ROOT / "dist/lambda/bronze/smoke_report.json")
    args = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        pkg = tmp / "task"
        with zipfile.ZipFile(args.zip) as archive:
            archive.extractall(pkg)
            contract = next(n for n in archive.namelist() if n.endswith("bronze_contract.json"))
            codec = json.loads(archive.read(contract))["parquet_compression"]
        inputs = tmp / "cases.json"
        inputs.write_text(json.dumps(_cases(tmp)))
        child = tmp / "child.py"
        child.write_text(CHILD)
        extra = sysconfig.get_paths()["purelib"]
        cmd = [sys.executable, "-I", "-S", str(child), str(pkg), extra, str(inputs)]
        cmd += [args.handler_module, codec, str(args.report.resolve())]
        args.report.parent.mkdir(parents=True, exist_ok=True)
        return subprocess.run(cmd, cwd=tmp, check=False).returncode  # noqa: S603 - fixed argv


if __name__ == "__main__":
    sys.exit(main())
