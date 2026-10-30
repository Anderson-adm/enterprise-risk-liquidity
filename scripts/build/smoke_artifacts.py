#!/usr/bin/env python3
"""Smoke-test the BUILT artifacts in ``dist/`` (not the source tree), each in an isolated
``python -I -S`` interpreter.

    uv run python scripts/build/build_artifacts.py && uv run python scripts/build/smoke_artifacts.py

Lambda (``dist/lambda/bronze/file_ingestion.zip``): the archive is extracted and placed first on
``sys.path`` (like /var/task). pyarrow, lxml, the project packages and the handler module must
load from the package; the contract's Parquet codec round-trips DECIMAL/timestamp/date/NULL
values; the three handlers run against an in-process S3 mock (moto).

Glue (``dist/glue/...``): the library ZIP is imported through zipimport, as with
``--extra-py-files``. Every module the two job scripts import must resolve from the ZIP
(pyspark/awsglue are replaced by import stubs - there is no JVM here), the Glue path must not
import PyArrow, SQLAlchemy, lxml or DuckDB, and the non-Spark acceptance steps run on moto S3.

Results: ``dist/smoke_report.json``. This is an emulated package test on this machine
(Linux x86_64, CPython 3.12). It is NOT a Lambda or Glue execution: the managed runtimes
(Amazon Linux, Glue's Python 3.11 + Spark 3.5), IAM and real S3 are not exercised.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import sysconfig
import tempfile
import zipfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIST = ROOT / "dist"
NOT_IN_GLUE_PATH = ("pyarrow", "sqlalchemy", "lxml", "duckdb", "psycopg", "pymssql")

LAMBDA_CHILD = r"""
import importlib, io, json, os, sys
pkg, extra, inputs_path, codec, out = sys.argv[1:6]
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
    handlers = importlib.import_module("file_ingestion")
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
for name in ("file_ingestion", "ingestion", "storage", "transform", "utils", "contracts"):
    origin = os.path.realpath(sys.modules[name].__file__)
    if not origin.startswith(os.path.realpath(pkg)):
        raise SystemExit(f"{name} was not loaded from the package: {origin}")
report["project_modules_from_package"] = True
json.dump(report, open(out, "w"), indent=2)
"""

GLUE_CHILD = r"""
import ast, hashlib, importlib, importlib.util, json, os, sys
lib, stubs, extra, scripts, forbidden, out = sys.argv[1:7]
sys.path.insert(0, lib)
sys.path.append(stubs)   # pyspark/awsglue import stubs (no JVM): import graph only
sys.path.append(extra)   # boto3/moto, provided by Glue (boto3) or the test environment (moto)
report = {"python": sys.version.split()[0], "scripts": {}}
for script in scripts.split(","):
    tree = ast.parse(open(script).read())
    modules = sorted({n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
                     | {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names})
    own = []
    for name in modules:
        mod = importlib.import_module(name)
        root = name.split(".")[0]
        if root in {"pyspark", "awsglue", "boto3", "__future__"} or root in sys.stdlib_module_names:
            continue
        if not os.path.realpath(mod.__file__).startswith(os.path.realpath(lib)):
            raise SystemExit(f"{name} imported by {script} is not in the library ZIP: {mod.__file__}")
        own.append(name)
    spec = importlib.util.spec_from_file_location("job_" + os.path.basename(script)[:-3], script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)   # __name__ != "__main__": defines functions only
    report["scripts"][os.path.basename(script)] = {"project_modules_from_zip": own}
loaded = sorted(m for m in forbidden.split(",") if m in sys.modules)
if loaded:
    raise SystemExit(f"the Glue path imported {loaded}")
report["not_imported"] = forbidden.split(",")

os.environ.update(AWS_DEFAULT_REGION="sa-east-1", AWS_ACCESS_KEY_ID="testing",
                  AWS_SECRET_ACCESS_KEY="testing", AWS_CONFIG_FILE="/dev/null",
                  AWS_SHARED_CREDENTIALS_FILE="/dev/null")
os.environ.pop("AWS_PROFILE", None)
import boto3
from moto import mock_aws
from ingestion import snapshot_manifests as sm
from storage.objects import S3Store
from utils.contracts import bronze_entity
with mock_aws():
    s3 = boto3.client("s3", region_name="sa-east-1")
    s3.create_bucket(Bucket="erl-smoke", CreateBucketConfiguration={"LocationConstraint": "sa-east-1"})
    store = S3Store("erl-smoke", client=s3)
    run, ref = "20261026T190000Z-abcdef", "2026-10-26"
    prefix = f"landing/db/fund_ops/fund/reference_date={ref}/"
    body = b"PAR1 synthetic landing bytes PAR1"
    store.put(prefix + f"{run}.parquet", body)
    store.put(prefix + f"{run}.json", json.dumps({
        "run_id": run, "source_system": "fund_ops", "entity": "fund", "reference_date": ref, "rows": 4,
        "object": prefix + f"{run}.parquet", "object_sha256": hashlib.sha256(body).hexdigest(),
        "content_sha256": "c" * 64}).encode())
    snap = sm.LandingSnapshot.read(store, prefix + f"{run}.json")
    assert bronze_entity("fund_ops", "fund").columns[0] == "fund_id"   # contract read from the ZIP
    assert sm.completed_snapshot(store, snap) is None
    sm.verify_landing_checksum(snap, sm.landing_object_sha256(store, snap))   # S3 stored SHA-256
    assert sm.record_duplicate(store, snap, "glue:smoke", snap.rows) is None
    target = f"bronze/structured/fund_ops/fund/bronze_reference_date={ref}/{run}.parquet"
    store.copy_if_absent(prefix + f"{run}.parquet", target)
    accepted = sm.record_accepted(store, snap, "glue:smoke", [target], snap.rows)
    assert sm.completed_snapshot(store, snap).status == "ACCEPTED"   # a retry reuses the commit
    try:
        store.copy_if_absent(prefix + f"{run}.parquet", target)
        raise SystemExit("an existing Bronze object was overwritten")
    except Exception as exc:
        assert type(exc).__name__ == "ObjectExistsError", exc
    report["acceptance_on_moto_s3"] = {"status": accepted.status, "bronze_object": accepted.bronze_object,
                                        "retry_reused_commit": True, "overwrite_refused": True}
spec = importlib.util.spec_from_file_location("job_db_ingest_profiles", scripts.split(",")[0])
jobs = importlib.util.module_from_spec(spec); spec.loader.exec_module(jobs)
report["volume_profiles"] = {
    "gb_10M_rows": jobs.parts_for("gb", 10_000_000), "tb_2.5M_rows": jobs.parts_for("tb", 2_500_000)}
json.dump(report, open(out, "w"), indent=2)
"""

STUBS = {
    "pyspark/__init__.py": "",
    "pyspark/context.py": "class SparkContext:\n    pass\n",
    "pyspark/sql/__init__.py": "class DataFrame:\n    pass\n\n\nclass SparkSession:\n    pass\n",
    "pyspark/sql/functions.py": "def col(name):\n    raise NotImplementedError\n\n\n"
    "def lit(value):\n    raise NotImplementedError\n",
    "pyspark/sql/types.py": "class StructType:\n    pass\n",
    "awsglue/__init__.py": "",
    "awsglue/context.py": "class GlueContext:\n    pass\n",
    "awsglue/utils.py": "def getResolvedOptions(argv, names):\n    raise NotImplementedError\n",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_manifest() -> dict:
    manifest = json.loads((DIST / "manifest.json").read_text())
    for name, meta in manifest["artifacts"].items():
        if _sha256(DIST / name) != meta["sha256"]:
            raise SystemExit(f"{name} does not match dist/manifest.json")
    return manifest


def _lambda_cases(tmp: Path) -> list[dict]:
    from sources import synthetic
    from utils.run_ids import new_run_id

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
    run_id = new_run_id()
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


def _run(child: str, args: list[str], tmp: Path) -> None:
    script = tmp / "child.py"
    script.write_text(child)
    result = subprocess.run([sys.executable, "-I", "-S", str(script), *args], cwd=tmp, check=False)  # noqa: S603
    if result.returncode:
        raise SystemExit(result.returncode)


def smoke_lambda(extra: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        pkg = tmp / "task"
        with zipfile.ZipFile(DIST / "lambda/bronze/file_ingestion.zip") as archive:
            archive.extractall(pkg)
            codec = json.loads(archive.read("contracts/bronze_contract.json"))["parquet_compression"]
        inputs = tmp / "cases.json"
        inputs.write_text(json.dumps(_lambda_cases(tmp)))
        out = tmp / "report.json"
        _run(LAMBDA_CHILD, [str(pkg), extra, str(inputs), codec, str(out)], tmp)
        return json.loads(out.read_text())


def smoke_glue(extra: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        stubs = tmp / "stubs"
        for name, body in STUBS.items():
            (stubs / name).parent.mkdir(parents=True, exist_ok=True)
            (stubs / name).write_text(body)
        scripts = ",".join(str(DIST / s) for s in ("glue/bronze/db_ingest.py", "glue/load/gold_load.py"))
        out = tmp / "report.json"
        lib = str(DIST / "glue/lib/erl_glue_lib.zip")
        _run(GLUE_CHILD, [lib, str(stubs), extra, scripts, ",".join(NOT_IN_GLUE_PATH), str(out)], tmp)
        return json.loads(out.read_text())


def main() -> int:
    manifest = _verify_manifest()
    extra = sysconfig.get_paths()["purelib"]
    report = {
        "scope": "emulated package test on this machine; not a Lambda or Glue execution",
        "git": manifest["git"],
        "artifacts": {k: v["sha256"] for k, v in manifest["artifacts"].items()},
        "lambda": smoke_lambda(extra),
        "glue": smoke_glue(extra),
    }
    (DIST / "smoke_report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
