#!/usr/bin/env python3
"""Import and exercise each BUILT unit in its own python -I -S process.

Lambda: actual pinned native dependencies, codec and handlers on moto S3. Glue: own ZIP,
Spark/Glue import stubs and non-Spark protocol on moto. This is not Spark or AWS execution.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import sysconfig
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIST = ROOT / "dist"

STUBS = {
    "pyspark/__init__.py": "",
    "pyspark/context.py": "class SparkContext: pass\n",
    "pyspark/sql/__init__.py": "class DataFrame: pass\nclass SparkSession: pass\n",
    "pyspark/sql/functions.py": (
        "def col(*a): raise NotImplementedError\ndef lit(*a): raise NotImplementedError\n"
    ),
    "pyspark/sql/types.py": "class StructType: pass\n",
    "awsglue/__init__.py": "",
    "awsglue/context.py": "class GlueContext: pass\n",
    "awsglue/utils.py": "def getResolvedOptions(*a): raise NotImplementedError\n",
}

LAMBDA_CHILD = r"""
import importlib, io, json, os, sys

pkg, extra, cases_file, output = sys.argv[1:5]
sys.path[:0] = [pkg, extra]
import pyarrow as pa
import pyarrow.parquet as pq

assert os.path.realpath(pa.__file__).startswith(pkg)
from erl_protocol import __version__
from erl_protocol.contracts import parquet_compression
from erl_protocol.records import row_hash
from erl_protocol.parquet import table_to_parquet_bytes, parquet_bytes_to_table

assert __version__ == "2.0.0"
assert row_hash(["a\x1fb", "c"]) != row_hash(["a", "b\x1fc"])
from datetime import date, datetime
from decimal import Decimal

schema = pa.schema(
    [
        ("quantity", pa.decimal128(24, 6)),
        ("money", pa.decimal128(20, 2)),
        ("date", pa.date32()),
        ("timestamp", pa.timestamp("us")),
        ("nullable", pa.string()),
    ]
)
table = pa.Table.from_pylist(
    [
        {
            "quantity": Decimal("999999999999999999.999999"),
            "money": Decimal("-999999999999999999.99"),
            "date": date(2026, 10, 26),
            "timestamp": datetime(2026, 10, 26, 19, 0, 0, 123456),
            "nullable": None,
        }
    ],
    schema=schema,
)
assert parquet_bytes_to_table(table_to_parquet_bytes(table, parquet_compression())).equals(table)
assert all(n not in sys.modules for n in ("pipeline", "sources", "load", "connect"))
import boto3
from moto import mock_aws

cases = json.load(open(cases_file))
report = {"protocol_version": __version__, "pyarrow": pa.__version__, "codec_roundtrip": True, "cases": []}
with mock_aws():
    s3 = boto3.client("s3", region_name="sa-east-1")
    s3.create_bucket(Bucket="erl-unit-smoke", CreateBucketConfiguration={"LocationConstraint": "sa-east-1"})
    handler = importlib.import_module("handler")
    for i, case in enumerate(cases):
        key = (f"landing/files/{case['source']}/delivery_date={case['reference_date']}/"
               f"{case['run_id']}/{case['name']}")
        s3.put_object(Bucket="erl-unit-smoke", Key=key, Body=open(case["path"], "rb").read())
        event = {
            "bucket": "erl-unit-smoke",
            "key": key,
            "run_id": case["run_id"],
            "reference_date": case["reference_date"],
        }
        m = handler.handler(event, type("Context", (), {"function_name": case["source"]})())["manifest"]
        assert m["status"] == case["expected"], m
        if m["bronze_object"]:
            body = s3.get_object(Bucket="erl-unit-smoke", Key=m["bronze_object"])["Body"].read()
            pf = pq.ParquetFile(io.BytesIO(body))
            assert pf.metadata.num_rows == m["records_written"]
            assert pf.metadata.row_group(0).column(0).compression == parquet_compression().upper()
        report["cases"].append(
            {"status": m["status"], "rows": m["records_written"], "reason": m["rejection_reason"]}
        )
for name in ("handler", "erl_protocol", cases[0]["source"]):
    assert os.path.realpath(sys.modules[name].__file__).startswith(pkg)
if cases[0]["source"] != "market_data":
    import lxml

    assert os.path.realpath(lxml.__file__).startswith(pkg)
    report["lxml"] = lxml.__version__
json.dump(report, open(output, "w"), indent=2)
"""

GLUE_CHILD = r"""
import ast, hashlib, importlib, importlib.util, json, os, sys

lib, script, stubs, extra, output = sys.argv[1:6]
sys.path[:0] = [lib, stubs, extra]
modules = []
for node in ast.walk(ast.parse(open(script).read())):
    if isinstance(node, ast.ImportFrom) and node.module:
        mod = importlib.import_module(node.module)
        if node.module == "erl_protocol":
            for a in node.names:
                importlib.import_module("erl_protocol." + a.name)
        if node.module.split(".")[0] in {"erl_protocol", "unit_spark"}:
            assert os.path.realpath(mod.__file__).startswith(lib)
            modules.append(node.module)
    elif isinstance(node, ast.Import):
        for alias in node.names:
            importlib.import_module(alias.name)
spec = importlib.util.spec_from_file_location("unit_job", script)
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)
assert not any(
    n in sys.modules for n in ("pyarrow", "sqlalchemy", "lxml", "duckdb", "pipeline", "sources", "load")
)
from erl_protocol import __version__
from erl_protocol.objects import S3Store
import boto3
from moto import mock_aws

report = {
    "protocol_version": __version__,
    "imports_from_own_zip": sorted(set(modules)),
    "spark_executed": False,
}
with mock_aws():
    s3 = boto3.client("s3", region_name="sa-east-1")
    s3.create_bucket(Bucket="erl-unit-smoke", CreateBucketConfiguration={"LocationConstraint": "sa-east-1"})
    store = S3Store("erl-unit-smoke", client=s3)
    store.put("tmp/input", b"synthetic acceptance fixture")
    store.copy_if_absent("tmp/input", "bronze/copied")
    try:
        store.copy_if_absent("tmp/input", "bronze/copied")
        raise AssertionError("overwrite was allowed")
    except Exception as e:
        assert type(e).__name__ == "ObjectExistsError"
    if hasattr(job, "parts_for"):
        from erl_protocol import snapshot_manifests as sm

        ref, run = "2026-10-26", "20261026T190000Z-abcdef"
        body = b"synthetic landing fixture (non-Spark acceptance only)"
        key = f"landing/db/fund_ops/fund/reference_date={ref}/{run}"
        store.put(key + ".parquet", body)
        store.put(
            key + ".json",
            json.dumps(
                {
                    "run_id": run,
                    "source_system": "fund_ops",
                    "entity": "fund",
                    "reference_date": ref,
                    "rows": 4,
                    "object": key + ".parquet",
                    "object_sha256": hashlib.sha256(body).hexdigest(),
                    "content_sha256": "a" * 64,
                    "content_hash_version": 2,
                }
            ).encode(),
        )
        snap = sm.LandingSnapshot.read(store, key + ".json")
        sm.verify_landing_checksum(snap, sm.landing_object_sha256(store, snap))
        assert sm.record_duplicate(store, snap, "smoke", 4) is None
        store.copy_if_absent(key + ".parquet", "bronze/accepted")
        result = sm.record_accepted(store, snap, "smoke", ["bronze/accepted"], 4)
        assert result.input_hash_version == 2 and sm.completed_snapshot(store, snap).status == "ACCEPTED"
        report["acceptance_status"] = result.status
        report["volume_profiles"] = {"gb": job.parts_for("gb", 2500000), "tb": job.parts_for("tb", 2500000)}
    else:
        from erl_protocol import publication

        publication.require_passed_gate({"status": "PASSED"})
        from datetime import date

        m = publication.build_manifest(
            date(2026, 10, 26),
            "synthetic-run",
            "glue:smoke",
            {"status": "PASSED"},
            {"product": {"object": "bronze/copied", "sha256": store.sha256_hex("bronze/copied"), "rows": 1}},
        )
        assert m["quality_gate"]["status"] == "PASSED"
        report["publication_manifest_checked"] = True
report["conditional_copy_refuses_overwrite"] = True
json.dump(report, open(output, "w"), indent=2)
"""

LOCAL_CHILD = r"""
import json, sys

archive, extra, unit, out = sys.argv[1:5]
sys.path[:0] = [archive, extra]
if unit == "apps/publisher":
    from load import publisher
    from load.sql_files import read_sql

    assert "SELECT" in read_sql("load/lock_publication_postgresql.sql")
    assert "pipeline" not in sys.modules and "sources" not in sys.modules
    report = {"publisher_imported": True, "own_sql_resources_read": True}
elif unit == "apps/source-generator":
    from sources import synthetic, seed
    from datetime import date

    dataset = synthetic.generate((date(2026, 10, 26),))
    assert len(dataset.tables) == 4
    assert "pipeline" not in sys.modules and "load" not in sys.modules
    report = {"generator_imported": True, "single_date_fixture_generated_in_memory": True}
else:
    from pipeline import cli
    from ingestion.parsers import PARSERS
    from utils.sql_files import read_sql

    assert set(PARSERS) == {"market_data", "internal_xml", "external_xml"}
    assert "SELECT" in read_sql("extract/extract_query_table_fund.sql")
    assert cli.main(["new-run-id"]) == 0
    report = {
        "cli_imported_from_artifact": True,
        "own_sql_resources_read": True,
        "public_parsers": sorted(PARSERS),
    }
json.dump(report, open(out, "w"))
"""


def run(child: str, args: list[str], work: Path) -> dict:
    script = work / "smoke.py"
    script.write_text(child)
    env = dict(os.environ)
    for key in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
        env.pop(key, None)
    env.update(
        AWS_CONFIG_FILE="/dev/null",
        AWS_SHARED_CREDENTIALS_FILE="/dev/null",
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",  # noqa: S106 - moto fake credentials
        AWS_EC2_METADATA_DISABLED="true",
        AWS_DEFAULT_REGION="sa-east-1",
        ERL_S3_BUCKET="erl-unit-smoke",
        ERL_DATA_DIR=str(work / "data"),
        ERL_MODE="local",
        ERL_SOURCE_BACKEND="sqlite",
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    result = subprocess.run(  # noqa: S603 - fixed isolated test interpreter
        [sys.executable, "-I", "-S", str(script), *args],
        cwd=work,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return json.loads(Path(args[-1]).read_text())


def lambda_cases(source: str, work: Path) -> list[dict]:
    from erl_protocol.run_ids import new_run_id
    from sources import synthetic

    selected = {
        "market_data": [("2026-10-26", "market_data_20261026.csv", "ACCEPTED")],
        "internal_xml": [("2026-10-26", "statement_PRT-0006_20261026.xml", "ACCEPTED")],
        "external_xml": [
            ("2026-10-26", "transfer_in_ADM77_EXT77-0001_20261026.xml", "ACCEPTED"),
            ("2026-10-27", "transfer_in_ADM77_EXT77-0002_20261027.xml", "REJECTED"),
        ],
    }
    delivered = {(s, str(d), name): body for s, d, name, body in synthetic.generate().files}
    cases = []
    for ref, name, status in selected[source]:
        path = work / name
        path.write_bytes(delivered[source, ref, name])
        cases.append(
            {
                "source": source,
                "reference_date": ref,
                "run_id": new_run_id(),
                "name": name,
                "path": str(path),
                "expected": status,
            }
        )
    return cases


def smoke_unit(key: str, manifest: dict) -> dict:
    extra = sysconfig.get_paths()["purelib"]
    directory = DIST / key
    for name, meta in manifest["artifacts"].items():
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != meta["sha256"]:
            raise ValueError(f"{key}/{name}: artifact differs from build manifest")
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        output = work / "report.json"
        if key.startswith("lambda/"):
            package = work / "package"
            with zipfile.ZipFile(directory / "function.zip") as archive:
                if sum(m.file_size for m in archive.infolist()) > 250 * 1024**2:
                    raise ValueError(f"{key}: uncompressed Lambda package exceeds 250 MiB")
                archive.extractall(package)
            source = json.loads((ROOT / key / "unit.json").read_text())["python_package"]
            cases = work / "cases.json"
            cases.write_text(json.dumps(lambda_cases(source, work)))
            return run(LAMBDA_CHILD, [str(package), extra, str(cases), str(output)], work)
        if key.startswith("glue/"):
            stubs = work / "stubs"
            for name, body in STUBS.items():
                path = stubs / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(body)
            return run(
                GLUE_CHILD,
                [str(directory / "lib.zip"), str(directory / "job.py"), str(stubs), extra, str(output)],
                work,
            )
        return run(
            LOCAL_CHILD, [str(directory / next(iter(manifest["artifacts"]))), extra, key, str(output)], work
        )


def main() -> None:
    manifest = json.loads((DIST / "manifest.json").read_text())
    report = {
        "scope": "Local artifact emulation only; no Spark JVM, Glue or AWS execution",
        "git": manifest["git"],
        "units": {key: smoke_unit(key, spec) for key, spec in manifest["units"].items()},
    }
    (DIST / "smoke_report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
