"""AWS-facing code exercised against moto (in-process S3 mock). These tests prove the Lambda
handlers and S3 store logic; they are NOT a validation of the real AWS services."""

from __future__ import annotations

import ast
import json
import tomllib
from pathlib import Path
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from erl.errors import ConfigurationError, ObjectExistsError
from erl.ingestion import bronze
from erl.sources import synthetic
from erl.storage import S3Store
from tests.conftest import D1, run_id

BUCKET = "erl-dev-test-data"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        client = boto3.client("s3", region_name="sa-east-1")
        client.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "sa-east-1"})
        yield client


def test_s3_store_refuses_overwrite(s3):
    store = S3Store(BUCKET, client=s3)
    store.put("bronze/x.parquet", b"1")
    with pytest.raises(ObjectExistsError):
        store.put("bronze/x.parquet", b"2")
    assert store.get("bronze/x.parquet") == b"1"


def test_lambda_handler_ingests_market_data_into_s3_bronze(s3, monkeypatch):
    monkeypatch.setenv("ERL_S3_BUCKET", BUCKET)
    from erl.aws import lambda_handlers

    name, body = next((n, b) for s, d, n, b in synthetic.generate().files if s == "market_data" and d == D1)
    run = run_id()
    key = f"landing/files/market_data/delivery_date={D1}/{run}/{name}"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)

    class Ctx:
        function_name = "erl-dev-ingest-market-data"

    out = lambda_handlers.market_data_handler(
        {"bucket": BUCKET, "key": key, "run_id": run, "reference_date": D1.isoformat()}, Ctx()
    )
    manifest = out["manifest"]
    assert manifest["status"] == bronze.ACCEPTED
    assert manifest["component"] == "lambda:erl-dev-ingest-market-data"
    s3.head_object(Bucket=BUCKET, Key=manifest["bronze_object"])
    stored = json.loads(
        s3.get_object(Bucket=BUCKET, Key=bronze.IngestionManifest(**manifest).key())["Body"].read()
    )
    assert stored["input_sha256"] == bronze.sha256_hex(body)


def test_lambda_handler_rejects_objects_outside_its_scope(s3, monkeypatch):
    monkeypatch.setenv("ERL_S3_BUCKET", BUCKET)
    from erl.aws import lambda_handlers

    event = {
        "bucket": BUCKET,
        "key": "landing/files/internal_xml/x.xml",
        "run_id": run_id(),
        "reference_date": D1.isoformat(),
    }
    with pytest.raises(ValueError, match="not a market_data landing object"):
        lambda_handlers.market_data_handler(event, None)
    with pytest.raises(ValueError, match="outside this function's scope"):
        lambda_handlers.market_data_handler(
            event | {"bucket": "other", "key": "landing/files/market_data/x.csv"}, None
        )


def test_wrong_aws_account_is_refused(s3, monkeypatch):
    from erl import config
    from erl.aws import runtime

    monkeypatch.setenv("ERL_MODE", "aws")
    monkeypatch.setenv("ERL_AWS_ACCOUNT_ID", "999999999999")  # moto reports 123456789012
    mocked_session = boto3.Session(region_name="sa-east-1")
    monkeypatch.setattr(runtime, "_session", lambda settings: mocked_session)
    with pytest.raises(ConfigurationError, match="expected 999999999999"):
        runtime.assert_expected_account(config.load())


@pytest.mark.parametrize("profile", [None, "default"])
def test_aws_session_refuses_implicit_or_default_credentials(settings, monkeypatch, profile):
    from erl.aws import runtime

    if profile is None:
        monkeypatch.delenv("AWS_PROFILE", raising=False)
    else:
        monkeypatch.setenv("AWS_PROFILE", profile)
    with pytest.raises(ConfigurationError, match="AWS_PROFILE"):
        runtime._session(settings)


def _glue_helper(name, **context):
    root = Path(__file__).resolve().parents[1]
    module = ast.parse((root / "glue/jobs/db_ingest.py").read_text())
    # Exercise the actual pure/S3 helpers without importing unavailable Spark/Glue libraries.
    selected = ast.Module(
        body=[n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == name], type_ignores=[]
    )
    exec(compile(selected, "glue/jobs/db_ingest.py", "exec"), context)
    return context[name]


def test_glue_bronze_part_commit_cannot_overwrite_an_existing_object(s3):
    from botocore.exceptions import ClientError

    s3.put_object(Bucket=BUCKET, Key="tmp/part", Body=b"new")
    s3.put_object(Bucket=BUCKET, Key="bronze/part", Body=b"previous")
    commit = _glue_helper("put_part_if_absent", s3=s3, BUCKET=BUCKET)
    with pytest.raises(ClientError, match="PreconditionFailed"):
        commit("tmp/part", "bronze/part")
    assert s3.get_object(Bucket=BUCKET, Key="bronze/part")["Body"].read() == b"previous"


def test_glue_dedup_compares_only_the_latest_accepted_delivery():
    manifests = {
        "a": {"status": "ACCEPTED", "input_sha256": "A", "ingested_at": "1", "run_id": "a"},
        "b": {"status": "ACCEPTED", "input_sha256": "B", "ingested_at": "2", "run_id": "b"},
    }
    client = SimpleNamespace(
        get_paginator=lambda *a: SimpleNamespace(
            paginate=lambda **kw: [{"Contents": [{"Key": k} for k in manifests]}]
        )
    )
    duplicate = _glue_helper("accepted_duplicate", s3=client, BUCKET=BUCKET, get_json=manifests.__getitem__)
    assert duplicate("fund_ops", "fund", "2026-10-26", "A", "latest") is None
    assert duplicate("fund_ops", "fund", "2026-10-26", "B", "latest") == manifests["b"]


def test_glue_sdk_dependencies_are_pinned_to_the_tested_lock():
    root = Path(__file__).resolve().parents[1]
    locked = {p["name"]: p["version"] for p in tomllib.loads((root / "uv.lock").read_text())["package"]}
    requirements = (root / "glue/requirements.txt").read_text().splitlines()
    for line in requirements:
        if line and not line.startswith("#"):
            name, version = line.split("==")
            assert locked[name] == version


def test_s3_exists_works_with_prefix_scoped_list_permission():
    from botocore.exceptions import ClientError

    def head_denied(**kwargs):
        raise ClientError({"Error": {"Code": "403"}}, "HeadObject")

    client = SimpleNamespace(
        head_object=head_denied,
        list_objects_v2=lambda **kw: {"Contents": [{"Key": "landing/file.json.extra"}]},
    )
    store = S3Store(BUCKET, client=client)
    assert not store.exists("landing/file.json")
    client.list_objects_v2 = lambda **kw: {"Contents": [{"Key": "landing/file.json"}]}
    assert store.exists("landing/file.json")


def test_lambda_without_the_parquet_codec_leaves_only_the_landing_object(s3, monkeypatch):
    """The failed AWS attempt left a raw Bronze XML without manifest; now nothing is written."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from erl.contracts import parquet_compression

    monkeypatch.setenv("ERL_S3_BUCKET", BUCKET)
    from erl.aws import lambda_handlers

    real = pq.write_table

    def write_table(table, where, *args, compression="snappy", **kwargs):
        if compression == parquet_compression():
            raise pa.ArrowNotImplementedError("Support for codec 'zstd' not built")
        return real(table, where, *args, compression=compression, **kwargs)

    monkeypatch.setattr(pq, "write_table", write_table)
    name, body = next((n, b) for s, d, n, b in synthetic.generate().files if s == "external_xml" and d == D1)
    run = run_id()
    key = f"landing/files/external_xml/delivery_date={D1}/{run}/{name}"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    with pytest.raises(ConfigurationError, match="cannot use Parquet codec"):
        lambda_handlers.external_xml_handler(
            {"bucket": BUCKET, "key": key, "run_id": run, "reference_date": D1.isoformat()}, None
        )
    assert [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]] == [key]
