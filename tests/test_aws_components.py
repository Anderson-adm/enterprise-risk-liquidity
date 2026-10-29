"""AWS-facing code exercised against moto (in-process S3 mock). These tests prove the Lambda
handlers and S3 store logic; they are NOT a validation of the real AWS services."""

from __future__ import annotations

import json

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
    with pytest.raises(ConfigurationError, match="expected 999999999999"):
        runtime.assert_expected_account(config.load())
