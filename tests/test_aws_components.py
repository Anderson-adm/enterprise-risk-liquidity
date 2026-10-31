"""AWS-facing code exercised against moto (in-process S3 mock). These tests prove the Lambda
handlers and S3 store logic; they are NOT a validation of the real AWS services."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from erl_protocol import manifests
from erl_protocol.checksums import sha256_hex
from erl_protocol.errors import ConfigurationError, ObjectExistsError
from erl_protocol.objects import S3Store
from sources import synthetic
from tests.conftest import D1, load_entrypoint, run_id

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
    lambda_handlers = load_entrypoint("lambda/market-data/handler.py")

    name, body = next((n, b) for s, d, n, b in synthetic.generate().files if s == "market_data" and d == D1)
    run = run_id()
    key = f"landing/files/market_data/delivery_date={D1}/{run}/{name}"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)

    class Ctx:
        function_name = "erl-dev-ingest-market-data"

    out = lambda_handlers.handler(
        {"bucket": BUCKET, "key": key, "run_id": run, "reference_date": D1.isoformat()}, Ctx()
    )
    manifest = out["manifest"]
    assert manifest["status"] == manifests.ACCEPTED
    assert manifest["component"] == "lambda:erl-dev-ingest-market-data"
    s3.head_object(Bucket=BUCKET, Key=manifest["bronze_object"])
    stored = json.loads(
        s3.get_object(Bucket=BUCKET, Key=manifests.IngestionManifest(**manifest).key())["Body"].read()
    )
    assert stored["input_sha256"] == sha256_hex(body)


def test_lambda_handler_rejects_objects_outside_its_scope(s3, monkeypatch):
    monkeypatch.setenv("ERL_S3_BUCKET", BUCKET)
    lambda_handlers = load_entrypoint("lambda/market-data/handler.py")

    event = {
        "bucket": BUCKET,
        "key": "landing/files/internal_xml/x.xml",
        "run_id": run_id(),
        "reference_date": D1.isoformat(),
    }
    with pytest.raises(ValueError, match="not a market_data landing object"):
        lambda_handlers.handler(event, None)
    with pytest.raises(ValueError, match="outside this function's scope"):
        lambda_handlers.handler(event | {"bucket": "other", "key": "landing/files/market_data/x.csv"}, None)


def test_wrong_aws_account_is_refused(s3):
    from connect.aws import assert_expected_account

    mocked_session = boto3.Session(region_name="sa-east-1")  # moto reports 123456789012
    with pytest.raises(ConfigurationError, match="expected 999999999999"):
        assert_expected_account(mocked_session, "999999999999")


@pytest.mark.parametrize("profile", [None, "default"])
def test_aws_session_refuses_implicit_or_default_credentials(settings, monkeypatch, profile):
    if profile is None:
        monkeypatch.delenv("AWS_PROFILE", raising=False)
    else:
        monkeypatch.setenv("AWS_PROFILE", profile)
    with pytest.raises(ConfigurationError, match="AWS_PROFILE"):
        settings.aws_session()


def test_aws_session_refuses_other_regions():
    from connect.aws import aws_session

    with pytest.raises(ConfigurationError, match="restricted"):
        aws_session("erl-dev-runner", "us-east-1", allowed_regions=frozenset({"sa-east-1"}))


def test_bronze_part_commit_cannot_overwrite_an_existing_object(s3):
    """Glue commits Spark parts with a conditional server-side copy (no bytes through the driver)."""
    store = S3Store(BUCKET, client=s3)
    s3.put_object(Bucket=BUCKET, Key="tmp/part", Body=b"new" * 1000)
    store.copy_if_absent("tmp/part", "bronze/fresh")
    assert s3.get_object(Bucket=BUCKET, Key="bronze/fresh")["Body"].read() == b"new" * 1000
    s3.put_object(Bucket=BUCKET, Key="bronze/part", Body=b"previous")
    with pytest.raises(ObjectExistsError):
        store.copy_if_absent("tmp/part", "bronze/part")
    assert s3.get_object(Bucket=BUCKET, Key="bronze/part")["Body"].read() == b"previous"
    assert s3.list_multipart_uploads(Bucket=BUCKET).get("Uploads", []) == []  # aborted, nothing pending


def test_s3_checksum_comes_from_s3_or_a_streamed_read(s3):
    store = S3Store(BUCKET, client=s3)
    store.put("landing/a.parquet", b"abc")  # uploaded with a SHA-256 checksum
    s3.put_object(Bucket=BUCKET, Key="landing/b.parquet", Body=b"abc")  # no stored checksum
    assert (
        store.sha256_hex("landing/a.parquet") == store.sha256_hex("landing/b.parquet") == sha256_hex(b"abc")
    )


def test_dedup_on_s3_compares_only_the_latest_accepted_delivery(s3):
    """The Glue job and the local writer share ingestion.manifests.find_accepted_duplicate."""
    store = S3Store(BUCKET, client=s3)
    prefix = "manifests/ingestion/fund_ops/fund/bronze_reference_date=2026-10-26/"
    for run, digest, at in (("a", "A", "1"), ("b", "B", "2")):
        store.put(
            prefix + f"{run}__x.json",
            json.dumps(
                {"status": "ACCEPTED", "input_sha256": digest, "ingested_at": at, "run_id": run}
            ).encode(),
        )
    assert manifests.find_accepted_duplicate(store, "fund_ops", "fund", D1, "A") is None
    assert manifests.find_accepted_duplicate(store, "fund_ops", "fund", D1, "B")["run_id"] == "b"


def test_glue_volume_profiles():
    db_ingest = load_entrypoint("glue/db-gb/job.py")
    assert db_ingest.parts_for("gb", 10_000_000) == 1
    assert db_ingest.parts_for("tb", 0) == 1
    assert db_ingest.parts_for("tb", 2_500_000) == 3
    with pytest.raises(ValueError):
        db_ingest.parts_for("pb", 1)


@pytest.mark.parametrize("unit", ["db-gb", "db-tb", "gold-load"])
def test_glue_sdk_dependencies_are_pinned_to_the_tested_lock(unit):
    root = Path(__file__).resolve().parents[1]
    locked = {p["name"]: p["version"] for p in tomllib.loads((root / "uv.lock").read_text())["package"]}
    requirements = (root / "glue" / unit / "requirements.txt").read_text().splitlines()
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

    from erl_protocol.contracts import parquet_compression

    monkeypatch.setenv("ERL_S3_BUCKET", BUCKET)
    lambda_handlers = load_entrypoint("lambda/external-xml/handler.py")

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
        lambda_handlers.handler(
            {"bucket": BUCKET, "key": key, "run_id": run, "reference_date": D1.isoformat()}, None
        )
    assert [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]] == [key]


class _FailingCopyClient:
    """moto S3 client whose second UploadPartCopy hits a transport error (not a ClientError)."""

    def __init__(self, client, *, abort_fails: bool = False):
        self._client, self._abort_fails, self.copies = client, abort_fails, 0

    def __getattr__(self, name):
        return getattr(self._client, name)

    def upload_part_copy(self, **kwargs):
        from botocore.exceptions import EndpointConnectionError

        self.copies += 1
        if self.copies == 2:
            raise EndpointConnectionError(endpoint_url="https://s3.sa-east-1.amazonaws.com")
        return self._client.upload_part_copy(**kwargs)

    def abort_multipart_upload(self, **kwargs):
        if self._abort_fails:
            raise RuntimeError("abort unavailable")
        return self._client.abort_multipart_upload(**kwargs)


@pytest.fixture
def two_part_source(s3, monkeypatch):
    from erl_protocol import objects

    monkeypatch.setattr(objects, "COPY_PART_BYTES", 5 * 1024 * 1024)
    s3.put_object(Bucket=BUCKET, Key="tmp/big", Body=b"x" * (6 * 1024 * 1024))  # two copy parts
    return s3


def test_transport_failure_during_multipart_copy_aborts_the_upload(two_part_source):
    from botocore.exceptions import EndpointConnectionError

    client = _FailingCopyClient(two_part_source)
    with pytest.raises(EndpointConnectionError):  # the original error, not a wrapped one
        S3Store(BUCKET, client=client).copy_if_absent("tmp/big", "bronze/big")
    assert client.copies == 2
    assert two_part_source.list_multipart_uploads(Bucket=BUCKET).get("Uploads", []) == []
    assert "Contents" not in two_part_source.list_objects_v2(Bucket=BUCKET, Prefix="bronze/")


def test_failed_abort_keeps_the_original_error_and_does_not_claim_cleanup(two_part_source):
    from botocore.exceptions import EndpointConnectionError

    client = _FailingCopyClient(two_part_source, abort_fails=True)
    with pytest.raises(EndpointConnectionError) as exc:
        S3Store(BUCKET, client=client).copy_if_absent("tmp/big", "bronze/big")
    assert any("abort of multipart upload" in n and "also failed" in n for n in exc.value.__notes__)
    assert len(two_part_source.list_multipart_uploads(Bucket=BUCKET)["Uploads"]) == 1  # really pending
    assert "Contents" not in two_part_source.list_objects_v2(Bucket=BUCKET, Prefix="bronze/")
