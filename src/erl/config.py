"""Environment-driven configuration. No secrets have defaults; see .env.example."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from erl.errors import ConfigurationError
from erl.storage import LocalStore, ObjectStore, S3Store

DB_SOURCES = ("fund_ops", "portfolio_risk", "legacy_finance", "exposure_compliance")
FILE_SOURCES = ("market_data", "internal_xml", "external_xml")
ALL_SOURCES = DB_SOURCES + FILE_SOURCES
_PG_SOURCES = {"fund_ops", "portfolio_risk"}


@dataclass(frozen=True)
class Settings:
    mode: str  # "local" (Python ingestion + DuckDB) or "aws" (S3 + Lambda/Glue + Athena)
    data_dir: Path
    source_backend: str  # "sqlite" (no container runtime) or "containers"
    s3_bucket: str | None
    aws_region: str | None
    aws_account_id: str | None
    name_prefix: str

    # ------------------------------------------------------------------ locations
    @property
    def inbox_dir(self) -> Path:
        """Where external parties drop files (CSV/XML). Lives outside the lake."""
        return self.data_dir / "inbox"

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "erl.duckdb"

    @property
    def runs_dir(self) -> Path:
        return self.data_dir / "runs"

    def store(self) -> ObjectStore:
        if self.mode == "aws":
            if not self.s3_bucket:
                raise ConfigurationError("ERL_S3_BUCKET is required in aws mode")
            from erl.aws.runtime import _session

            return S3Store(self.s3_bucket, client=_session(self).client("s3"))
        return LocalStore(self.lake_dir)

    # ------------------------------------------------------------------ databases
    def source_url(self, source: str) -> str:
        override = os.environ.get(f"ERL_{source.upper()}_URL")
        if override:
            return override
        if self.source_backend == "sqlite":
            return f"sqlite:///{self.data_dir / 'sources' / f'{source}.sqlite'}"
        if source in _PG_SOURCES:
            pwd = _require("ERL_PG_PASSWORD")
            host = os.environ.get("ERL_PG_HOST", "localhost")
            return f"postgresql+psycopg://erl:{pwd}@{host}:5432/{source}"
        return _mssql_url(source)

    def consumer_url(self) -> str:
        override = os.environ.get("ERL_CONSUMER_URL")
        if override:
            return override
        if self.source_backend == "sqlite":
            return f"sqlite:///{self.data_dir / 'consumer' / 'consumer_reporting.sqlite'}"
        return _mssql_url("consumer_reporting")


def _mssql_url(database: str) -> str:
    pwd = _require("ERL_MSSQL_SA_PASSWORD")
    host = os.environ.get("ERL_MSSQL_HOST", "localhost")
    return f"mssql+pymssql://sa:{pwd}@{host}:1433/{database}"


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigurationError(f"environment variable {name} is required (see .env.example)")
    return value


def load() -> Settings:
    mode = os.environ.get("ERL_MODE", "local")
    backend = os.environ.get("ERL_SOURCE_BACKEND", "sqlite")
    if mode not in {"local", "aws"}:
        raise ConfigurationError(f"ERL_MODE must be local|aws, got {mode!r}")
    if backend not in {"sqlite", "containers"}:
        raise ConfigurationError(f"ERL_SOURCE_BACKEND must be sqlite|containers, got {backend!r}")
    return Settings(
        mode=mode,
        data_dir=Path(os.environ.get("ERL_DATA_DIR", "data")).resolve(),
        source_backend=backend,
        s3_bucket=os.environ.get("ERL_S3_BUCKET"),
        aws_region=os.environ.get("ERL_AWS_REGION", "sa-east-1"),
        aws_account_id=os.environ.get("ERL_AWS_ACCOUNT_ID"),
        name_prefix=os.environ.get("ERL_NAME_PREFIX", "erl-dev"),
    )
