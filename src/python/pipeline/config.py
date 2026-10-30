"""Environment-driven configuration. No secrets have defaults; see .env.example."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import Engine

from connect.databases import ServerConfig, engine_for, postgres_connect, sqlite_connect, sqlserver_connect
from connect.secrets import get_secret
from storage.objects import LocalStore, ObjectStore, S3Store
from utils.errors import ConfigurationError

DB_SOURCES = ("fund_ops", "portfolio_risk", "legacy_finance", "exposure_compliance")
FILE_SOURCES = ("market_data", "internal_xml", "external_xml")
ALL_SOURCES = DB_SOURCES + FILE_SOURCES
_PG_SOURCES = {"fund_ops", "portfolio_risk"}
AWS_REGIONS = frozenset({"sa-east-1"})
REPO_ROOT = Path(__file__).resolve().parents[3]  # src/python/pipeline/config.py -> repository

DatabaseTarget = str | Path | ServerConfig  # URL override | SQLite file | server database


@dataclass(frozen=True)
class Settings:
    mode: str  # "local" (Python ingestion + DuckDB) or "aws" (S3 + Lambda/Glue + Athena)
    data_dir: Path
    source_backend: str  # "sqlite" (no container runtime) or "containers"
    s3_bucket: str | None
    aws_region: str | None
    aws_account_id: str | None
    name_prefix: str
    dbt_project_dir: Path = REPO_ROOT / "dbt"
    extract_batch_rows: int = 5_000

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

    def aws_session(self):
        from connect.aws import aws_session

        return aws_session(os.environ.get("AWS_PROFILE"), self.aws_region, allowed_regions=AWS_REGIONS)

    def store(self) -> ObjectStore:
        if self.mode == "aws":
            if not self.s3_bucket:
                raise ConfigurationError("ERL_S3_BUCKET is required in aws mode")
            return S3Store(self.s3_bucket, client=self.aws_session().client("s3"))
        return LocalStore(self.lake_dir)

    # ------------------------------------------------------------------ databases
    def _target(self, database: str, override_var: str) -> DatabaseTarget:
        override = os.environ.get(override_var)
        if override:
            return override
        if self.source_backend == "sqlite":
            folder = "consumer" if database == "consumer_reporting" else "sources"
            return self.data_dir / folder / f"{database}.sqlite"
        if database in _PG_SOURCES:
            return ServerConfig(
                "postgresql",
                os.environ.get("ERL_PG_HOST", "localhost"),
                5432,
                database,
                "erl",
                get_secret(os.environ, "ERL_PG_PASSWORD"),
            )
        return ServerConfig(
            "mssql",
            os.environ.get("ERL_MSSQL_HOST", "localhost"),
            1433,
            database,
            "sa",
            get_secret(os.environ, "ERL_MSSQL_SA_PASSWORD"),
        )

    def source_target(self, source: str) -> DatabaseTarget:
        return self._target(source, f"ERL_{source.upper()}_URL")

    def consumer_target(self) -> DatabaseTarget:
        return self._target("consumer_reporting", "ERL_CONSUMER_URL")

    def source_url(self, source: str) -> str:
        return target_url(self.source_target(source))

    def consumer_url(self) -> str:
        return target_url(self.consumer_target())

    def source_engine(self, source: str) -> Engine:
        return connect(self.source_target(source))

    def consumer_engine(self) -> Engine:
        return connect(self.consumer_target())


def target_url(target: DatabaseTarget) -> str:
    if isinstance(target, ServerConfig):
        return target.url().render_as_string(hide_password=False)
    if isinstance(target, Path):
        return f"sqlite:///{target}"
    return target


def connect(target: DatabaseTarget) -> Engine:
    """Engine for a resolved target; the caller disposes it at the end of its unit of work."""
    if isinstance(target, ServerConfig):
        return postgres_connect(target) if target.dialect == "postgresql" else sqlserver_connect(target)
    if isinstance(target, Path):
        return sqlite_connect(target)
    return engine_for(target)


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
        dbt_project_dir=Path(os.environ.get("ERL_DBT_PROJECT_DIR", REPO_ROOT / "dbt")).resolve(),
    )
