"""SQLAlchemy engines for the source and consumer databases.

The caller owns the engine: open connections with ``with engine.connect()``/``engine.begin()``
for the unit of work and call ``engine.dispose()`` at its end. Engines are never shipped to
Spark workers (Spark/JDBC has its own lifecycle).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import Engine, create_engine, make_url
from sqlalchemy.engine import URL

DRIVERS = {"postgresql": "postgresql+psycopg", "mssql": "mssql+pymssql"}


@dataclass(frozen=True)
class ServerConfig:
    """One database on a PostgreSQL or SQL Server instance (password resolved by the caller)."""

    dialect: str  # "postgresql" | "mssql"
    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)

    def url(self) -> URL:
        if self.dialect not in DRIVERS:
            raise ValueError(f"unsupported dialect {self.dialect!r}")
        return URL.create(
            DRIVERS[self.dialect],
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )


def postgres_connect(config: ServerConfig) -> Engine:
    if config.dialect != "postgresql":
        raise ValueError("postgres_connect needs a postgresql ServerConfig")
    return create_engine(config.url())


def sqlserver_connect(config: ServerConfig) -> Engine:
    if config.dialect != "mssql":
        raise ValueError("sqlserver_connect needs an mssql ServerConfig")
    return create_engine(config.url())


def sqlite_connect(path: Path) -> Engine:
    """SQLite stand-in used when no container runtime is available (development/tests)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(URL.create("sqlite", database=str(path)))


def engine_for(url: str | URL) -> Engine:
    """Engine for a full URL (configuration overrides, isolated test databases)."""
    u = make_url(url)
    if u.get_backend_name() == "sqlite" and u.database:
        Path(u.database).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(u)
