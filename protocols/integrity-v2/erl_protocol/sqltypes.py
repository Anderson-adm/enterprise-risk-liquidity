"""Shared SQLAlchemy column types."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import DateTime, Numeric, String, TypeDecorator
from sqlalchemy.dialects import mssql


class Dec(TypeDecorator):
    """Exact decimal. SQLite has no exact decimal type, so the dev fallback stores canonical text."""

    impl = Numeric
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "sqlite":
            return dialect.type_descriptor(String(48))
        return dialect.type_descriptor(Numeric(self.impl.precision, self.impl.scale, asdecimal=True))

    def process_bind_param(self, value, dialect):
        if dialect.name == "sqlite" and value is not None:
            return format(Decimal(value), "f")
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == "sqlite" and value is not None:
            return Decimal(value)
        return value


# SQL Server's legacy DATETIME rounds to 1/300 s; DATETIME2 keeps microseconds like PostgreSQL.
Timestamp = DateTime().with_variant(mssql.DATETIME2(), "mssql")
