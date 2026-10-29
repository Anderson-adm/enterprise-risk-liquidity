"""Authorial schemas of the four database sources (SQLAlchemy Core, dialect-neutral).

The same definitions create tables on PostgreSQL, SQL Server and SQLite. Views of the legacy
system are plain SQL that runs on all three dialects.

Physical layout (see docker/compose.yaml):
    PostgreSQL instance: databases fund_ops, portfolio_risk
    SQL Server instance: databases legacy_finance, exposure_compliance, consumer_reporting
"""

from __future__ import annotations

from sqlalchemy import (
    CHAR,
    Column,
    Date,
    ForeignKey,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
)

from erl.sqltypes import Dec, Timestamp

# --- Source 1: operational fund database (PostgreSQL) ---------------------------------
fund_ops = MetaData()
Table(
    "fund",
    fund_ops,
    Column("fund_id", String(20), primary_key=True),
    Column("fund_name", String(120), nullable=False),
    Column("fund_type", String(30), nullable=False),
    Column("base_currency", CHAR(3), nullable=False),
    Column("inception_date", Date, nullable=False),
    Column("status", String(10), nullable=False),
)
Table(
    "portfolio",
    fund_ops,
    Column("portfolio_id", String(20), primary_key=True),
    Column("fund_id", String(20), ForeignKey("fund.fund_id"), nullable=False),
    Column("portfolio_name", String(120), nullable=False),
    # which system is the source of record for this portfolio's positions
    Column("position_source_system", String(30), nullable=False),
    Column("status", String(10), nullable=False),
    Column("opened_on", Date, nullable=False),
)
Table(
    "source_identity_map",
    fund_ops,
    Column("source_system", String(30), nullable=False),
    Column("external_identifier", String(40), nullable=False),
    Column("portfolio_id", String(20), ForeignKey("portfolio.portfolio_id"), nullable=False),
    Column("valid_from", Date, nullable=False),
    Column("valid_to", Date, nullable=True),
    PrimaryKeyConstraint("source_system", "external_identifier"),
)

# --- Source 2: portfolio risk database (PostgreSQL) -----------------------------------
portfolio_risk = MetaData()
Table(
    "processing_batch",
    portfolio_risk,
    Column("batch_id", Integer, primary_key=True, autoincrement=False),
    Column("reference_date", Date, nullable=False),
    Column("status", String(20), nullable=False),
    Column("started_at", Timestamp, nullable=False),
    Column("finished_at", Timestamp, nullable=True),
)
Table(
    "processed_position",
    portfolio_risk,
    Column("position_id", Integer, primary_key=True, autoincrement=False),
    Column("portfolio_code", String(30), nullable=False),  # risk system's own code format
    Column("instrument_code", String(20), nullable=False),
    Column("reference_date", Date, nullable=False, index=True),
    Column("quantity", Dec(24, 6), nullable=False),
    Column("market_value", Dec(20, 2), nullable=False),
    Column("currency", CHAR(3), nullable=False),
    Column("processing_status", String(20), nullable=False),
    Column("processing_version", Integer, nullable=False),
    Column("processed_at", Timestamp, nullable=False),
)

# --- Source 3: legacy financial system (SQL Server, exposed through views) ------------
legacy_finance = MetaData()
Table(
    "tb_cart",
    legacy_finance,
    Column("cod_cart", String(10), primary_key=True),
    Column("nm_cart", String(120), nullable=False),
    Column("dt_abert", Date, nullable=False),
)
Table(
    "tb_saldo_caixa",
    legacy_finance,
    Column("cod_cart", String(10), ForeignKey("tb_cart.cod_cart"), nullable=False),
    Column("dt_ref", Date, nullable=False),
    Column("vl_saldo", Dec(20, 2), nullable=False),
    Column("cd_moeda", CHAR(3), nullable=False),
    PrimaryKeyConstraint("cod_cart", "dt_ref"),
)
Table(
    "tb_resgate",
    legacy_finance,
    Column("id_resgate", Integer, primary_key=True, autoincrement=False),
    Column("cod_cart", String(10), ForeignKey("tb_cart.cod_cart"), nullable=False),
    Column("dt_pedido", Date, nullable=False),
    Column("dt_cotizacao", Date, nullable=False),
    Column("vl_resgate", Dec(20, 2), nullable=False),
    Column("st_resgate", String(12), nullable=False),
)
LEGACY_VIEWS = {
    "vw_saldo_caixa": (
        "SELECT s.cod_cart, s.dt_ref, s.vl_saldo, s.cd_moeda "
        "FROM tb_saldo_caixa s JOIN tb_cart c ON c.cod_cart = s.cod_cart"
    ),
    "vw_resgate_pendente": (
        "SELECT r.id_resgate, r.cod_cart, r.dt_pedido, r.dt_cotizacao, r.vl_resgate, r.st_resgate "
        "FROM tb_resgate r JOIN tb_cart c ON c.cod_cart = r.cod_cart "
        "WHERE r.st_resgate <> 'CANCELADO'"
    ),
}

# --- Source 4: exposure & compliance database (SQL Server) ----------------------------
exposure_compliance = MetaData()
Table(
    "instrument_registry",
    exposure_compliance,
    Column("instrument_code", String(20), primary_key=True),
    Column("instrument_name", String(120), nullable=False),
    Column("instrument_type", String(20), nullable=False),
    Column("issuer_code", String(20), nullable=False),
    Column("issuer_name", String(120), nullable=False),
    Column("issuer_type", String(20), nullable=False),
)
Table(
    "stock_position",
    exposure_compliance,
    Column("stock_id", Integer, primary_key=True, autoincrement=False),
    Column("fund_ref", String(30), nullable=False),  # compliance system's identifier
    Column("instrument_code", String(20), nullable=False),
    Column("position_date", Date, nullable=False, index=True),
    Column("quantity", Dec(24, 6), nullable=False),
    Column("book_value", Dec(20, 2), nullable=False),
)
Table(
    "concentration_limit",
    exposure_compliance,
    Column("rule_id", String(20), primary_key=True),
    Column("rule_scope", String(20), nullable=False),
    Column("limit_pct", Dec(9, 6), nullable=False),
    Column("excluded_issuer_type", String(20), nullable=True),
    Column("valid_from", Date, nullable=False),
    Column("valid_to", Date, nullable=True),
)

SOURCE_METADATA = {
    "fund_ops": (fund_ops, {}),
    "portfolio_risk": (portfolio_risk, {}),
    "legacy_finance": (legacy_finance, LEGACY_VIEWS),
    "exposure_compliance": (exposure_compliance, {}),
}
