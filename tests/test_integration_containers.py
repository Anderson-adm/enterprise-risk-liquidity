"""Integration against the real PostgreSQL and SQL Server containers (docker/compose.yaml).

Not run by default. Start the containers, then:
    ERL_IT_CONTAINERS=1 uv run pytest -m integration
Status: written, NOT yet executed (no container runtime was available when it was written).
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from erl import config, pipeline
from erl.sources import seed
from tests.conftest import D1

pytestmark = [
    pytest.mark.integration,
    pytest.mark.dbt,
    pytest.mark.skipif(
        os.environ.get("ERL_IT_CONTAINERS") != "1", reason="set ERL_IT_CONTAINERS=1 with containers up"
    ),
]


@pytest.fixture(scope="module")
def settings(tmp_path_factory):
    os.environ["ERL_DATA_DIR"] = str(tmp_path_factory.mktemp("it") / "data")
    os.environ["ERL_SOURCE_BACKEND"] = "containers"
    os.environ["ERL_MODE"] = "local"
    s = config.load()
    seed.generate_all(s)
    return s


def test_sources_are_real_postgres_and_sqlserver(settings):
    dialects = {src: seed.engine_for(settings.source_url(src)).dialect.name for src in config.DB_SOURCES}
    assert dialects == {
        "fund_ops": "postgresql",
        "portfolio_risk": "postgresql",
        "legacy_finance": "mssql",
        "exposure_compliance": "mssql",
    }


def test_pipeline_publishes_to_sql_server_and_is_idempotent(settings):
    first = pipeline.run_all(D1, settings=settings)
    assert first["status"] == "SUCCEEDED", first
    second = pipeline.run_all(D1, settings=settings)
    assert second["steps"]["publish"]["publication_status"] == "SKIPPED_IDENTICAL"
    engine = seed.engine_for(settings.consumer_url())
    with engine.connect() as conn:
        n = conn.execute(
            text("SELECT count(*) FROM rpt.liquidity_summary WHERE reference_date = :d"), {"d": D1}
        ).scalar()
        dtype = conn.execute(
            text(
                "SELECT DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = 'rpt' AND TABLE_NAME = 'liquidity_summary' "
                "AND COLUMN_NAME = 'total_value'"
            )
        ).one()
    engine.dispose()
    assert n == 8
    assert tuple(dtype) == ("numeric", 20, 2)
