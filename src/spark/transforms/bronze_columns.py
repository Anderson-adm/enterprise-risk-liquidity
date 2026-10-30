"""Bronze shaping for Spark: constant metadata columns, contract order and types.

Native column expressions only; the caller supplies names, values and order (from the Bronze
contract), so these functions know no source or entity.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def with_constant_columns(df: DataFrame, values: Mapping[str, str]) -> DataFrame:
    """Add/replace text columns holding one value for every row (delivery metadata)."""
    return df.withColumns({name: F.lit(value).cast("string") for name, value in values.items()})


def select_in_order(df: DataFrame, order: Sequence[str], bigint_columns: frozenset[str]) -> DataFrame:
    """Columns in ``order``; ``bigint_columns`` as BIGINT, every other column as STRING."""
    return df.select(*[F.col(c).cast("bigint" if c in bigint_columns else "string").alias(c) for c in order])


def single_row(spark: SparkSession, schema: StructType, values: Sequence[object]) -> DataFrame:
    """A one-row DataFrame, e.g. the empty-snapshot marker (business columns NULL, row 0)."""
    if len(values) != len(schema.fields):
        raise ValueError("one value per schema field is required")
    return spark.createDataFrame([tuple(values)], schema)
