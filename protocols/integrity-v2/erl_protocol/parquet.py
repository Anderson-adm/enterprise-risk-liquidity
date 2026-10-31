"""Parquet serialisation with an explicit codec, and proof that the runtime supports it.

The codec comes from the caller (normally ``utils.contracts.parquet_compression()``).
"""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq

from erl_protocol.errors import ConfigurationError


def table_to_parquet_bytes(table: pa.Table, compression: str) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)
    return buf.getvalue()


def parquet_bytes_to_table(data: bytes) -> pa.Table:
    return pq.read_table(io.BytesIO(data))


def require_codec(compression: str) -> str:
    """Prove that this runtime's PyArrow can write *and* read ``compression``.

    Callers run it before the first object of a delivery is written, so an incompatible runtime
    fails without leaving partial objects. ``import pyarrow`` or ``Codec.is_available`` is not
    proof: the AWS-managed pandas layer imported fine and then failed with "Support for codec
    'zstd' not built" (AWS attempt of 2026-10-30).
    """
    probe = pa.table({"probe": pa.array(["x", None], pa.string())})
    try:
        restored = parquet_bytes_to_table(table_to_parquet_bytes(probe, compression))
    except (pa.ArrowException, NotImplementedError, OSError) as exc:
        raise ConfigurationError(
            f"pyarrow {pa.__version__} in this runtime cannot use Parquet codec {compression!r}: {exc}"
        ) from exc
    if not restored.equals(probe):
        raise ConfigurationError(f"Parquet codec {compression!r} round-trip changed the data")
    return compression
