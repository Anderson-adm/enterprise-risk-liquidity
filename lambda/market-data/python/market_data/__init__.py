"""Public local ingestion API; implementation owned by this Lambda unit."""

from .parser import parse

__all__ = ["parse"]
