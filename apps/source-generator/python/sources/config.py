"""Public configuration shape consumed by the source generator; caller owns connections."""

from pathlib import Path
from typing import Protocol

from sqlalchemy import Engine


class SourceEnvironment(Protocol):
    source_backend: str

    @property
    def inbox_dir(self) -> Path: ...
    def source_url(self, source: str) -> str: ...
    def consumer_url(self) -> str: ...
    def source_engine(self, source: str) -> Engine: ...
