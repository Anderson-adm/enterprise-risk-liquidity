"""Secret resolution at the edge of a run. Values are never logged or put in error messages."""

from __future__ import annotations

from typing import Protocol

from utils.errors import ConfigurationError


class SecretProvider(Protocol):
    """Anything with ``get(key) -> str | None``: ``os.environ`` locally (git-ignored ``.env``),
    or an adapter over a secrets service injected by an entry point."""

    def get(self, key: str, /) -> str | None: ...


def get_secret(provider: SecretProvider, key: str) -> str:
    value = provider.get(key)
    if not value:
        raise ConfigurationError(f"secret {key} is not set (see .env.example)")
    return value
