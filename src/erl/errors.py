"""Failure taxonomy.

Each exception carries a ``category`` that is written to logs and run reports so an
operator can tell an infrastructure outage from a data problem without reading a traceback.
"""

from __future__ import annotations


class ErlError(Exception):
    category = "unexpected"

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message)
        self.details = details or {}  # structured context recorded in the run state


class ConfigurationError(ErlError):
    """Invalid parameters or environment (e.g. malformed reference date, wrong AWS account)."""

    category = "configuration"


class InfrastructureError(ErlError):
    """A dependency (database, object store, AWS service) could not be reached."""

    category = "infrastructure"


class SourceUnavailableError(InfrastructureError):
    """A required source did not deliver data for the reference date."""

    category = "source_unavailable"


class ContractError(ErlError):
    """Input does not satisfy its data contract (schema, XSD, reference date)."""

    category = "contract"


class ObjectExistsError(ErlError):
    """Append-only storage refused to overwrite an existing object."""

    category = "append_only_violation"


class QualityGateError(ErlError):
    """A critical data quality check failed; publication must not happen."""

    category = "quality_gate"


class PublicationError(ErlError):
    """The consumer database publication failed or was refused."""

    category = "publication"
