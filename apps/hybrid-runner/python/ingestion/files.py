"""Local composition adapter: select a public parser and run the versioned acceptance protocol."""

from datetime import date

from erl_protocol import file_acceptance
from erl_protocol.manifests import IngestionManifest
from erl_protocol.objects import ObjectStore
from ingestion.parsers import FILE_SOURCES, PARSERS


def ingest_file(
    store: ObjectStore,
    *,
    source_system: str,
    object_name: str,
    data: bytes,
    run_id: str,
    run_reference_date: date,
    component: str,
    compression: str,
) -> IngestionManifest:
    return file_acceptance.ingest_file(
        store,
        source_system=source_system,
        object_name=object_name,
        data=data,
        run_id=run_id,
        run_reference_date=run_reference_date,
        component=component,
        compression=compression,
        parse=PARSERS[source_system],
        entity_name=FILE_SOURCES[source_system][0],
    )
