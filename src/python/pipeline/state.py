"""Run state: ``<data_dir>/runs/<run_id>/state.json``, one entry per step (atomic rewrite)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from sqlalchemy import make_url

from connect.databases import ServerConfig
from pipeline.config import DB_SOURCES, DatabaseTarget, Settings
from utils.errors import ConfigurationError


def state_path(settings: Settings, run_id: str) -> Path:
    return settings.runs_dir / run_id / "state.json"


def load_state(settings: Settings, run_id: str) -> dict:
    path = state_path(settings, run_id)
    return json.loads(path.read_text()) if path.exists() else {"run_id": run_id, "steps": {}}


def run_context(settings: Settings) -> dict:
    """What a run's checkpoints depend on besides run id and reference date: the executors
    (mode), where it reads (source databases) and writes (object store, consumer). The local
    lake and warehouse live in the same data directory as the state, so they are implied.
    Operational settings (batch size, log level, dbt location, AWS profile) are left out, and
    database targets are recorded without passwords."""
    context = {
        "mode": settings.mode,
        "source_backend": settings.source_backend,
        "sources": {s: _target_identity(settings, settings.source_target(s)) for s in DB_SOURCES},
        "consumer": _target_identity(settings, settings.consumer_target()),
    }
    if settings.mode == "aws":
        context |= {
            "s3_bucket": settings.s3_bucket,
            "aws_account_id": settings.aws_account_id,
            "aws_region": settings.aws_region,
            "name_prefix": settings.name_prefix,
        }
    return context


def _target_identity(settings: Settings, target: DatabaseTarget) -> str:
    if isinstance(target, ServerConfig):
        return f"{target.dialect}://{target.username}@{target.host}:{target.port}/{target.database}"
    if isinstance(target, Path):
        inside = target.is_relative_to(settings.data_dir)
        return f"sqlite:{target.relative_to(settings.data_dir) if inside else target}"
    return make_url(target).render_as_string(hide_password=True)


def check_context(state: dict, context: dict, run_id: str) -> None:
    """Checkpoints are reusable only in the context they were recorded in. Refuses (without
    touching the state) a run recorded elsewhere or recorded before contexts existed."""
    recorded = state.get("context")
    if recorded is None:
        if state["steps"]:
            raise ConfigurationError(
                f"run {run_id} has checkpoints but no recorded context (older state); use a new run id"
            )
        return
    if recorded != context:
        changed = sorted(k for k in recorded.keys() | context.keys() if recorded.get(k) != context.get(k))
        raise ConfigurationError(
            f"run {run_id} was recorded in another context ({', '.join(changed)} differ); "
            "its checkpoints are not reused - use a new run id"
        )


def save_step(settings: Settings, run_id: str, ref: date, step: str, outcome: dict) -> None:
    state = load_state(settings, run_id)
    state.setdefault("context", run_context(settings))
    state["reference_date"] = ref.isoformat()
    state["mode"] = settings.mode
    state["steps"][step] = outcome
    path = state_path(settings, run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str))
    tmp.replace(path)
