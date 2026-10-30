"""Run state: ``<data_dir>/runs/<run_id>/state.json``, one entry per step (atomic rewrite)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from pipeline.config import Settings


def state_path(settings: Settings, run_id: str) -> Path:
    return settings.runs_dir / run_id / "state.json"


def load_state(settings: Settings, run_id: str) -> dict:
    path = state_path(settings, run_id)
    return json.loads(path.read_text()) if path.exists() else {"run_id": run_id, "steps": {}}


def save_step(settings: Settings, run_id: str, ref: date, step: str, outcome: dict) -> None:
    state = load_state(settings, run_id)
    state["reference_date"] = ref.isoformat()
    state["mode"] = settings.mode
    state["steps"][step] = outcome
    path = state_path(settings, run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str))
    tmp.replace(path)
