# HANDOFF

_Last update: 2026-10-29 by Claude Code (Opus 5.5). Branch `main`; nothing pushed yet._

## State

Milestones 1–3 are implemented. Milestone 4 (AWS) has not started and needs authorisation.

| Area | State |
|---|---|
| 7 sources, generator, 8 scenarios | done; validated locally on SQLite stand-ins |
| Extraction → landing → Bronze (manifests, checksums, duplicates, quarantine) | done; validated locally + moto |
| dbt Silver/Gold (DuckDB), quality gate, rejected records | done; validated locally |
| Publication package + transactional/idempotent publisher | done; validated on SQLite |
| Airflow DAG (local 3.3.2) | done; success + failure runs validated with `airflow dags test` |
| Lambda handlers + package | done; tested with moto; not run on AWS |
| Glue jobs (db_ingest GB/TB, gold_load) | written; **not executed** |
| Terraform bootstrap + dev + module | done; fmt/validate/TFLint/Checkov/`terraform test` pass; **never applied** |
| docker compose (PostgreSQL + SQL Server) | written; **not executed** (engine not running) |
| CI (GitHub Actions) | written; not yet run on GitHub |

## Tests executed (2026-10-29, local)

See `evidence/local/pytest.txt` and `evidence/terraform/checks.txt`.
* pytest (not integration): unit, contract, moto and 9 end-to-end dbt scenarios.
* `scripts/collect_evidence.py`: 8/8 scenarios with the expected outcomes.
* Airflow: `scripts/airflow_local.sh test 2026-10-26` → success; after degrading market data
  → DagRun failed at `gate`, alert logged, no publication, last version kept.
* Terraform: fmt, validate (3 dirs), 11 mocked tests, TFLint 0 issues, Checkov 148 passed /
  0 failed / 33 justified skips.

## Defects found and fixed during implementation

* XML with a DOCTYPE/external entity crashed lxml validation (would have crashed the Lambda) →
  DOCTYPE rejected as `DTD_NOT_ALLOWED`, validation errors become quarantine.
* A critical test failing on one Gold candidate still let the other Gold table advance →
  both final Gold models now depend on both candidates.
* Airflow DagRun showed `success` after a failed step (leaf `report` with `all_done`) → watcher task.
* Glue `gold-load` role lacked access to its Spark temporary prefix → scoped permission added.
* Two concurrent first publications of a date could both become ACTIVE (no row to lock) →
  filtered unique index on `publication_log(reference_date) WHERE status = 'ACTIVE'`.

## Known issues / limitations

* No container runtime was running: PostgreSQL/SQL Server code paths (and
  `tests/test_integration_containers.py`) are unexecuted. Docker Desktop is installed but the
  engine is stopped and WSL integration is off.
* Athena/Iceberg compatibility of the dbt SQL is unverified; Glue scripts never ran.
* AWS SSO tokens for `data-dev` were expired at preflight; the `default` profile holds static
  access keys (recommend removing/rotating them; the project never uses `default`).
* Athena `merge` does not delete vanished rows (mitigated: Load exports only current-run rows).
* The local Airflow uses SQLite metadata (dev only).

## AWS resources

None created. Observed AWS cost: none.

## Next concrete step

1. Owner decision: enable Docker Desktop WSL integration and start the engine, then run
   `docker compose -f docker/compose.yaml up -d --wait`, set `ERL_SOURCE_BACKEND=containers`,
   run `uv run erl generate`, `ERL_IT_CONTAINERS=1 uv run pytest -m integration`, and
   `scripts/collect_evidence.py` (evidence will then say "containers").
2. Push to GitHub and confirm CI is green.
3. Milestone 4 following docs/AWS_VALIDATION.md (each apply/destroy authorised by the owner).
