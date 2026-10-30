# AGENTS.md — shared instructions for Claude Code, Codex and humans

## Goal

A small, complete, honest data platform for Case 01 (Enterprise Risk & Liquidity): seven
synthetic sources → Bronze → dbt Silver/Gold → quality gate → transactional SQL Server
publication, orchestrated by Airflow, with AWS infrastructure in Terraform. Architecture
reference: the portfolio site's Case 01 diagram (Glue, Lambda, S3/Iceberg, Athena + dbt,
Airflow, CloudWatch/SNS, Terraform, SQL Server consumer). Develop locally; validate AWS only in
controlled, authorised runs.

## Current architecture (where things live)

Layout follows the `data-project-engineering` skill; mapping and decisions: [docs/REFACTORING.md](docs/REFACTORING.md).
`src/python` and `src/spark` are import roots (each subdirectory is a top-level package; there is
no project wrapper package). Import as `from ingestion import files`, `from pipeline import steps`.

| Path | What |
|---|---|
| `src/contracts/` | Bronze contract (JSON, also read by Terraform; declares the Parquet codec), consumer contract, XSDs - data only |
| `src/sql/extract/`, `src/sql/load/` | versioned SQL: source extraction/readiness, warehouse report, Gold export, publication locks/schemas |
| `src/python/connect/` | `postgres_connect`, `sqlserver_connect`, `engine_for`, `get_secret`, AWS SSO session + account guard |
| `src/python/extract/` | generic chunked SQL executor; landing snapshot writer |
| `src/python/transform/` | pure Bronze record shaping (canonical text, row hashes, metadata columns) |
| `src/python/ingestion/` | key layout, manifests (commit markers), duplicates, quarantine; file and DB-snapshot acceptance |
| `src/python/storage/` | `LocalStore`/`S3Store` (conditional writes, conditional server-side copy); Parquet bytes + codec proof |
| `src/python/load/` | publication package (local builder, reader/validator, shared manifest); transactional publisher |
| `src/python/utils/` | errors, structured log, run ids, checksums, contract access, SQL file loader, SQL types |
| `src/python/pipeline/` | `Settings`, extraction catalog (entity -> SQL file), steps + run state, hybrid AWS calls, `erl` CLI |
| `src/python/sources/` | simulated source systems: schemas, synthetic generator, seeding, scenarios |
| `src/spark/{readers,transforms,writers}/` | native PySpark operations for the Glue jobs (no UDFs) |
| `lambda/bronze/file_ingestion.py` | thin Lambda handlers (one per file source) |
| `glue/bronze/db_ingest.py`, `glue/load/gold_load.py` | thin Glue 5.0 entry points; code from the library ZIP |
| `scripts/build/` | `build_artifacts.py` -> `dist/` (+ `manifest.json`), `smoke_artifacts.py` (tests the built files) |
| `dbt/` | staging -> silver (incl. rejected records) -> gold candidates (tested) -> gold. dbt owns Silver/Gold SQL |
| `airflow/dags/` | DAG calling `erl step <name>`; Airflow has its own venv |
| `infra/` | `bootstrap/` (state), `environments/dev/` (lab, artifacts bucket), `modules/secure_bucket/` |
| `docker/compose.yaml` | local PostgreSQL 17 + SQL Server 2022 (sources 1-4 and the consumer) |
| `docs/`, `evidence/` | documentation; outputs produced by real runs only (`evidence/local/<backend>/`, `evidence/databases/`, `evidence/tests/`, `evidence/refactor/`) |

## Commands

```bash
uv sync                                    # install (locked)
uv run ruff check src tests airflow glue lambda scripts && uv run ruff format --check src tests airflow glue lambda scripts
uv run pytest -m "not dbt and not integration"
uv run pytest -m "dbt and not integration" # end-to-end with dbt (~6 min)
uv run python scripts/build/build_artifacts.py && uv run python scripts/build/smoke_artifacts.py   # dist/
uv run erl generate && uv run erl run --date 2026-10-26
uv run python scripts/collect_evidence.py  # regenerates evidence/local/<ERL_SOURCE_BACKEND>/
# real databases (containers must already be running; see docs/LOCAL_RUNTIME.md):
set -a; . ./.env; set +a
ERL_IT_CONTAINERS=1 uv run pytest -m integration           # isolated it_* databases
uv run python scripts/collect_evidence.py && uv run python scripts/collect_db_evidence.py   # ev_* databases
scripts/airflow_local.sh setup|test <date>
scripts/terraform_checks.sh   # fmt, validate, mocked tests, TFLint, Checkov on a temporary copy, AWS credentials hidden
# Never run terraform directly in infra/environments/dev on this machine: its .terraform/ still
# holds S3 backend metadata from the destroyed lab and would try to refresh SSO credentials.
```

## Conventions

* Python 3.12 runtime, code compatible with 3.11 (Glue 5.0). Ruff, line length 110.
* Money is `Decimal`/`DECIMAL`; never binary floats. Bronze stores canonical text (ISO 8601 dates and
  timestamps, plain decimals); typing in Silver. Extraction types its result columns from the source
  metadata so every engine yields the same text (SQLite would otherwise return raw stored strings).
* Timestamps use `utils.sqltypes.Timestamp` (DATETIME2 on SQL Server; legacy DATETIME rounds to 1/300 s).
* SQL lives in `src/sql` (values as binds; identifiers only through `utils.sql_files.render_identifiers`
  with a contract allowlist). The publisher keeps SQLAlchemy Core DML built from the consumer
  contract. dbt SQL stays in `dbt/`.
* Functions receive their operational values (codec, batch size, SQL text, keys) from the caller;
  `pipeline` composes. No module-level AWS calls or logging configuration in entry points.
* Parquet codec: `bronze_contract.json` `parquet_compression`; writers prove it with
  `storage.parquet.require_codec` before the first write of a delivery.
* Artifacts: `dist/` is generated (git-ignored). Terraform only uploads/references it; change the
  build in `scripts/build/build_artifacts.py` and keep `tests/test_layout_and_artifacts.py` green.
* Synthetic dates: scenario roles are positional (D1 clean, D2 problems, D3 late arrival).
* Database tests take the `db_settings` fixture: they run on SQLite by default and on the real
  containers with `-m integration` (`it_*` databases). Behaviour that differs by engine needs a test
  in `tests/test_integration_containers.py`.
* Business logic in the `src/python` packages; Airflow operators only call the CLI.
* Bronze and publication packages are append-only (run-scoped keys, conditional writes).
* Any engine-specific SQL goes in `dbt/macros/platform.sql`.
* Contracts: change `bronze_contract.json` / `consumer_contract.json` first, then code; tests
  check consistency with dbt and Terraform.
* Small, coherent commits; end commit messages with the agent attribution line.
* The working copy lives on a Windows drive (`core.filemode=false`): mark new scripts executable
  with `git update-index --chmod=+x <file>`, or CI fails with exit code 126.

## Safety rules (non-negotiable)

* Never run `terraform apply`/`destroy`, create AWS resources, run Glue/Athena with cost, or
  start/install container runtimes or system packages without explicit human authorisation.
  (On 2026-10-29 the owner authorised running this project's two database containers locally;
  that does not cover installing or reconfiguring Docker/WSL, `.wslconfig`, or other projects'
  containers and volumes — never remove volumes you did not create.)
* AWS account: data-dev only (`allowed_account_ids`); region sa-east-1; SSO credentials only.
  Never use the `default` AWS profile (it holds static keys). Never touch data-prod.
* No secrets, credentials, tfstate, plans or `.env` in git.
* No real/proprietary data, names or schemas. Synthetic rules are not regulatory rules.
* Never claim AWS validation without real AWS evidence; label status honestly
  (Validated locally / Validated on AWS / Implemented, not validated / Planned).
* Do not modify the portfolio site repository.

## Acceptance criteria (summary)

Seven sources contribute; extraction → Bronze → Silver → two Gold products → consumer works;
idempotent re-runs; reference date parameterised; critical failures block publication and keep
the last valid version; automated tests pass; Terraform fmt/validate/TFLint/Checkov/tests pass;
CI defined; README verifiable; limitations explicit. Current state: [docs/HANDOFF.md](docs/HANDOFF.md).
