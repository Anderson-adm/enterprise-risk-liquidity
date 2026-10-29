# AGENTS.md — shared instructions for Claude Code, Codex and humans

## Goal

A small, complete, honest data platform for Case 01 (Enterprise Risk & Liquidity): seven
synthetic sources → Bronze → dbt Silver/Gold → quality gate → transactional SQL Server
publication, orchestrated by Airflow, with AWS infrastructure in Terraform. Architecture
reference: the portfolio site's Case 01 diagram (Glue, Lambda, S3/Iceberg, Athena + dbt,
Airflow, CloudWatch/SNS, Terraform, SQL Server consumer). Develop locally; validate AWS only in
controlled, authorised runs.

## Current architecture (where things live)

| Path | What |
|---|---|
| `src/erl/contracts/` | Bronze contract (JSON, also read by Terraform), consumer contract, XSDs |
| `src/erl/sources/` | source schemas, synthetic generator, seeding/scenarios, SQL extraction |
| `src/erl/ingestion/` | Bronze writer, manifests, quarantine; file (CSV/XML) and DB-snapshot ingestion |
| `src/erl/publish/` | publication package, transactional publisher |
| `src/erl/pipeline.py` | step functions + run state; `src/erl/cli.py` = `erl` CLI |
| `src/erl/aws/` | Lambda handlers, hybrid AWS runtime (Lambda/Glue invocation, account guard) |
| `dbt/` | staging → silver (incl. rejected records) → gold candidates (tested) → gold |
| `glue/jobs/` | Glue 5.0 PySpark jobs (db_ingest, gold_load) |
| `airflow/dags/` | DAG calling `erl step <name>`; Airflow has its own venv |
| `infra/` | `bootstrap/` (state), `environments/dev/` (lab), `modules/secure_bucket/` |
| `docker/compose.yaml` | local PostgreSQL 17 + SQL Server 2022 (sources 1–4 and the consumer) |
| `docs/`, `evidence/` | documentation; outputs produced by real runs only (`evidence/local/<backend>/`, `evidence/databases/`, `evidence/tests/`) |

## Commands

```bash
uv sync                                    # install (locked)
uv run ruff check src tests airflow glue scripts && uv run ruff format --check src tests airflow glue scripts
uv run pytest -m "not dbt and not integration"
uv run pytest -m "dbt and not integration" # end-to-end with dbt (~4 min)
uv run erl generate && uv run erl run --date 2026-10-26
uv run python scripts/collect_evidence.py  # regenerates evidence/local/<ERL_SOURCE_BACKEND>/
# real databases (containers must already be running; see docs/LOCAL_RUNTIME.md):
set -a; . ./.env; set +a
ERL_IT_CONTAINERS=1 uv run pytest -m integration           # isolated it_* databases
uv run python scripts/collect_evidence.py && uv run python scripts/collect_db_evidence.py   # ev_* databases
scripts/airflow_local.sh setup|test <date>
cd infra && terraform fmt -check -recursive
terraform -chdir=infra/environments/dev init -backend=false && terraform -chdir=infra/environments/dev validate && terraform -chdir=infra/environments/dev test
.tools/bin/tflint --chdir=infra/environments/dev --config=$PWD/infra/.tflint.hcl   # see docs/TERRAFORM.md
uvx --python 3.12 checkov@3.4.0 -d infra --framework terraform --compact --quiet
```

## Conventions

* Python 3.12 runtime, code compatible with 3.11 (Glue 5.0). Ruff, line length 110.
* Money is `Decimal`/`DECIMAL`; never binary floats. Bronze stores canonical text (ISO 8601 dates and
  timestamps, plain decimals); typing in Silver. Extraction types its result columns from the source
  metadata so every engine yields the same text (SQLite would otherwise return raw stored strings).
* Timestamps use `erl.sqltypes.Timestamp` (DATETIME2 on SQL Server; legacy DATETIME rounds to 1/300 s).
* Database tests take the `db_settings` fixture: they run on SQLite by default and on the real
  containers with `-m integration` (`it_*` databases). Behaviour that differs by engine needs a test
  in `tests/test_integration_containers.py`.
* Business logic in `erl` modules; Airflow operators only call the CLI.
* Bronze and publication packages are append-only (run-scoped keys, conditional writes).
* Any engine-specific SQL goes in `dbt/macros/platform.sql`.
* Contracts: change `bronze_contract.json` / `consumer_contract.json` first, then code; tests
  check consistency with dbt and Terraform.
* Small, coherent commits; end commit messages with the agent attribution line.

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
