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
| `docs/`, `evidence/` | documentation; outputs produced by real runs only |

## Commands

```bash
uv sync                                    # install (locked)
uv run ruff check src tests airflow glue scripts && uv run ruff format --check src tests airflow glue scripts
uv run pytest -m "not dbt and not integration"
uv run pytest -m "dbt and not integration" # end-to-end with dbt (~4 min)
uv run erl generate && uv run erl run --date 2026-10-26
uv run python scripts/collect_evidence.py  # regenerates evidence/local
scripts/airflow_local.sh setup|test <date>
cd infra && terraform fmt -check -recursive
terraform -chdir=infra/environments/dev init -backend=false && terraform -chdir=infra/environments/dev validate && terraform -chdir=infra/environments/dev test
.tools/bin/tflint --chdir=infra/environments/dev --config=$PWD/infra/.tflint.hcl   # see docs/TERRAFORM.md
uvx --python 3.12 checkov@3.4.0 -d infra --framework terraform --compact --quiet
```

## Conventions

* Python 3.12 runtime, code compatible with 3.11 (Glue 5.0). Ruff, line length 110.
* Money is `Decimal`/`DECIMAL`; never binary floats. Bronze stores text as received; typing in Silver.
* Business logic in `erl` modules; Airflow operators only call the CLI.
* Bronze and publication packages are append-only (run-scoped keys, conditional writes).
* Any engine-specific SQL goes in `dbt/macros/platform.sql`.
* Contracts: change `bronze_contract.json` / `consumer_contract.json` first, then code; tests
  check consistency with dbt and Terraform.
* Small, coherent commits; end commit messages with the agent attribution line.

## Safety rules (non-negotiable)

* Never run `terraform apply`/`destroy`, create AWS resources, run Glue/Athena with cost, or
  start/install container runtimes or system packages without explicit human authorisation.
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
