# Independent review brief (Codex)

## Objective

Review the code and **run** the checks. Do not rely on the README. Find and fix defects locally,
respecting the same prohibitions as the implementer (AGENTS.md "Safety rules"): no AWS apply or
destroy, no paid AWS calls, no container runtime start or install, no system packages, and no
changes outside this repository.

## Intended architecture

Portfolio Case 01 diagram: 7 sources → Glue (DB, GB/TB patterns) and Lambda (B3 CSV, internal
XML, external XML) → S3 Bronze → Athena + dbt → Iceberg Silver/Gold with test gates →
Glue Load → SQL Server reporting. Airflow orchestrates, CloudWatch/SNS alert, Terraform
provisions. Hybrid lab: sources, Airflow and SQL Server are local; AWS never reaches localhost
(Glue Load writes a publication package; the local publisher loads SQL Server).

## Implemented scope

See the status table in [README.md](../README.md#3-architecture). Summary: everything runs end to
end locally with SQLite stand-ins for PostgreSQL/SQL Server and DuckDB for dbt. Lambda handlers
are tested with moto. Glue scripts, Athena/Iceberg, real SQL Server/PostgreSQL and every AWS
service are **implemented but not executed**.

## Reproduce

```bash
uv sync
uv run ruff check src tests airflow glue scripts
uv run pytest -m "not dbt and not integration"
uv run pytest -m "dbt and not integration"
uv run python scripts/collect_evidence.py && git diff --stat evidence/   # timings/ids differ, outcomes must not
scripts/build_lambda.sh
scripts/airflow_local.sh setup && scripts/airflow_local.sh test 2026-10-26
cd infra && terraform fmt -check -recursive
for d in modules/secure_bucket bootstrap environments/dev; do terraform -chdir=$d init -backend=false && terraform -chdir=$d validate && terraform -chdir=$d test; done
```

TFLint and Checkov commands are in docs/TERRAFORM.md.

## Critical functionality to verify

1. **Publication safety**: `src/erl/publish/publisher.py`. Is the replace atomic on SQL Server
   (single transaction, isolation, `with_for_update` rendering under mssql)? Can a failure leave
   partial rows or delete the last valid version? Is `SKIPPED_IDENTICAL` correct when only one
   product changes (the content hash covers both)?
2. **Gate coupling**: `dbt/models/gold/gold_*.sql` depend on both candidates. Confirm with a
   failing test on either candidate that **neither** Gold table changes
   (`tests/test_pipeline_e2e.py::test_08`).
3. **Snapshot semantics**: the `latest_delivery` macro (scope `snapshot` vs `latest`), the
   late-arrival path and the duplicate-detection keys (`files.py`, `database.py`, Glue `db_ingest.py`).
4. **Identity resolution and rejection reasons**: `int_position_checks.sql` ordering of reasons;
   superseded versions are excluded but not counted as rejections.
5. **Decimal exactness** end to end (extraction refuses floats; publisher type checks).
6. **Bronze append-only**: conditional writes locally and on S3 (`IfNoneMatch`), run-scoped keys.
7. **Airflow**: retries by step, failure callback, `report` on `all_done`, watcher marks the
   run failed.
8. **Glue scripts** (not executed): manifest key format must equal
   `bronze.IngestionManifest.key()`; Iceberg catalog configuration in `infra/environments/dev/glue.tf`;
   IAM coverage of every S3 call they make.
9. **Athena compatibility** of the dbt SQL (not executed): `try_cast`, decimal division,
   `list_agg` dispatch, `partitioned_by` on Iceberg, `merge` with `unique_key` list.

## Security criteria

No secrets in git; `.env`, tfstate, plans and backend config are ignored. XML parsing has no DTD
or entity resolution (a DOCTYPE is rejected). SQL uses bound parameters; identifiers come only
from static tables. Lambdas validate bucket and prefix scope. The AWS account guard runs before
any AWS step. IAM has no service-wide wildcards (asserted in `terraform test`).

## Terraform criteria

Provider pinned (`~> 6.71`, lockfile for 4 platforms), `allowed_account_ids`, region explicit,
partial S3 backend with `use_lockfile`, state bootstrap separated with `prevent_destroy`,
buckets private/encrypted/versioned/TLS-only/lifecycle, Athena cutoff, bounded Glue jobs, log
retention, budget, Terraform does not own dbt tables, every Checkov skip is inline with a reason
(docs/TERRAFORM.md). Tests are plan-only with a mocked provider.

## Known limitations

See README §14 and docs/HANDOFF.md "Known issues".

## Evidence available

`evidence/local/` (scenario run reports, consumer queries, pytest summary) and
`evidence/terraform/checks.txt`. There is no AWS evidence.

## Report format

List findings by severity: (1) functional bugs, (2) security, (3) data loss or corruption,
(4) idempotency, (5) incorrect infrastructure, (6) insufficient tests, (7) maintainability,
(8) cosmetic. For each: file:line, failure scenario, fix (and a test that fails before it).
Avoid large refactors without concrete benefit.
