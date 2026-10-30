# Independent review brief (Codex)

> **Historical document.** File links and paths below refer to the layout before the
> 2026-10-30 refactoring (`src/erl/...`, `glue/jobs/...`); see
> [REFACTORING.md](REFACTORING.md) for where each module lives now. Content is unchanged.

## Objective

Review the code and **run** the checks. Do not rely on the README. Find and fix defects locally,
respecting the same prohibitions as the implementer (AGENTS.md "Safety rules"): no AWS apply or
destroy, no paid AWS calls, no container runtime install or reconfiguration, no system packages,
and no changes outside this repository. If this project's database containers are already running
(or the owner authorises starting them), run the integration suite too; otherwise rely on the CI
`integration` job, which starts the same containers on the GitHub runner.

## Intended architecture

Portfolio Case 01 diagram: 7 sources → Glue (DB, GB/TB patterns) and Lambda (B3 CSV, internal
XML, external XML) → S3 Bronze → Athena + dbt → Iceberg Silver/Gold with test gates →
Glue Load → SQL Server reporting. Airflow orchestrates, CloudWatch/SNS alert, Terraform
provisions. Hybrid lab: sources, Airflow and SQL Server are local; AWS never reaches localhost
(Glue Load writes a publication package; the local publisher loads SQL Server).

## Implemented scope

See the status table in [README.md](../README.md#3-architecture). Summary: everything runs end to
end locally on **PostgreSQL 17 + SQL Server 2022 containers** and, separately, on SQLite stand-ins,
with DuckDB for dbt. Lambda handlers are tested with moto. Glue scripts, Athena/Iceberg and every
AWS service are **implemented but not executed**.

## Reproduce

```bash
uv sync
uv run ruff check src tests airflow glue scripts
uv run pytest -m "not dbt and not integration"
uv run pytest -m "dbt and not integration"
ERL_SOURCE_BACKEND=sqlite uv run python scripts/collect_evidence.py && git diff --stat evidence/   # timings/ids differ, outcomes must not
# with the containers running (docs/LOCAL_RUNTIME.md):
set -a; . ./.env; set +a
ERL_IT_CONTAINERS=1 uv run pytest -m integration
uv run python scripts/collect_evidence.py && uv run python scripts/collect_db_evidence.py
scripts/build_lambda.sh
scripts/airflow_local.sh setup && scripts/airflow_local.sh test 2026-10-26
cd infra && terraform fmt -check -recursive
for d in modules/secure_bucket bootstrap environments/dev; do terraform -chdir=$d init -backend=false && terraform -chdir=$d validate && terraform -chdir=$d test; done
```

TFLint and Checkov commands are in docs/TERRAFORM.md.

## Critical functionality to verify

1. **Publication safety**: `src/erl/publish/publisher.py`. The replace runs in one transaction
   serialised by `_acquire_publication_lock` (`sp_getapplock` / `pg_advisory_xact_lock`); the
   filtered unique index is the last line of defence. Can a failure leave partial rows or delete
   the last valid version? Are readers of `rpt.*` blocked during the replace (READ COMMITTED
   without RCSI)? Is `SKIPPED_IDENTICAL` correct when only one product changes (the content hash
   covers both)? `publication_id` uses an attempt count read outside the lock — can it collide?
2. **Gate coupling**: `dbt/models/gold/gold_*.sql` depend on both candidates. Confirm with a
   failing test on either candidate that **neither** Gold table changes
   (`tests/test_pipeline_e2e.py::test_08`).
3. **Snapshot semantics**: the `latest_delivery` macro (scope `snapshot` vs `latest`), the
   late-arrival path and the duplicate-detection keys (`files.py`, `database.py`, Glue `db_ingest.py`).
4. **Identity resolution and rejection reasons**: `int_position_checks.sql` ordering of reasons;
   superseded versions are excluded but not counted as rejections.
5. **Decimal exactness** end to end (extraction refuses floats; publisher type checks; the
   integration suite round-trips the maximum value of every NUMERIC(p,s) on both engines).
   Bronze text must be identical across engines (`test_extraction_is_identical_to_the_sqlite_stand_in`).
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

## Defects already found and fixed (do not re-report; do check the fixes)

Listed with the test that exposes each one in docs/HANDOFF.md ("Defects found and fixed").
The ones found with the real databases on 2026-10-29: SQL Server ignored `FOR UPDATE` and
concurrent publishers deadlocked (fixed with an application lock); `DATETIME` rounding on SQL
Server (now DATETIME2); different Bronze text for timestamps between SQLite and the real engines
(typed result columns); PostgreSQL consumer without `rpt`/`stg` schemas; `ensure_databases`
ignored database names given by URL overrides.

## Known limitations

See README §14 and docs/HANDOFF.md "Known issues".

## Evidence available

`evidence/local/sqlite/` and `evidence/local/containers/` (8 scenarios: run reports, consumer
queries), `evidence/databases/{sqlite,postgresql,sqlserver}.json` (server versions, catalog types,
views, row counts), `evidence/tests/` (pytest outputs), `evidence/terraform/checks.txt`.
There is no AWS evidence.

## Report format

List findings by severity: (1) functional bugs, (2) security, (3) data loss or corruption,
(4) idempotency, (5) incorrect infrastructure, (6) insufficient tests, (7) maintainability,
(8) cosmetic. For each: file:line, failure scenario, fix (and a test that fails before it).
Avoid large refactors without concrete benefit.
