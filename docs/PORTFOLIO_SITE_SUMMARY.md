# Portfolio site update summary (Case 01) — input for a later, authorised site edit

The site repository was **not** modified.

## Implemented in the repository

Seven synthetic sources (2 PostgreSQL DBs, 2 SQL Server DBs incl. legacy views, daily CSV,
internal XML, external transfer-in XML); SQL extraction; append-only Bronze with checksums,
manifests and quarantine; dbt Silver/Gold (liquidity + exposure/compliance) with critical test
gates; transactional, idempotent SQL Server publication with a publication log; Airflow DAG;
Lambda handlers for the three file feeds; Glue 5.0 jobs (DB ingestion GB/TB patterns, Gold load);
Terraform for S3, Glue Catalog, Athena workgroup, IAM, Lambda, Glue, CloudWatch/SNS/EventBridge,
budget; GitHub Actions CI.

## Tested locally (real executions, 2026-10-29)

* End-to-end pipeline for 3 reference dates and 8 scenarios (identifier divergence, duplicates,
  unmapped portfolio, invalid XML, late delivery + reprocessing, idempotent re-run, correction of
  a published position, critical quality failure, unavailable source) — on SQLite stand-ins for
  the databases and DuckDB for dbt.
* Airflow 3.3.2 DAG: one successful run and one failed run (quality gate) via `airflow dags test`.
* Lambda handlers against an S3 API mock (moto).
* Terraform: fmt, validate, TFLint, Checkov, `terraform test` with mocked provider.

## Tested on AWS

None yet.

## Planned / pending

Execution with real PostgreSQL/SQL Server containers; AWS lab apply, hybrid run (Lambda, Glue,
Athena + Iceberg), CloudWatch/SNS alert evidence, teardown; CI run on GitHub.

## Measured technical results (this reconstruction)

* ≈ 40 s per successful local run of all steps for one reference date on a Ryzen 5 5600G under
  WSL2 with the repo on `/mnt/c` (dominated by `dbt build`, 67 nodes). Synthetic volumes
  (hundreds of rows), so this is not a performance benchmark.
* 8/8 failure/reprocessing scenarios behave as specified (evidence/local/summary.json).

## Limitations

Synthetic data and rules; tiny volumes; no AWS execution; database engines replaced by SQLite
in the recorded runs.

## Page claims vs. evidence in this repository (factual, no proposals)

| Claim / component on the page | Evidence status here |
|---|---|
| 7 sources integrated into one validated base | validated locally (SQLite stand-ins for the 4 databases) |
| Glue jobs (GB, TB, Load) | implemented, not executed |
| Lambdas (B3 daily, internal XML, external XML) | implemented; handler logic tested with an S3 mock; not executed on AWS |
| Bronze / Silver / Gold on S3 + Iceberg | Bronze/Silver/Gold validated locally (filesystem + DuckDB); S3 + Iceberg not executed |
| Athena + dbt, tests block promotion, invalid data goes to quarantine | dbt + gate validated locally on DuckDB; Athena not executed |
| Airflow, idempotent reprocessing by reference date, failed-step alert | validated locally (Airflow 3.3.2 `dags test`; alert = structured log locally) |
| CloudWatch + SNS | implemented, not executed |
| Terraform | static checks and mocked tests pass; never applied |
| SQL Server "controle e relatórios" | publisher validated locally on SQLite; SQL Server not executed |
| "-80% time to deliver validated data", "one week to one day" | historical results of the original experience; not measured by this reconstruction |
