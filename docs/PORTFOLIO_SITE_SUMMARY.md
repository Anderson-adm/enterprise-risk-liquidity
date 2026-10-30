# Portfolio site update summary (Case 01) — input for a later, authorised site edit

The site repository was **not** modified.

## Implemented in the repository

Seven synthetic sources (2 PostgreSQL DBs, 2 SQL Server DBs incl. legacy views, daily CSV,
internal XML, external transfer-in XML); SQL extraction; append-only Bronze with checksums,
manifests and quarantine; dbt Silver/Gold (liquidity + exposure/compliance) with critical test
gates; transactional, idempotent SQL Server publication with a publication log; Airflow DAG;
Lambda handlers for the three file feeds; Glue 5.0 jobs (DB ingestion GB/TB patterns, Gold load);
Terraform for S3, Glue Catalog, Athena workgroup, IAM, Lambda, Glue, CloudWatch/SNS/EventBridge,
budget; GitHub Actions CI (green on GitHub, including the PostgreSQL + SQL Server integration job).

## Tested locally (real executions, 2026-10-29)

* End-to-end pipeline for 3 reference dates and 8 scenarios (identifier divergence, duplicates,
  unmapped portfolio, invalid XML, late delivery + reprocessing, idempotent re-run, correction of
  a published position, critical quality failure, unavailable source) — on PostgreSQL 17 and
  SQL Server 2022 containers (Docker Desktop/WSL2) and, separately, on SQLite stand-ins; DuckDB
  for dbt. Same outcomes and identical published values on both backends.
* SQL Server publication under concurrency (4 simultaneous publishers): exactly one ACTIVE
  version, no deadlock; exact DECIMAL limits and catalog types checked on both engines.
* Airflow 3.3.2 DAG: one successful run and one failed run (quality gate) via `airflow dags test`.
* Lambda handlers against an S3 API mock (moto).
* Terraform: fmt, validate, TFLint, Checkov, `terraform test` with mocked provider.

## Tested on AWS

* Terraform bootstrap (7 resources) and dev (69 resources) applied and verified through direct
  AWS reads, then fully destroyed at the owner's request. No Case 01 AWS resources remain.
* Landing uploads succeeded for 2026-10-26; one external-XML Lambda invocation failed while
  writing Parquet because the managed PyArrow layer lacks Zstandard support. No retry.
* CloudWatch recorded the real error and a successful alarm action to SNS. The subscription
  was confirmed; human receipt was not verified.
* **The complete AWS pipeline is not validated:** no Glue run, Athena processing query,
  Iceberg Silver/Gold, AWS Gold product or SQL Server publication followed this attempt.
  Evidence: [first AWS attempt](../evidence/aws/functional/2026-10-26/summary.json),
  [teardown](../evidence/aws/teardown/verification.json).

## Planned / pending

Codec-compatible Lambda packaging, the one-date generator defect and complete AWS execution
remain unresolved. The owner stopped cloud work and removed resources; any future deployment
or paid validation requires a new explicit decision. No further cloud work is authorised.

## Measured technical results (this reconstruction)

* ≈ 40–50 s per successful local run of all steps for one reference date on a Ryzen 5 5600G under
  WSL2 with the repo on `/mnt/c` (dominated by `dbt build`, 67 nodes). Synthetic volumes
  (hundreds of rows), so this is not a performance benchmark.
* 8/8 failure/reprocessing scenarios behave as specified on both backends
  (evidence/local/containers/summary.json, evidence/local/sqlite/summary.json).

## Limitations

Synthetic data and rules; tiny volumes; one failed AWS ingestion attempt, with no complete
AWS pipeline result; databases are local containers, not managed or enterprise instances.

## Page claims vs. evidence in this repository (factual, no proposals)

| Claim / component on the page | Evidence status here |
|---|---|
| 7 sources integrated into one validated base | validated locally (PostgreSQL + SQL Server containers; also SQLite stand-ins) |
| Glue jobs (GB, TB, Load) | implemented, not executed |
| Lambdas (B3 daily, internal XML, external XML) | implemented; moto tested; external XML invoked once on AWS and failed on missing Zstandard codec; others not invoked |
| Bronze / Silver / Gold on S3 + Iceberg | validated locally; AWS has Landing and one uncommitted raw XML only; no committed Bronze delivery or Iceberg tables produced; objects later deleted |
| Athena + dbt, tests block promotion, invalid data goes to quarantine | dbt + gate validated locally on DuckDB; Athena not executed |
| Airflow, idempotent reprocessing by reference date, failed-step alert | validated locally (Airflow 3.3.2 `dags test`; alert = structured log locally) |
| CloudWatch + SNS | real error logs and alarm action to SNS observed; subscription confirmed; human delivery/other alert paths not verified |
| Terraform | local checks pass; 76 resources applied/verified and then destroyed; all cloud resources removed |
| SQL Server "controle e relatórios" | validated locally on a SQL Server 2022 container (transactional, idempotent, concurrent publishers) |
| "-80% time to deliver validated data", "one week to one day" | historical results of the original experience; not measured by this reconstruction |
