# Refactoring map (2026-10-30)

Baseline: `4bc1f65` (clean tree). The layout follows the `data-project-engineering` skill:
`src/python` and `src/spark` are import roots (no redundant project package), SQL lives in
`src/sql`, contracts in `src/contracts`, runtime entry points in `glue/` and `lambda/`.
Business behaviour, contracts, Bronze keys and publication semantics are unchanged; the two
defects fixed beforehand (Parquet codec on Lambda, single-date generation) are described in
[HANDOFF.md](HANDOFF.md).

| Before (`src/erl/...`) | After | Responsibility |
|---|---|---|
| `config.py` | `src/python/pipeline/config.py` | environment → `Settings`, connection URLs |
| — (`_require` env lookups) | `src/python/connect/secrets.py` | `get_secret(provider, key)` |
| `sources/seed.py::engine_for` | `src/python/connect/databases.py` | `postgres_connect`, `sqlserver_connect`, `engine_for` |
| `aws/runtime.py::_session`, account guard | `src/python/connect/aws.py` | SSO/runner session, expected-account check |
| `aws/runtime.py` (Lambda/Glue calls) | `src/python/pipeline/aws_services.py` | hybrid AWS orchestration |
| `pipeline.py` | `src/python/pipeline/{steps,state,extraction_catalog}.py` | step composition, run state, entity -> SQL file |
| `cli.py` | `src/python/pipeline/cli.py` | `erl` command |
| `sources/extract.py` (SQL strings) | `src/python/extract/{sql_query,landing_snapshot}.py` + `src/sql/extract/*.sql` | generic chunked query executor; landing snapshot writer |
| `ingestion/bronze.py` | `src/python/ingestion/{layout,manifests}.py`, `src/python/transform/bronze_records.py` | keys/manifests/quarantine; pure record shaping |
| `ingestion/files.py`, `ingestion/database.py` | `src/python/ingestion/{parsers,files,snapshot_manifests,snapshots}.py` | parsing/XSD; file acceptance; snapshot steps shared with Glue; local snapshot writer |
| `storage.py` | `src/python/storage/objects.py`, `src/python/storage/parquet.py` | object I/O; Parquet bytes + codec check |
| `publish/package.py`, `publish/publisher.py` | `src/python/load/{package,package_manifest,publisher}.py` + `src/sql/load/*.sql` | package, transactional publication |
| `errors.py`, `log.py`, `sqltypes.py` | `src/python/utils/{errors,structured_log,sqltypes,run_ids,checksums,contracts}.py` | transversal helpers |
| `contracts/*.json, *.xsd` | `src/contracts/` | data contracts (no functions) |
| `sources/{schemas,synthetic}.py`, seeding | `src/python/sources/` | simulated source systems (dev/test data) |
| `aws/lambda_handlers.py` | `lambda/bronze/file_ingestion.py` | thin Lambda handlers |
| `glue/jobs/db_ingest.py` | `glue/bronze/db_ingest.py` + `src/spark/{readers,transforms,writers}` | thin Glue entry point |
| `glue/jobs/gold_load.py` | `glue/load/gold_load.py` + `src/spark/...` | thin Glue entry point |
| `scripts/build_lambda.sh` | `scripts/build/build_artifacts.py` → `dist/` | pre-Terraform build + manifest |

No `glue/silver` or `glue/gold`: dbt (DuckDB locally, Athena/Iceberg on AWS) owns Silver/Gold.
No `src/sql/transform`: transformation SQL belongs to `dbt/`.

## Decisions

* **SQLAlchemy Core stays for contract-driven DML** in the publisher (insert/select/delete built
  from `consumer_contract.json` table metadata). Hand-written SQL text moved to `src/sql`.
  Identifiers that SQL cannot bind (Gold model, columns) are validated against the contract
  before rendering.
* **Exception: simulated source DDL stays in `sources/`.** Creating the synthetic databases,
  tables and legacy views (`sources/seed.py`, `sources/schemas.py`) is test-fixture tooling for
  the fake source systems, not pipeline SQL; database names are validated by a regex before
  DDL. Evidence scripts keep their small report queries inline for the same reason.
* **Artifacts:** `dist/` is built and smoke-tested before any Terraform plan. A separate
  versioned artifacts bucket holds Glue scripts, the Glue library ZIP and the Lambda ZIP
  (the Lambda ZIP is above the 50 MB direct-upload limit because it bundles PyArrow).
  Stable keys per project/service; the Terraform state bucket is never used for artifacts.
* **Contracts and SQL travel with the code.** `src/contracts` and `src/sql` are resource
  packages inside the wheel, the Lambda ZIP and the Glue library, read with
  `importlib.resources` (works from zipimport). The Glue jobs no longer read a separate contract
  copy from S3 (`--contract_s3_key`/`--consumer_contract_s3_key` removed).
* **Shared acceptance steps.** The Glue `db_ingest` job and the local writer compose the same
  `ingestion.snapshot_manifests` functions; Glue previously had its own copies of manifest keys,
  duplicate detection and checksum logic.
* **No bytes through the Spark driver.** Glue commits Spark parts with
  `S3Store.copy_if_absent` (UploadPartCopy + `CompleteMultipartUpload` with `If-None-Match`)
  instead of GET + conditional PUT. Verified with moto only.
* **Gold Load single file is bounded.** `coalesce(1)` stays (one Parquet object per product, as
  the publisher expects) after an explicit row-count check against `--max_product_rows`.
* **Editable install.** hatch `dev-mode-dirs` puts the three roots on `sys.path` in development
  (which also exposes empty `python`/`spark` namespaces there); wheels and runtime ZIPs contain
  exactly the declared packages. `tests/test_layout_and_artifacts.py` checks that no top-level
  name collides with the standard library or another installed distribution. The Glue runtime's
  own module list could not be checked offline.
* **Log logger names** changed with the module paths (`erl.ingestion.files` → `ingestion.files`);
  event names and fields are unchanged.

## Historical test cases (106) after the move

All 106 cases still run. Calls now pass the codec and batch size explicitly; four cases were
rewritten because the code they exercised moved or was replaced:

| Before | After | Why |
|---|---|---|
| `test_glue_bronze_part_commit_cannot_overwrite_an_existing_object` (AST-extracted Glue helper) | `test_bronze_part_commit_cannot_overwrite_an_existing_object` | Glue now commits with `S3Store.copy_if_absent` |
| `test_glue_dedup_compares_only_the_latest_accepted_delivery` (Glue copy of the logic) | `test_dedup_on_s3_compares_only_the_latest_accepted_delivery` | Glue uses the shared `find_accepted_duplicate` |
| `test_wrong_aws_account_is_refused`, `test_aws_session_refuses_implicit_or_default_credentials[2]` | same names | session/account guard moved to `connect.aws` + `Settings.aws_session` |
| `test_chunked_extraction_produces_the_same_content` (monkeypatched `CHUNK_ROWS`) | same name, `batch_rows=3` argument | batch size is a parameter now |

Terraform mocked tests: `rejects_untrusted_lambda_layer` was replaced (no layer is used) by
`artifacts_come_from_the_dedicated_bucket` and `rejects_short_artifact_recovery_window`.
