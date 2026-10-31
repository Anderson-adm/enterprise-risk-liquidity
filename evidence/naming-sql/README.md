# Owner-selected Glue naming and SQL ownership — 2026-10-31

Baseline `fec9280`; local/offline only. The local commit containing this evidence completes the
change. Historical AWS logs/evidence are unchanged. No AWS, SSO, remote backend, real provider
plan, apply/destroy/deploy, push, paid service or container startup.

| Old unit | New unit | Actual scope, unchanged |
|---|---|---|
| db-gb | extract-liquidez | GB-profile landing snapshots: fund_ops, portfolio_risk, compliance registers/limits |
| db-tb | extract-enquadramento | TB-profile landing snapshots: legacy_finance views, compliance stock_position |
| gold-load | export-risk-analytics | Both Gold products -> committed publication package, not SQL Server publication |

Names do not imply exclusive business domains. Runtime contract labels/profiles and entity
routing remain unchanged. Each job keeps independent source, Spark, SDK requirements, artifacts
and deploy references. All nine unit.json interfaces/resources/effects/lifecycle were checked.

| Verification re-executed | Result | Evidence |
|---|---|---|
| Fast Python tests | 159 passed | fast.txt |
| dbt/DuckDB end-to-end tests; telemetry disabled | 11 passed | dbt.txt |
| Ruff | check passed, 87 files formatted | lint.txt |
| Full offline build and real built-unit smoke | 9 units / 12 artifacts, 9 smokes passed | build_manifest.json, smoke_report.json |
| Three selected CLI builds | 6 Glue artifacts reproduced; unrelated files untouched | selected_builds.json |
| Terraform fmt/validate/test, temporary copy, local provider mirror, no backend | 3 roots valid; 15 mocked tests passed | terraform.txt |

All 54 versioned SQL files retain their bytes and locations: 12 runner, six publisher, 36 dbt.
The smoke reads every packaged SQL through its owner's importlib.resources loader from actual
ZIPs: runner 12 + bundled publisher six, standalone publisher six. dbt SQL belongs to the separate
versioned project. Glue uses native DataFrame API, so no empty SQL directories were created.
Individual ownership/executor/artifact paths and SHA-256: sql_inventory.json. Job executable AST
and other touched business programs are unchanged after normalizing approved names; see
behavior_preservation.json. Published contracts, dbt, Airflow and dependency pins are unchanged.

Current tree/interfaces and future rollout caveats: docs/REFACTORING.md. Changed Terraform job
keys/names, artifact prefixes, worker-map keys and exporter IAM identifiers require review if an
old state is reused; no automatic state migration was made. Real managed Spark/AWS execution is
unvalidated; future deployment needs idle jobs and explicit authorization. Code rollback does not
revert data/schema/state. Exact artifact hashes: artifact_hashes.json. Provenance/limits:
verification.json and source_inputs.json; build metadata honestly records the pre-commit snapshot.
