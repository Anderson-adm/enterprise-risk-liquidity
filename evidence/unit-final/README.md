# Final scoped autonomy adjustments — 2026-10-31

Baseline: `d75ebec`. Local/offline only; no AWS, remote Terraform, deploy, push or containers.
The local commit containing this evidence completes the change.

| Check re-executed | Result | Evidence |
|---|---|---|
| Focused build/layout/service doubles | 58 passed | focused.txt |
| Remaining fast suite (no repeated focused files) | 99 passed, 35 deselected | remaining-fast.txt |
| Ruff | passed; 87 files formatted | lint.txt |
| Full offline build | 9 units, 12 artifacts; 11 unchanged vs baseline | build.txt, artifact_hashes.json, artifact_comparison.json |
| Actual built-unit smoke | 9 passed; moto/native codecs, Spark/Glue stubs | smoke_report.json |

Tests change each Glue script/Spark/requirements, add a job and remove jobs one by one in an
isolated copy. Unrelated artifacts remain identical. Public parser/publisher/generator changes
propagate only to their owner and the declared runner consumer. Deploy references remain per
unit; no real rollout or rollback was attempted.

F1: all eight packages affected by objects.py actually consume it. Import-closure tests find no
unused protocol modules. A compatible internal change keeps contract version 2.0.0 while hashes
identify new bytes. F2: GB/TB stay independent, with deliberate local implementation repetition.
Names remain accurate volume profiles; runner routing now uses an explicit map.

Modified: nine unit.json files; scripts/build/build_artifacts.py;
apps/hybrid-runner/python/pipeline/aws_services.py; tests/test_autonomous_units.py,
tests/test_aws_components.py, tests/test_layout_and_artifacts.py; one comment in
infra/environments/dev/glue.tf; AGENTS.md, docs/REFACTORING.md, docs/HANDOFF.md; this evidence.
No published contract, dependency pin, Glue/Lambda implementation, business SQL, dbt or Airflow change.

Previous dbt (11), eight-scenario equivalence (248 exact decimals), and Terraform mocks (15)
were inspected and reused, **not rerun**: their implementations/configuration expressions did
not change. Details, provenance and limits: verification.json. source_inputs.json records exact
build inputs; build_manifest.json records the honest pre-commit snapshot. dist/ is private to
local builds and ignored. Real Spark/Glue/Athena/Iceberg remain unvalidated; deploy only with
jobs idle, and code rollback does not roll back data/schema/state.
