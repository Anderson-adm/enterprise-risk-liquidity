# Autonomous-unit migration evidence (offline)

Baseline: approved `3e11db9`. Source was tested before the local commit; source_inputs.json records
owned file hashes and build_manifest.json records exact artifact bytes/dependencies/provenance.
Ignored dist/ is rebuilt after committing to verify the same bytes and current Git provenance.

* fast.txt: 145 fast cases; dbt.txt: 11 DuckDB/dbt cases (telemetry disabled).
* bronze_flow_conformance.txt: 8 additional GB/TB flow cases, only Spark processing stubbed.
* layout_and_build.txt: 28 rechecks, including independent change/add/remove builds; native
  dependencies replaced only in the cheap non-propagation fixtures, real packages smoke-tested.
* terraform_runner.txt: 4 failure/privacy/backend-isolation checks; terraform.txt: fmt/validate,
  15 mocked Terraform cases, TFLint and Checkov. No remote backend/provider/API used.
* build.txt/build_manifest.json: three local ZIPs, three Lambda ZIPs, three Glue scripts/libs.
* smoke_report.json: actual built artifacts, isolated interpreters, native codecs and moto.
* wheel_build.txt/wheel_smoke.txt: offline non-editable wheel, imports/resources/CLI from archive.
* comparison/: eight scenarios on baseline and migration, separate fixtures; diff.json is exact
  after excluding run ids/time/lineage-only hashes as documented by the established normalizer.

No container start, installation from network, AWS, SSO, paid service, deploy, push, apply or destroy.
No real Spark/Glue/Athena/Iceberg validation is claimed. Historical evidence is unchanged.
