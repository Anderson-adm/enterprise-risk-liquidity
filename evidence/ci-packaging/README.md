# CI packaging cache fix — 2026-11-01

Baseline: published b01a512. Failed CI: actions/runs/41307647915. Integration/Terraform passed;
Python failed at the offline packaging installer because lxml==6.1.3 was unavailable in its
cache. uv sync alone did not provide the required packaging-cache contract. Prior local builds
used an already warmed developer cache; this regression explicitly starts with an empty cache.

The new preparation command derives dependency names from unit.json and versions from uv.lock,
then uses the same target installer as build with downloads explicitly enabled. Normal build
keeps --offline; the CI build/smoke step also sets UV_OFFLINE=true. Only the preparation stage
used package-download access. No global installation or dependency update.

Re-executed: 45 affected tests, Ruff, two complete offline builds and nine actual built-unit
smokes (moto/native codec tests, Spark/Glue stubs). Both lxml and pyarrow failed offline before
preparation and succeeded afterwards using only the new isolated cache. All 12 artifact hashes
match between builds AND the published baseline. Integration/Terraform workflow definitions
are unchanged; no containers, Terraform or AWS were executed and no push was made.

Evidence: cold_before.txt, prepare.txt, cold_cache_verification.json, tests.txt, lint.txt,
build-first.txt, build-second.txt, smoke_report.json, artifact_hashes.json, verification.json.
The commit containing this evidence is the local fix; corrected GitHub CI still requires an
authorised push. Pre-commit smoke provenance is retained honestly, with exact source hashes.
