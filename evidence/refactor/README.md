# Evidence: defect fixes and refactoring (2026-10-30, offline)

Produced on the owner's machine (WSL2, PostgreSQL 17 + SQL Server 2022 containers already
running). No AWS call was intended or needed; see the incident note in
[docs/HANDOFF.md](../../docs/HANDOFF.md). Earlier evidence (`evidence/local/`, `evidence/tests/`,
`evidence/terraform/`, `evidence/review/`, `evidence/aws/`) is unchanged.

| Path | What |
|---|---|
| `baseline/tests.txt` | ruff + 72 fast + 10 dbt + 24 integration on `4bc1f65` (clean tree) |
| `baseline/generator_single_date_before.txt` | reproduction of the single-date defect on `4bc1f65` |
| `baseline/generator_tests_fail_on_baseline.txt` | the new generator tests run against `4bc1f65` (8 of 10 fail) |
| `phase1/tests.txt`, `phase1/codec_guard_mutation.txt` | `9cdb750`: suites; the codec regressions fail when the guard is removed |
| `after/tests.txt` | suites on the refactored tree (fast suite rerun on `c81446a`: 114 passed) |
| `airflow_single_date.json`, `memory_incident/` | Airflow DAG test; the ENOMEM evidence run that was repeated |
| `comparison/timing_ext4.json` | baseline vs refactor wall time on ext4 worktrees |
| `comparison/` | normalised outcomes of the 8 scenarios before (`4bc1f65`) and after, per backend, and the exact diff report |
| `local/{sqlite,containers}/` | `collect_evidence.py` output after the refactor (same format as `evidence/local/`) |
| `build_manifest.json`, `smoke_report.json` | `dist/manifest.json` and the smoke report of the built Lambda ZIP and Glue library |
| `wheel_contents.txt` | non-editable wheel contents and an isolated-venv CLI check |
| `terraform_checks.txt` | `scripts/terraform_checks.sh` on the refactored tree |

The comparison excludes values that change on every run by design: run ids, timestamps,
elapsed times, dbt invocation ids, publication ids/UUIDs, `content_sha256` (it covers
`lineage_bronze_runs`, i.e. run ids) and the `lineage_bronze_runs` column. Everything else in the
run reports (records read, ingestion statuses, quarantine reasons, late arrivals, gate status,
warnings and failures, rejected-record counts, publication status and row counts) and every
column of the published consumer rows, DECIMAL values compared as exact text, is compared.
