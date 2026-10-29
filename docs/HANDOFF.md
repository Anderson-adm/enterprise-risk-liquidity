# HANDOFF

_Last update: 2026-10-29 by Codex, review closure and AWS preparation. Review fixes are in
`5fe19db`; documentation and review evidence accompany publication to `main`, authorised by
the owner. Current remote validation: [GitHub Actions](https://github.com/Anderson-adm/enterprise-risk-liquidity/actions/workflows/ci.yml).
The review baseline was `13f9820`; `evidence/review/verification.json` preserves that tested
snapshot, before the publication-only documentation updates._

## State

Milestones 1–3 are implemented and validated locally, now also on **real PostgreSQL 17 and SQL
Server 2022 containers**. Milestone 4 (AWS) has **not** started: no Terraform apply/destroy, no AWS
resource, no AWS call was made or is authorised.

Independent review conclusion: **SIM COM RESSALVAS** for starting a controlled, explicitly
authorised Milestone 4 validation. The repaired implementation passes local checks; AWS
execution remains unverified. Full findings, before/after regressions and operational limits:
[INDEPENDENT_REVIEW.md](INDEPENDENT_REVIEW.md), [../evidence/review/](../evidence/review/).
Final Python results: 72 fast tests + 10 dbt end-to-end tests (82 local), and 24 real-database
integration tests. The suite now collects 106 cases; 27 cases were added during the review.

| Area | State |
|---|---|
| 7 sources, generator, 8 scenarios | validated locally on PostgreSQL + SQL Server containers and on SQLite stand-ins |
| Extraction → landing → Bronze (manifests, checksums, duplicates, quarantine) | validated locally on both backends; identical Bronze content hashes across engines; S3 path with moto |
| dbt Silver/Gold (DuckDB), quality gate, rejected records | validated locally |
| Publication package + transactional/idempotent publisher | validated on a SQL Server 2022 container (incl. 4 concurrent publishers) and on SQLite |
| Airflow DAG (local 3.3.2) | independently validated on real PostgreSQL + SQL Server: success, quality failure, blocked load/publish, failure alert and preserved consumer; isolated `review_*` databases and SQLite metadata under `/tmp` |
| Lambda handlers + package | tested with moto; not run on AWS |
| Glue jobs (db_ingest GB/TB, gold_load) | written; **not executed** |
| Terraform bootstrap + dev + module | fmt/validate/TFLint pass, 13 mocked tests pass, Checkov 148 passed / 0 failed / 36 skipped; **never applied** |
| CI (GitHub Actions) | fixes and evidence published to main; closing response records the confirmed commit/run and the three job results |

## Independent review corrections (Codex, 2026-10-29)

* Snapshot reversions A → B → A compare against the latest accepted delivery, scoped by XML
  portfolio or by the current reference register across dates.
* Bronze contract v2 includes an empty-snapshot marker. Silver excludes uncommitted Bronze
  and filters markers only after choosing the latest delivery. Existing v1 nonempty files remain readable.
* Gold removes disappeared keys in the same MERGE that upserts the date; both products and
  preserved dates are covered by the local end-to-end scenario.
* Gate rejects nonzero dbt exits and cannot reuse stale `run_results.json`. An entirely rejected
  required market delivery blocks ingestion instead of silently reusing previous prices.
  Empty publication products are refused. Reporting survives an incomplete warehouse.
* Concurrent retries of one package use unique publication attempt ids; SQL Server regression
  reproduces the old primary-key collision and passes after the fix.
* Successful steps, completed extraction snapshots and completed ingestion deliveries are
  reusable checkpoints. Collect/load also recognise completed immutable outputs.
* Glue SDK dependencies are pinned to versions supporting conditional S3 writes. Bronze part
  commits use conditional PUT rather than a racy HEAD/COPY; part listings are paginated.
* Terraform declares the manifest control table, correct Glue 5.0 log groups and namespace-scoped
  metric permissions. SNS's incompatible managed-key setting is replaced by a documented
  synthetic-lab encryption exception. The original 33 Checkov skips were reassessed; the count
  is now 36 (one new SNS skip plus two instances for the extra log group).
* Explicit non-default SSO/runner profile, account checks on individual AWS steps, and region
  restrictions prevent implicit use of default static credentials. Terraform providers/backend
  examples are explicit too. No real AWS session was opened during the review.
* Teardown excludes state buckets through lab-specific tags, checks exact database names and
  propagates partial deletion errors. The local Terraform check runner no longer masks failures.

Review evidence records the original failures as well as passing checks. The two local database
containers were started under the existing authorisation, with other projects untouched; volumes
are preserved. See `evidence/review/container_shutdown.txt` for their final state.

## Earlier implementation session (Claude Code, before independent review)

The following command counts, memory measurements and security review describe the earlier
implementation session. Current review results are listed above and in `evidence/review/`.

Containers (Docker Desktop 29.8.0 already running with WSL integration for Ubuntu; nothing
installed or reconfigured; `.wslconfig` untouched; pre-existing volumes of other projects untouched):

```bash
# .env (git-ignored) created with random local passwords; values never printed
docker compose -f docker/compose.yaml --env-file .env up -d --wait postgres    # healthy
docker compose -f docker/compose.yaml --env-file .env up -d --wait sqlserver   # healthy (~18 s)
set -a; . ./.env; set +a
uv run erl generate                                   # 4 source DBs on the real engines
ERL_IT_CONTAINERS=1 uv run pytest -m integration      # 23 passed (it_* databases)
uv run python scripts/collect_evidence.py             # 8/8 scenarios as expected (ev_* databases)
ERL_SOURCE_BACKEND=sqlite uv run python scripts/collect_evidence.py   # 8/8 as expected
uv run python scripts/collect_db_evidence.py          # evidence/databases/*.json
uv run pytest -v                                      # default suite, see evidence/tests/
scripts/terraform_checks.sh                           # fmt, validate, 11 tests, TFLint 0, Checkov 148/0/33
```

Memory (`docker stats`): PostgreSQL idle ~30 MiB / peak ~97 MiB (limit 384 MiB); SQL Server idle
~625 MiB / peak ~1.14 GiB (limit 2 GiB). VM kept ≥ 4.5 GiB available. No memory problem.
Airflow was not run together with the databases.

Evidence (separate per engine): `evidence/local/{sqlite,containers}/`,
`evidence/databases/{sqlite,postgresql,sqlserver}.json`, `evidence/tests/`,
`evidence/terraform/checks.txt`.

Cross-engine comparison: same scenario outcomes; identical published business values. The
publication content hashes differ between backends only through `lineage_bronze_runs` (the run id
that wrote each Bronze snapshot), which is expected.

## Defects found and fixed

With the real databases (2026-10-29) — each has a test that fails without the fix:

* **Concurrent publishers deadlocked on SQL Server** (error 1205); on PostgreSQL the first
  publication race hit the unique index. Cause: SQLAlchemy renders no `FOR UPDATE` on SQL Server
  and the first publication of a date has no row to lock. Fix: transaction-scoped publication lock
  (`sp_getapplock` / `pg_advisory_xact_lock`). Test:
  `test_concurrent_publications_are_serialised_and_leave_one_consistent_version`,
  `test_publication_lock_also_serialises_a_postgresql_consumer` (verified to fail with the lock disabled).
* **SQL Server `DATETIME` rounding** (1/300 s) for `published_at`/`superseded_at` and source
  timestamps → `erl.sqltypes.Timestamp` (DATETIME2). Test: `test_*_column_types_*`.
* **Bronze text differed by engine** for timestamps (`2026-10-27 19:00:00.000000` on SQLite vs
  `2026-10-27T19:00:00` on PostgreSQL) → extraction types result columns from source metadata.
  Test: `test_extraction_is_identical_to_the_sqlite_stand_in`.
* **PostgreSQL consumer** lacked `rpt`/`stg` schemas → created in `ensure_schema`.
* **`ensure_databases`** ignored database names from URL overrides → names come from the URL and
  are validated before DDL. Test: `test_database_names_taken_from_urls_are_validated_before_ddl`.
* **Missing evidence file**: docs cited `evidence/local/pytest.txt`, which `collect_evidence.py`
  deleted (it wiped `evidence/local/`). Evidence is now split by backend; each script only
  rewrites its own directory.
* **Wrong Checkov justification** (CKV_AWS_18 claimed CloudTrail management events cover S3
  object access — they do not). Reworded; docs/TERRAFORM.md lists residual risk per exception.

Earlier (SQLite phase): DOCTYPE/XXE crash in XML validation; gate coupling between the two Gold
tables; Airflow DagRun `success` after a failed step; Glue gold-load IAM temp prefix; two ACTIVE
publications for one date.

## Repository security review before the first push (2026-10-29)

* All commits inspected (`git log --all --name-only`, full patch scan with a local pattern scanner
  for AWS keys, private keys, tokens, URLs with credentials, password literals, account ids, e-mails).
  gitleaks could not be used (download denied by the session's permission policy).
* Not in history or tree: the local AWS key pair of the `default` profile, the data-dev/data-prod
  account ids, the SSO start URL, the local database passwords (checked by value, never printed).
* The only 12-digit number is AWS's public account that owns the managed `AWSSDKPandas` layer.
  E-mails found: `@pytest...` decorators, `you@example.com`, `alerts@example.com`.
* No `.env`, tfstate, plans, `.terraform/`, keys or data files ever committed. `.gitignore`
  hardened (`*.tfvars`, plan files, `.env.*`, `*.key`, `*.p12`, `*.pfx`).
* Commits carry the owner's git identity (`user.email` from the global config), which becomes public.
* Checkov: 33 skips = 17 inline annotations × instances; each reviewed (docs/TERRAFORM.md).

## CI on GitHub

* Run 40925068498 (commit `bee415d`, first push): `integration` and `terraform` passed; `python`
  **failed** at "Lambda package builds" with exit code 126 — the scripts had no executable bit in
  git (repo on `/mnt/c`, `core.filemode=false`). Fixed in `22c6c5f` (`git update-index --chmod=+x`).
* Run 40925344256 (commit `22c6c5f`): **all green** — `python` 72 s, `integration` (real
  PostgreSQL + SQL Server containers on the runner) 75 s, `terraform` 61 s.
  https://github.com/Anderson-adm/enterprise-risk-liquidity/actions/runs/40925344256
* Note for agents on this machine: the repo is on a Windows drive; new scripts need
  `git update-index --chmod=+x <file>`. Pushes run through Windows `git.exe` (Git Credential
  Manager); WSL git has no GitHub credentials.

## Known issues / limitations

* Athena/Iceberg execution is unverified; offline Athena parse passes, but Glue Spark execution,
  Ion manifest reads, dependency installation, IAM and actual alert delivery still need AWS evidence.
  Athena decimal division operands now receive sufficient scale before division; this change is
  based on Trino's documented typing rules and is not an AWS execution claim.
  DuckDB ratios now use exact integer/decimal arithmetic instead of DOUBLE; the large-value
  rounding regression and the end-to-end scenarios cover the local change.
* Readers of `rpt.*` on SQL Server use READ COMMITTED without RCSI, so a report query may wait for
  a running publication (consistent, but blocking). Enabling `READ_COMMITTED_SNAPSHOT` on the
  consumer database would let readers see the previous version without waiting — not done.
* AWS SSO tokens were expired at the earlier preflight; their status was not rechecked during
  the independent review. The default profile is now refused by project runtime/provider settings.
* The earlier Windows GitHub CLI token was invalid; publication uses the existing Git Credential
  Manager through Windows Git, without changing credentials.
* One pipeline at a time, including manual CLI invocations; sources must remain stable during
  extraction. The CLI does not enforce a global lock, and source entities use separate SELECTs.
* Failures before a manifest/state checkpoint may leave immutable uncommitted objects. Use a
  new run id rather than overwriting them. Silver ignores uncommitted Bronze; Load requires a
  committed publication manifest. Large-volume recovery/performance is not validated.
* The local Airflow uses SQLite metadata (dev only).
* Docker stopped SQL Server with exit 137 even with a 60-second timeout; `OOMKilled=false`.
  Integration tests passed before shutdown; volumes were preserved. No Docker/WSL configuration
  was changed; see `evidence/review/container_shutdown_diagnostics.txt`.

## AWS resources

No resources were created by this review. The earlier implementation reports no AWS resources
created; AWS inventory and account costs were not independently queried because AWS APIs were
not authorised. No AWS cost measurement is claimed.

## Next concrete step

1. Obtain approval for `aws sso login --profile data-dev` and read-only account/region,
   permission/quota and inventory checks. Local configuration passed; no SSO login or AWS API
   was executed during preparation.
2. Follow docs/AWS_VALIDATION.md: bootstrap saved plan → approval → bootstrap apply → lab
   backend/plan → approval → lab apply → separately approved single-date paid validation.
3. Preserve sanitised evidence locally before authorised teardown; retain the state bucket.
   Preliminary first-date allowance: USD 1–3, subject to sa-east-1 pricing and actual job duration.
