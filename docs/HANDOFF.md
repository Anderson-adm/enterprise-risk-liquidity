# HANDOFF

_Last update: 2026-10-29 by Claude Code (Opus 5.5). Branch `main`, pushed to
`Anderson-adm/enterprise-risk-liquidity` (public). CI: see "CI on GitHub" below._

## State

Milestones 1–3 are implemented and validated locally, now also on **real PostgreSQL 17 and SQL
Server 2022 containers**. Milestone 4 (AWS) has **not** started: no Terraform apply/destroy, no AWS
resource, no AWS call was made or is authorised.

| Area | State |
|---|---|
| 7 sources, generator, 8 scenarios | validated locally on PostgreSQL + SQL Server containers and on SQLite stand-ins |
| Extraction → landing → Bronze (manifests, checksums, duplicates, quarantine) | validated locally on both backends; identical Bronze content hashes across engines; S3 path with moto |
| dbt Silver/Gold (DuckDB), quality gate, rejected records | validated locally |
| Publication package + transactional/idempotent publisher | validated on a SQL Server 2022 container (incl. 4 concurrent publishers) and on SQLite |
| Airflow DAG (local 3.3.2) | success + failure runs validated earlier with `airflow dags test` on the SQLite backend; **not re-run on the containers backend** |
| Lambda handlers + package | tested with moto; not run on AWS |
| Glue jobs (db_ingest GB/TB, gold_load) | written; **not executed** |
| Terraform bootstrap + dev + module | fmt/validate/TFLint/Checkov/`terraform test` pass; **never applied** |
| CI (GitHub Actions) | python, integration (containers) and terraform jobs; see below |

## Commands executed on 2026-10-29 (this session) and results

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

_To be filled with the real result of the first run._

## Known issues / limitations

* Athena/Iceberg compatibility of the dbt SQL is unverified; Glue scripts never ran; no AWS evidence.
* Airflow DAG not re-tested against the containers backend (it only calls the same CLI steps).
* Readers of `rpt.*` on SQL Server use READ COMMITTED without RCSI, so a report query may wait for
  a running publication (consistent, but blocking). Enabling `READ_COMMITTED_SNAPSHOT` on the
  consumer database would let readers see the previous version without waiting — not done.
* AWS SSO tokens for `data-dev` were expired at preflight; the `default` profile holds static
  access keys (recommend removing/rotating them; the project never uses `default`).
* The Windows GitHub CLI token is invalid (`gh auth status`); pushes used Git Credential Manager.
* Athena `merge` does not delete vanished rows (mitigated: Load exports only current-run rows).
* The local Airflow uses SQLite metadata (dev only).

## AWS resources

None created. Observed AWS cost: none.

## Next concrete step

1. Independent review by Codex following docs/CODEX_REVIEW.md.
2. Milestone 4 following docs/AWS_VALIDATION.md — only with the owner's explicit authorisation
   for each apply/destroy (account and region checked, saved plan reviewed first).
3. Optional: `docker compose -f docker/compose.yaml down` when the databases are not needed
   (volumes are kept).
