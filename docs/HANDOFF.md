# HANDOFF

_Last update: 2026-10-30 (America/Fortaleza) by Claude Code: offline fix of the two defects found
on AWS, refactoring to the `data-project-engineering` layout, pre-Terraform build of artifacts.
**Local commits only; nothing was pushed.** `origin/main` is still `4bc1f65` (last green CI
`41068977870` per the owner). No AWS resource exists or was created; see "Incidents" for the one
unintended AWS request of this session._

## State (2026-10-30)

| Area | State |
|---|---|
| AWS | fully decommissioned (unchanged); nothing deployed, applied, invoked or billed in this session |
| Lambda Parquet codec (AWS failure) | **fixed locally**: codec in the Bronze contract, round-trip proof before any write, Lambda ZIP bundles PyArrow/lxml pinned to `uv.lock`, no managed layer; built ZIP smoke-tested locally. **Not validated on AWS** |
| Single-date generator (D2 on D1) | **fixed**: positional roles, strictly increasing dates, prefix-stable; `erl generate --dates 2026-10-26` = clean D1 slice |
| Layout | `src/python`, `src/spark`, `src/sql`, `src/contracts`, thin `lambda/` + `glue/` entry points; `src/erl` removed ([REFACTORING.md](REFACTORING.md)) |
| Build | `scripts/build/build_artifacts.py` -> `dist/` + `manifest.json`; `smoke_artifacts.py` tests the built Lambda ZIP and Glue library; wheel mapping fixed and tested |
| Terraform | artifacts bucket (separate from state and lake), Lambda from S3 object version, Glue script + `--extra-py-files` library with SHA-256 tags; never applied |
| Local validation | see table below - all green |

Commits (on top of `4bc1f65`): `9cdb750` fix (codec + generator, with tests), `c81446a`
refactor, then the documentation/evidence commit. `dist/` was rebuilt from `c81446a`
(`dirty: false`); the SHA-256 values equal the pre-commit build (deterministic archives).

### What was run (2026-10-30, this machine, WSL2; containers were already running)

| Check | Result | Evidence |
|---|---|---|
| Baseline `4bc1f65` | ruff clean; 72 fast + 10 dbt + 24 integration = 106 passed | [baseline/tests.txt](../evidence/refactor/baseline/tests.txt) |
| Defect reproduction | single-date generation put the D2 correction, duplicate row, truncated resend, XSD-failing XML, duplicated CSV line and a D2 redemption into D1; the managed layer itself could not be fetched offline (would be an AWS call) | [generator_single_date_before.txt](../evidence/refactor/baseline/generator_single_date_before.txt) |
| New regressions fail without the fixes | 8/10 generator tests fail on `4bc1f65`; both "no orphan object" codec tests fail with the guard removed | [baseline/](../evidence/refactor/baseline/), [phase1/](../evidence/refactor/phase1/) |
| Phase 1 (`9cdb750`) | 89 fast + 11 dbt + 24 integration passed | [phase1/tests.txt](../evidence/refactor/phase1/tests.txt) |
| After refactor | ruff clean; **114 fast + 11 dbt + 24 integration = 149 passed** (106 historical + 43 new) | [after/tests.txt](../evidence/refactor/after/tests.txt) |
| Exact before/after comparison, 8 scenarios | **identical** on SQLite and on PostgreSQL + SQL Server (`4bc1f65` vs refactor): statuses, records read, ingestion statuses, quarantine reasons, gate, rejected-record counts, 24 + 24 consumer rows (248 DECIMAL values as exact text), 6 publication-log rows; SQLite vs containers also identical after the refactor. Timing on ext4 worktrees: 59.6 s baseline vs 53.8 s refactor for the 8 scenarios | [comparison/](../evidence/refactor/comparison/) |
| Built artifacts | Lambda ZIP 54.3 MiB: PyArrow 26.0.0 + lxml 6.1.3 load from the package, ZSTD round-trip of DECIMAL(24,6)/(20,2) extremes, timestamp, date, NULL; 3 handlers ACCEPTED + 1 REJECTED on moto S3. Glue library: every job import resolves from the ZIP, no PyArrow/SQLAlchemy/lxml/DuckDB in the Glue path, acceptance steps + conditional copy on moto | [smoke_report.json](../evidence/refactor/smoke_report.json), [build_manifest.json](../evidence/refactor/build_manifest.json) |
| Wheel | non-editable wheel holds exactly the 14 packages + SQL/contract resources; CLI runs from an isolated venv | [wheel_contents.txt](../evidence/refactor/wheel_contents.txt) |
| Terraform (isolated copy, credentials hidden) | fmt, validate (3 dirs), 14 mocked tests, TFLint 0, Checkov 162/0/40 | [terraform_checks.txt](../evidence/refactor/terraform_checks.txt) |
| Airflow 3.3.2 (`airflow dags test`) | DagRun **success** (62.7 s), 9 steps, ACTIVE publication 8 + 8 rows, after `erl generate --dates 2026-10-26`; SQLite backend (containers were stopped for memory, see incident 4) | [airflow_single_date.json](../evidence/refactor/airflow_single_date.json) |

### Not tested

* Spark: no JVM here, so `src/spark` and the Glue jobs only had import/graph checks with stubs;
  Glue's Python 3.11 runtime and module list were not available (syntax targets py311 via ruff).
* AWS: no Lambda, Glue, Athena/dbt-athena, Iceberg, Gold Load or hybrid publication run. S3
  `If-None-Match` on `CompleteMultipartUpload` (Glue part commits) is verified with moto only.
* The exact `AWSSDKPandas-Python312:32` layer was not reproduced (no local copy; fetching it is
  an AWS call). The fix removes the dependency on it instead.
* `dbt parse --target athena` was not used as evidence of Athena behaviour.

### Incidents in this session (reported, not hidden)

1. **One unintended AWS request.** Running `terraform init -backend=false` / `test` directly in
   `infra/environments/dev` made Terraform try to refresh the cached SSO token (the local
   `.terraform/` still holds S3 backend metadata from the destroyed lab). The SSO OIDC
   `CreateToken` call was refused (`InvalidGrantException`, HTTP 400): no session, no resource
   access, no cost. Prevention: `scripts/terraform_checks.sh` now runs on a temporary copy of the
   versioned files with AWS credentials hidden (test added); AGENTS.md warns against running
   Terraform directly in that directory.
2. `terraform fmt .` rewrote whitespace in the private, git-ignored `dev.auto.tfvars` (values not
   read or printed). It may still set the removed variables `lambda_pandas_layer_arn` /
   `lambda_package_path`; review it before any future plan.
3. While inspecting a private run-environment file under `data/aws-functional-2026-10-29/`, its
   local database passwords and the data-dev account id were shown in this session's tool output
   (not written to git or evidence). The passwords are local-only container passwords; rotating
   them (`.env` + container re-creation) is recommended. Nothing was sent elsewhere.

4. **Memory.** The first post-refactor SQLite evidence run on `/mnt/c` failed 3 dbt builds with
   `IO Error ... Cannot allocate memory` (WSL swap exhausted: 2.0 GiB used, ~5 MiB free; SQL Server
   container ~1.3-1.6 GiB; this session was running other commands in parallel). Not a code
   defect: the containers run, an isolated rerun and an ext4 worktree run are all identical to the
   baseline. Concurrent work was stopped and **this project's two containers were stopped**
   (`docker compose stop`; `erl_pgdata`/`erl_mssqldata` and other projects' volumes untouched).
   They were running when the session started; start them again with `docker compose up -d --wait`
   when needed. Evidence: [memory_incident/](../evidence/refactor/memory_incident/).

### Next step (each needs explicit authorisation)

1. Review the local commits; if accepted, authorise a push to `Anderson-adm/enterprise-risk-liquidity`
   and check GitHub Actions (the CI now builds and smoke-tests `dist/`).
2. Any AWS resumption follows [AWS_VALIDATION.md - future run proposal](AWS_VALIDATION.md#future-run-proposal-not-authorised)
   (fresh bootstrap, new plans, one date, estimated USD 1-3, not a cap). Nothing is authorised now.

## Earlier header (Codex, before this session)

_Last update: 2026-10-30 (America/Fortaleza) by Codex, documentation/evidence publication and AI handoff prompt. Review fixes are in
`5fe19db`; documentation and review evidence accompany publication to `main`, authorised by
the owner. Current remote validation: [GitHub Actions](https://github.com/Anderson-adm/enterprise-risk-liquidity/actions/workflows/ci.yml).
The review baseline was `13f9820`; `evidence/review/verification.json` preserves that tested
snapshot, before the publication-only documentation updates._

## State before the 2026-10-30 refactor (Codex)

**AWS Case 01 is fully decommissioned.** At the owner's explicit request, Terraform removed
all **69 dev + 7 bootstrap resources**, including all three S3 buckets, object versions,
CloudWatch logs/alarms and the budget. Direct reads verified all 76 resources absent, no
project-tagged resources and no extra project log groups. States, object/state history and
execution evidence are preserved privately/local; no secret/state/plan was committed.
The bootstrap `prevent_destroy` source guard was temporarily disabled for this expressly
authorised removal and restored afterwards. The owner then explicitly expanded deletion to
the account's pre-existing projects: four more S3 buckets and all 27 versions/delete markers,
one Lambda, its log group and application IAM role were removed. A 17-enabled-region/global
inventory found no further billable resources in the checked services, active Marketplace
agreements, Savings Plans or reserved-service commitments. S3 buckets, Lambda functions,
logs, alarms and tagged resources are now absent across the checked regions.
AWS-managed `alias/aws/lambda` (not customer-deletable, no key-storage charge), seven free
access/service IAM roles and free default service metadata remain. Support plan/invoice were
not independently audited; disabled regions were not enabled. No zero-invoice claim is made.
Local repositories, databases/volumes and other AWS accounts were untouched.
No further AWS processing or reprovisioning is authorised.
The 2026-10-30 repository update changes documentation/evidence only; it does not fix the
codec/generator defects or make a new AWS call. A copyable discussion prompt is in
[PROJECT_CONTEXT_PROMPT.md](PROJECT_CONTEXT_PROMPT.md).
Evidence: [full teardown verification](../evidence/aws/teardown/verification.json).
Expanded account scope: [account cleanup](../evidence/aws/account-cleanup/verification.json).

The following provisioning and failed-execution details are historical; those AWS resources
no longer exist. Local code and evidence remain available for offline work.

Milestones 1–3 are implemented and validated locally, now also on **real PostgreSQL 17 and SQL
Server 2022 containers**. Milestone 4 has completed its authorised **AWS preflight and
bootstrap apply** (7 creates) and the separately authorised **dev apply** (69 creates,
0 changes, 0 destroys). All 69 dev resources were verified through AWS reads. The first
authorised functional attempt for **2026-10-26 failed during ingestion**: one external-XML
Lambda raised `ArrowNotImplementedError: Support for codec 'zstd' not built`.
The run stopped without retry; no Glue job, Athena query, Gold or SQL Server publication ran.
Run: `20261030T000957Z-93958f` (run-id timestamp is UTC; reference date remains 2026-10-26).
Account `********8330` matched the secure expected-account configuration through SSO; region
`sa-east-1`. Bootstrap/dev states were separate and have now been archived locally after teardown.
SNS e-mail is **Confirmed**. The real Lambda error triggered its CloudWatch alarm and an SNS
action with `Succeeded` status; receipt by the human recipient was not independently verified.
Evidence: [bootstrap verification](../evidence/aws/bootstrap/summary.json),
[dev provisioning](../evidence/aws/dev-apply/verification.json),
[first functional attempt](../evidence/aws/functional/2026-10-26/summary.json),
[dev plan](../evidence/aws/dev-plan/plan_summary.json),
[preflight summary](../evidence/aws/preflight/summary.json),
[redacted plan](../evidence/aws/preflight/bootstrap_plan_redacted.txt).

Independent review conclusion: **SIM COM RESSALVAS** for starting a controlled, explicitly
authorised Milestone 4 validation. Local checks pass, but the complete AWS pipeline remains
unvalidated after the Lambda codec failure. Full findings and operational limits:
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
| Lambda handlers + package | external XML invoked once on AWS and failed on missing Zstandard codec in managed layer; other two functions not invoked; no retry |
| Glue jobs (db_ingest GB/TB, gold_load) | written; **not executed** |
| Terraform bootstrap | applied/verified, then all 7 resources destroyed under full-teardown authorisation; private state history preserved |
| Terraform dev + module | applied/verified, then all 69 resources destroyed; private state/object archives preserved |
| CI (GitHub Actions) | `f1e9fa5b20b8553372511e0847ef783e10e788e6`, run [40939982435](https://github.com/Anderson-adm/enterprise-risk-liquidity/actions/runs/40939982435): python, integration and terraform successful |

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

## Known issues / limitations (as of 4bc1f65; the codec and generator items are fixed in 9cdb750)

* **AWS ingestion blocked:** the managed `AWSSDKPandas-Python312:32` layer's PyArrow 24.0.0
  build cannot write the Zstandard codec required by `src/erl/ingestion/bronze.py:237`.
  CloudWatch request id: `7531cd78-178c-4336-890a-d8094ea13e3e`; duration 8,623.34 ms,
  billed 9,390 ms, max memory 220/512 MiB. Local PyArrow 26.0.0 supports Zstandard.
  All three functions share this writer/layer. A compatible release, new deployment plan and
  explicit authorisation are needed before another paid attempt. No code/deployment fix was made.
* `erl generate --dates <one-date>` aliases its D2 fixtures onto D1. This run instead loaded
  only the canonical D1 slice into five **new** `aws1_*` databases, excluding two deliberate
  unmapped fixture rows: four valid files, no other processing date or failure scenario.
  Existing databases were not recreated. The same sources produced a passed local reference:
  eight rows per Gold product, a passed gate and zero rejects.
* Athena/Iceberg execution is unverified; offline Athena parse passes, but Glue Spark execution,
  Ion manifest reads, dependency installation, IAM and actual alert delivery still need AWS evidence.
  Athena decimal division operands now receive sufficient scale before division; this change is
  based on Trino's documented typing rules and is not an AWS execution claim.
  DuckDB ratios now use exact integer/decimal arithmetic instead of DOUBLE; the large-value
  rounding regression and the end-to-end scenarios cover the local change.
* Readers of `rpt.*` on SQL Server use READ COMMITTED without RCSI, so a report query may wait for
  a running publication (consistent, but blocking). Enabling `READ_COMMITTED_SNAPSHOT` on the
  consumer database would let readers see the previous version without waiting — not done.
* SSO login and expected-account identity were confirmed during the authorised AWS preflight.
  Sessions expire; recheck before apply. The default profile is refused by runtime/provider settings.
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

## AWS resource history (all Case 01 resources now removed)

The authorised preflight found no Case 01 resources by tag or expected names. The subsequent
authorised bootstrap created `erl-tfstate-********8330` and its six configurations;
unrelated account resources were untouched. Public-access block, ownership, encryption,
versioning, TLS policy and lifecycle were read back successfully. Current objects/active locks
do not expire; noncurrent versions are retained for 90 days. Bootstrap state and a matching
private backup are local and git-ignored, with verified restricted Windows file ACLs; both
copies share one physical disk. The bucket held the separate dev state/lock history until the
owner explicitly requested full teardown, including bootstrap; all versions were archived
locally before the bucket was deleted.
All 85 sampled IAM action simulations were allowed; dev creation succeeded, while runtime
permissions remain unproven. Quotas fit the small serial run, but do not reserve capacity.
Regional Pricing metadata was read; no billing/Cost Explorer query or measured-cost claim was made.
The authorised dev apply then created exactly 69 resources, including the budget and e-mail
subscription, with zero alterations/deletions. Direct reads verified every planned resource;
apply emitted no errors or warnings. The subsequent authorised functional attempt invoked one
Lambda and stopped on the codec failure; no Glue jobs or Athena processing queries ran.
The dev backend is encrypted and uses native lock files under `case01/dev/terraform.tfstate`;
no state migration or overwrite occurred. Historical lock versions remain after normal unlock;
the active lock is absent. The private final plan configures the supplied alert recipient and
narrows runner trust to the current SSO role. The applied budget is USD 5/month for the whole account,
with 80% actual/100% forecast notifications; it is not an enforced spending limit.
The failed run left 24 Landing objects and one raw XML Bronze object, with no accepted ingestion
manifest. They are preserved, uncommitted; no Silver/Gold tables, snapshots or publication objects
were created. The isolated SQL Server consumer has neither product nor publication log.
Observed inputs imply USD 0.00007845 for Lambda and USD 0.000175 for at least 25 PUTs: a partial
estimate excluding unmetered reads/logs/SNS/storage and existing alarms, not an invoice.
Pipeline wall time to stop: 89.845 s. No billing API was queried.

## Next step as of 4bc1f65 (superseded by the section at the top)

1. Keep the private archives and sanitised evidence. Do not reuse consumed plans or attempt
   to access the removed S3 backend. No further AWS action is authorised by the teardown.
2. If the owner later resumes the case, fix the documented codec/generator issues locally first;
   a fresh bootstrap/dev deployment and functional run need separate authorisation.
3. The later expanded account cleanup also removed the pre-existing application/storage.
   Retain the private archives. Historical usage can still be billed; check the billing/support
   plan separately if an invoice-level assurance is needed. No literal empty-account claim is made.
