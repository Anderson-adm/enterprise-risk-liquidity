# AWS validation (Marco 4) — procedure

**Current status: full teardown completed on 2026-10-29 (America/Fortaleza).** The owner
requested removal of all Case 01 AWS resources, including bootstrap. All 69 dev and 7 bootstrap
resources were removed and verified absent; the three buckets and all their versions are gone.
Private state/object archives and sanitised evidence remain local. The initial teardown excluded
other projects; the owner subsequently authorised account-wide cleanup, which also removed
four pre-existing buckets, a Lambda, its log group and application role. The expanded inventory
covered 17 enabled regions and global services; remaining AWS-managed/access defaults have no
standing resource charge. Billing/support tier was not audited and disabled regions were not enabled.
Expanded evidence: [account cleanup](../evidence/aws/account-cleanup/verification.json).
The procedures/status below describe the earlier deployment/failed run, not live resources.
Do not reuse the consumed plans or removed backend; resuming needs a fresh authorised bootstrap.
Evidence: [teardown verification](../evidence/aws/teardown/verification.json).

**Status, 2026-10-29: bootstrap and authorised dev apply verified on AWS.**
Bootstrap created 7 Terraform resources; the separately authorised dev saved plan created
**69 resources, 0 changes, 0 destroys**, including the USD 5 budget and SNS subscription.
All 69 were verified by direct reads. SNS e-mail is now **Confirmed**. The first separately
authorised functional attempt for **2026-10-26 failed on the first Lambda**: its managed
PyArrow layer lacks the Zstandard codec required by the Bronze writer. No Glue/Athena run,
Gold or SQL Server publication followed. The genuine error triggered a successful CloudWatch
action to SNS; human receipt remains unverified. No intentional failure scenario was run.
Further paid processing, deployment changes and destroy require new authorisation.
Evidence: [failed functional attempt](../evidence/aws/functional/2026-10-26/summary.json),
[Lambda logs](../evidence/aws/functional/2026-10-26/lambda_cloudwatch.log),
[dev provisioning verification](../evidence/aws/dev-apply/verification.json),
[dev plan](../evidence/aws/dev-plan/plan_summary.json),
[bootstrap verification](../evidence/aws/bootstrap/summary.json),
[preflight summary](../evidence/aws/preflight/summary.json),
[redacted bootstrap plan](../evidence/aws/preflight/bootstrap_plan_redacted.txt).

Hybrid design: sources, Airflow and the consumer SQL Server stay local. The local pipeline
uploads landing objects to S3 with temporary SSO credentials; Lambdas and Glue ingest them;
dbt runs on Athena producing Iceberg Silver/Gold; the Glue `gold-load` job writes the
publication package to S3; the local publisher loads SQL Server. AWS never connects to
localhost and no local database is exposed.

## 0. Preconditions (read-only)

Run commands from the repository root. SSO login succeeded using only `data-dev`. STS matched
the explicit SSO account ID pinned from secure local configuration into ignored project variables
before login (public identifier: `********8330`); the session credential method was `sso` and
the deployment region was `sa-east-1`. No global AWS configuration was modified.
All 85 sampled IAM action simulations were allowed; this does not prove resource creation or
the future runner's effective permissions. Glue quota is 500 DPUs (2 needed per serial job),
Lambda concurrency is 10 and Athena active DML/DDL quotas are 20 each. Before bootstrap,
S3 had 4/10,000 buckets; bootstrap added one.
Athena engine 3 and the Python 3.12/x86_64 layer below were confirmed available. No Case 01
resources were found before bootstrap; dev-plan inventory found only the state bucket.
The dev resources have now been applied and verified. Service availability does not reserve capacity.

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_DEFAULT_PROFILE
export AWS_PROFILE=data-dev AWS_REGION=sa-east-1 AWS_DEFAULT_REGION=sa-east-1
aws sso login --profile data-dev
aws sts get-caller-identity --profile data-dev        # must be the data-dev account
aws configure get region --profile data-dev           # sa-east-1
```

Compare the returned account with the locally configured `data-dev` SSO account before
continuing. Never use `default`. Confirm deployment-role IAM/PassRole permissions, Glue G.1X
capacity in sa-east-1 (two workers per job), and Lambda concurrency availability before apply.
Do not request quota changes automatically. For traceability, retain sanitised command outputs
and record the confirmed account privately; keep account/principal identifiers out of public evidence.

Inventory before (read-only, for the evidence file):

```bash
aws resourcegroupstaggingapi get-resources --profile data-dev \
  --tag-filters Key=Project,Values=enterprise-risk-liquidity
```

## 1. Bootstrap the state bucket (once) — completed under explicit authorisation

The saved plan is `data/aws-preflight-2026-10-29/bootstrap.tfplan` (git-ignored).
`init` and `validate` succeeded; `plan -detailed-exitcode` returned 2, meaning changes exist.
It contains **7 creates, 0 changes, 0 destroys**: one S3 state bucket and six configurations
(ownership, public-access block, versioning, AES256 encryption, lifecycle and TLS-only policy).
SHA-256: `abae7580f58536ca813c5575298655bf39a374ef2ec3c89c3520853a888162c9`.
The owner authorised only this saved plan, with no lab services or budget. Before applying,
SSO identity, expected account, region, full checksum, all seven addresses, lifecycle and local
directory access controls were checked again. The executed command was:

```bash
terraform -chdir=infra/bootstrap apply "$PWD/data/aws-preflight-2026-10-29/bootstrap.tfplan"
```

Apply exited 0. Direct S3 reads confirmed public-access block 4/4, nonpublic policy,
versioning, AES256/SSE-S3, BucketOwnerEnforced, TLS-only access, tags and `sa-east-1` ownership.
Lifecycle has **no current-object expiration**: current state and active lock files are not
expired. Noncurrent versions remain for 90 days; incomplete multipart uploads are aborted
after one day. Immediately after bootstrap, the bucket was empty, including historical versions
and delete markers; it now holds the separate dev state and lock history.
The local state at `infra/bootstrap/terraform.tfstate` and private backup at
`data/aws-preflight-2026-10-29/bootstrap.initial.tfstate` have matching SHA-256 digests,
are git-ignored/untracked, and have Windows ACLs allowing only the owner, SYSTEM and
Administrators. Both copies are on the same disk; no independent off-device backup was made.
Do not rerun this consumed saved plan. Bootstrap state remains local; the future dev root uses S3.

For a replacement plan (requires renewed review), the general sequence is:

```bash
terraform -chdir=infra/bootstrap init -input=false -lockfile=readonly
terraform -chdir=infra/bootstrap plan -var='allowed_account_id=<id>' -out=bootstrap.tfplan
terraform -chdir=infra/bootstrap show -no-color bootstrap.tfplan
```

Stop and review the replacement saved plan before seeking approval.
Keep the existing bootstrap state and private backup; this root uses local state. The state
bucket remains private, versioned, encrypted and protected by `prevent_destroy`.

## 2. Plan the lab — completed under explicit authorisation

The backend uses the bootstrap bucket, key `case01/dev/terraform.tfstate`, SSO profile
`data-dev`, `sa-east-1`, encryption and `use_lockfile = true`. Init used `-reconfigure`, never
`-migrate-state`: no existing dev state was present, and the bootstrap local state's checksum
is unchanged. Terraform released both planning locks; at that point there was no remote dev
state object. After apply the dev state is present and its active lock is absent. S3 versioning
retains historical lock objects/delete markers until lifecycle or separately authorised cleanup;
no manual object deletion was performed.

The applied plan is `data/aws-dev-plan-2026-10-29/dev-final.tfplan`, git-ignored and private.
SHA-256: `910dcc94954f57d2edbfe8c2a38b3790dc8668b8c0766b3a4f7a046a0067c64f`.
It supersedes the preliminary plan without an alert recipient. The supplied recipient is now
configured privately: one SNS e-mail subscription plus budget ACTUAL >80% and FORECASTED >100%
notifications. The USD 5/month budget covers the **whole account**, not just this project's tags;
it does not cap spending. The owner explicitly authorised all 69 creates, including this budget.
The runner trust is narrowed to the current data-dev SSO role; the Lambda ZIP was frozen privately.
Before apply, identity, plan hash and every file in the private source manifest were checked again.
This saved plan has been consumed; do not rerun it. No replacement plan was generated.

The commands below are the general preparation sequence; do not overwrite the saved inputs or
generate a replacement without preserving/reviewing the new plan:

```bash
scripts/build_lambda.sh
cp infra/environments/dev/backend.hcl.example infra/environments/dev/backend.hcl
cp infra/environments/dev/terraform.tfvars.example infra/environments/dev/dev.auto.tfvars
# Fill confirmed account, state bucket, sa-east-1 layer ARN and alert_email in these ignored files.
terraform -chdir=infra/environments/dev init -reconfigure -backend-config=backend.hcl -lockfile=readonly
terraform -chdir=infra/environments/dev plan -out=dev.tfplan
terraform -chdir=infra/environments/dev show -no-color dev.tfplan > infra/environments/dev/dev.plan.txt
```

`-reconfigure` switches from the backend-disabled local checks to the S3 backend; do not migrate
the bootstrap root's local state into the lab state. The S3 backend uses native lock files.
The official [layer list](https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html) lists
`arn:aws:lambda:sa-east-1:336392948345:layer:AWSSDKPandas-Python312:32`
for Python 3.12/x86_64; `GetLayerVersion` confirmed version 32 during this preflight.

`alert_email` is set in the ignored local variables. After an authorised apply, the owner must
confirm the SNS subscription before testing e-mail delivery. The default null value would create
no e-mail subscription or budget notification recipients.
Account for idle CloudWatch alarms/retained logs as well as data/query/job costs; the budget
is informational and does not stop spending.
The lab Terraform created the budget under the owner's separate explicit apply authorisation.
Further budget modifications need new authorisation.

Review checklist: account and region, actual resource count in the saved plan, project tags,
no NAT/RDS/EC2/MWAA, bucket names, IAM statements, Glue worker counts, Athena cutoff.

## 3. Apply — completed under explicit authorisation

```bash
terraform -chdir=infra/environments/dev apply "$PWD/data/aws-dev-plan-2026-10-29/dev-final.tfplan"
terraform -chdir=infra/environments/dev output
```

The saved-plan apply exited 0, with 69 creates and no errors/warnings. Direct reads verified
S3 security/lifecycle and all four uploaded objects; Glue databases, tables, jobs/security;
Lambda configuration and ZIP/layer hash; IAM trust/inline policies; Athena's enforced 100 MiB
cutoff; CloudWatch, EventBridge, SNS and budget recipients/thresholds. Glue job histories are empty.
The dev state is AES256-encrypted/versioned under `case01/dev/terraform.tfstate`; its active
lock was released. A private snapshot is preserved at
`data/aws-dev-apply-2026-10-29/dev.after-apply.tfstate`, excluded from Git with restricted
Windows ACLs. Bootstrap state is unchanged. No automatic retry or cleanup was performed.

Runner/environment preparation was separately authorised for the first functional attempt:
the project-private `data/aws-functional-2026-10-29/aws.config` contains only the data-dev SSO
source and `erl-dev-runner`; no global AWS configuration was changed. The private runtime
environment and scripts preserve the exact settings used. For a later authorised run,
configure an AWS profile that assumes `runner_role_arn` from the SSO session
(`source_profile = data-dev`, `role_arn = <runner_role_arn>`), then fill `.env`:
`AWS_PROFILE=erl-dev-runner` (required explicitly), `ERL_MODE=aws`, `ERL_AWS_ACCOUNT_ID`, `ERL_S3_BUCKET`, `ERL_ATHENA_RESULTS_S3`,
`ERL_LAKEHOUSE_S3`, `ERL_ATHENA_WORKGROUP`, `ERL_GLUE_DB_PREFIX`, `ERL_ALERTS_TOPIC_ARN`.
Load/export `.env` before invoking the CLI (`set -a; . ./.env; set +a`); the CLI does not
automatically read it. Profiles with static credentials and regions other than `sa-east-1`
are refused. Individual AWS steps check the account too, not just the validation task.

The independent review is complete: [INDEPENDENT_REVIEW.md](INDEPENDENT_REVIEW.md).
Execute **one pipeline at a time**, including manual CLI invocations, and keep synthetic source
databases stable while extracting. If a process stops before an object/manifest checkpoint
completes, restart with a new run id; never overwrite immutable run objects.

## 4. Run the hybrid pipeline — has (small) cost

Confirm the two local database containers are healthy before using `containers`. Generate
synthetic data once in the intended isolated local environment; generation resets source tables.
Set `ERL_SOURCE_BACKEND=containers` and the local DB settings, configure the SSO-backed runner
profile, then load/export `.env` as above. Obtain separate approval for the paid validation.
Start with **one reference date** before expanding the scenarios:

```bash
uv run erl generate
uv run erl run --date 2026-10-26
# or through Airflow: scripts/airflow_local.sh test 2026-10-26
```

The first functional validation is restricted to **2026-10-26**. The authorised attempt has
been consumed and failed during ingestion; **do not repeat it or reuse its run id**.
Four file deliveries were prepared, but only the first external-XML Lambda was invoked
(8.623 s runtime, 9.390 s billed, 220/512 MiB peak memory). No automatic retry was performed.
Another paid attempt requires a compatible Lambda release, a reviewed/authorised deployment
plan and fresh single-date execution authorisation. The one-date generator option currently
aliases D2 failure fixtures; this run used a clean canonical D1 slice in fresh `aws1_*` databases.
Private scripts and evidence preserve that setup. Additional dates/scenarios need a later decision.

This exercises all seven sources, three ingestion Lambdas, both DB Glue patterns, S3 Bronze,
dbt/Athena Iceberg Silver/Gold with gates, Glue Gold Load, and SQL Server publication. Airflow
remains the local orchestrator; CloudWatch/SNS/EventBridge and Terraform retain their diagram roles.
Allow roughly 0.2–0.5 DPU-hour for the three Glue jobs of the first date (2 workers, assumed
2–5 minutes/job), costing USD 0.138–0.345 at the confirmed regional USD 0.69/DPU-hour.
A provisional USD 1–3 allowance includes small retries/diagnostics; it is not a spending cap.
Regional rates and assumptions: [TERRAFORM.md](TERRAFORM.md#cost-model-estimate--verify-before-apply).

Evidence to capture (store under `evidence/aws/`): run reports, Lambda/Glue run ids and
CloudWatch log excerpts, `SHOW CREATE TABLE` / `$snapshots` of the Iceberg Gold tables in
Athena, an Athena query of Gold, the publication log from SQL Server, an SNS alert from a forced
failure (degraded market data scenario), Cost Explorer for the day.

Also exercise the reviewed AWS-specific paths: Glue SDK installation/conditional writes,
the Ion manifest control table, Gold disappearance/reversion/empty-snapshot scenarios,
six-decimal ratios versus local results, both Glue log groups, and actual SNS delivery from
CloudWatch, EventBridge and the runner. A successful dbt parse is not an Athena execution test.

Only after these artefacts exist may anything be described as "validated on AWS".
Before teardown, copy required reports, manifests, query results and log excerpts locally;
sanitise them before git. Keep plans, state, raw Terraform outputs and credentials private.
Cost Explorer may lag and its API may be chargeable; obtain approval before querying it.

## 5. Teardown — needs authorisation

```bash
AWS_PROFILE=data-dev uv run python scripts/teardown_aws.py --account-id <id>              # dry-run
AWS_PROFILE=data-dev uv run python scripts/teardown_aws.py --account-id <id> --execute    # typed confirmation
terraform -chdir=infra/environments/dev plan -destroy -out=destroy.tfplan
terraform -chdir=infra/environments/dev apply destroy.tfplan  # authorised human only
aws resourcegroupstaggingapi get-resources --profile data-dev \
  --tag-filters Key=Project,Values=enterprise-risk-liquidity  # should be empty except state
```

The normal lab-only teardown keeps the state bucket (`infra/bootstrap`, `prevent_destroy`).
For the owner's later **full teardown** request, that local guard was temporarily disabled,
all state versions were archived privately, and a reviewed destroy-only bootstrap plan removed
its seven resources after dev was empty. The guard source was restored. No state bucket remains.
Teardown uses the authorised deployment SSO profile, not the pipeline runner: the runner
deliberately has no permission to remove object versions or delete the lab's resources.
The teardown helper additionally requires dev lab bucket tags and exact Silver/Gold database
names, excludes the state bucket and stops on per-object/per-table deletion errors. Incomplete
multipart uploads and unexpected resources still need inventory/review before final teardown.
