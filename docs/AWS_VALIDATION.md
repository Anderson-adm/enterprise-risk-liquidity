# AWS validation (Marco 4) — procedure

**Status: not executed.** Nothing has been provisioned. Every step below that creates, changes
or deletes AWS resources requires explicit human authorisation at the time it is run.

Hybrid design: sources, Airflow and the consumer SQL Server stay local. The local pipeline
uploads landing objects to S3 with temporary SSO credentials; Lambdas and Glue ingest them;
dbt runs on Athena producing Iceberg Silver/Gold; the Glue `gold-load` job writes the
publication package to S3; the local publisher loads SQL Server. AWS never connects to
localhost and no local database is exposed.

## 0. Preconditions (read-only)

Run commands from the repository root. **Not authorised yet:** even SSO login and read-only
AWS checks need the owner's next approval. Local preflight confirmed `data-dev` uses SSO,
its region is `sa-east-1`, and no credential environment variables were active; token validity,
actual permissions, service quotas and account inventory have not been checked.

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

## 1. Bootstrap the state bucket (once) — needs authorisation

```bash
terraform -chdir=infra/bootstrap init -input=false -lockfile=readonly
terraform -chdir=infra/bootstrap plan -var='allowed_account_id=<id>' -out=bootstrap.tfplan
terraform -chdir=infra/bootstrap show -no-color bootstrap.tfplan
```

Stop and review the saved plan. After explicit bootstrap-apply approval:
`terraform -chdir=infra/bootstrap apply bootstrap.tfplan`.
Back up `infra/bootstrap/terraform.tfstate` privately; this root uses local state. The state
bucket remains private, versioned, encrypted and protected by `prevent_destroy`.

## 2. Plan the lab — AWS access; separate approval

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
for Python 3.12/x86_64 at this preflight; confirm availability during authorised AWS checks.

Set `alert_email` and confirm the resulting SNS subscription before testing e-mail delivery.
The default null value creates no e-mail subscription and no budget notification recipients.
Account for idle CloudWatch alarms/retained logs as well as data/query/job costs; the budget
is informational and does not stop spending.

Review checklist: account and region, actual resource count in the saved plan, project tags,
no NAT/RDS/EC2/MWAA, bucket names, IAM statements, Glue worker counts, Athena cutoff.

## 3. Apply — needs authorisation

```bash
terraform -chdir=infra/environments/dev apply dev.tfplan
terraform -chdir=infra/environments/dev output
```

Configure an AWS profile that assumes `runner_role_arn` from the SSO session
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
# After reviewing success/cost/evidence, run 2026-10-27 then 2026-10-28 serially.
# or through Airflow: scripts/airflow_local.sh test 2026-10-26
```

This exercises all seven sources, three ingestion Lambdas, both DB Glue patterns, S3 Bronze,
dbt/Athena Iceberg Silver/Gold with gates, Glue Gold Load, and SQL Server publication. Airflow
remains the local orchestrator; CloudWatch/SNS/EventBridge and Terraform retain their diagram roles.
Allow roughly 0.2–0.5 DPU-hour for the three Glue jobs of the first date (2 workers, assumed
2–5 minutes/job). A provisional USD 1–3 allowance includes small retries/diagnostics, not a
regional quote or spending cap; confirm sa-east-1 DPU, Athena and monitoring prices first.

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

The state bucket remains (it is in `infra/bootstrap`, protected by `prevent_destroy`).
Teardown uses the authorised deployment SSO profile, not the pipeline runner: the runner
deliberately has no permission to remove object versions or delete the lab's resources.
The teardown helper additionally requires dev lab bucket tags and exact Silver/Gold database
names, excludes the state bucket and stops on per-object/per-table deletion errors. Incomplete
multipart uploads and unexpected resources still need inventory/review before final teardown.
