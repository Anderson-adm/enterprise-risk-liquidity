# AWS validation (Marco 4) — procedure

**Status: not executed.** Nothing has been provisioned. Every step below that creates, changes
or deletes AWS resources requires explicit human authorisation at the time it is run.

Hybrid design: sources, Airflow and the consumer SQL Server stay local. The local pipeline
uploads landing objects to S3 with temporary SSO credentials; Lambdas and Glue ingest them;
dbt runs on Athena producing Iceberg Silver/Gold; the Glue `gold-load` job writes the
publication package to S3; the local publisher loads SQL Server. AWS never connects to
localhost and no local database is exposed.

## 0. Preconditions (read-only)

```bash
aws sso login --profile data-dev
aws sts get-caller-identity --profile data-dev        # must be the data-dev account
aws configure get region --profile data-dev           # sa-east-1
```

The `default` profile on the development machine holds **static access keys**. Never use it
for this project; prefer removing or rotating those keys.

Inventory before (read-only, for the evidence file):

```bash
aws resourcegroupstaggingapi get-resources --profile data-dev \
  --tag-filters Key=Project,Values=enterprise-risk-liquidity
```

## 1. Bootstrap the state bucket (once) — needs authorisation

```bash
cd infra/bootstrap
terraform init
terraform plan -var allowed_account_id=<id> -out=bootstrap.tfplan    # review
terraform apply bootstrap.tfplan                                       # authorised human only
```

Keep `infra/bootstrap/terraform.tfstate` safe (it is git-ignored).

## 2. Plan the lab — no cost

```bash
scripts/build_lambda.sh
cd infra/environments/dev
cp backend.hcl.example backend.hcl            # fill bucket name
cp terraform.tfvars.example dev.auto.tfvars   # account id, layer ARN (see AWS SDK for pandas docs)
terraform init -backend-config=backend.hcl
terraform plan -out=dev.tfplan
terraform show -no-color dev.tfplan > dev.plan.txt   # attach to the review (do not commit)
```

Review checklist: account and region, about 66 resources (count derived from the code; confirm in the plan) all tagged `Project=enterprise-risk-liquidity`,
no NAT/RDS/EC2/MWAA, bucket names, IAM statements, Glue worker counts, Athena cutoff.

## 3. Apply — needs authorisation

```bash
terraform apply dev.tfplan
terraform output
```

Configure an AWS profile that assumes `runner_role_arn` from the SSO session
(`source_profile = data-dev`, `role_arn = <runner_role_arn>`), then fill `.env`:
`ERL_MODE=aws`, `ERL_AWS_ACCOUNT_ID`, `ERL_S3_BUCKET`, `ERL_ATHENA_RESULTS_S3`,
`ERL_LAKEHOUSE_S3`, `ERL_ATHENA_WORKGROUP`, `ERL_GLUE_DB_PREFIX`, `ERL_ALERTS_TOPIC_ARN`.

## 4. Run the hybrid pipeline — has (small) cost

```bash
uv run erl generate
for d in 2026-10-26 2026-10-27 2026-10-28; do uv run erl run --date $d; done
# or through Airflow: scripts/airflow_local.sh test 2026-10-26
```

Evidence to capture (store under `evidence/aws/`): run reports, Lambda/Glue run ids and
CloudWatch log excerpts, `SHOW CREATE TABLE` / `$snapshots` of the Iceberg Gold tables in
Athena, an Athena query of Gold, the publication log from SQL Server, an SNS alert from a forced
failure (degraded market data scenario), Cost Explorer for the day.

Only after these artefacts exist may anything be described as "validated on AWS".

## 5. Teardown — needs authorisation

```bash
python scripts/teardown_aws.py --account-id <id>              # dry-run: lists what would go
python scripts/teardown_aws.py --account-id <id> --execute    # typed confirmation required
terraform -chdir=infra/environments/dev plan -destroy -out=destroy.tfplan
terraform -chdir=infra/environments/dev apply destroy.tfplan  # authorised human only
aws resourcegroupstaggingapi get-resources --profile data-dev \
  --tag-filters Key=Project,Values=enterprise-risk-liquidity  # should be empty except state
```

The state bucket remains (it is in `infra/bootstrap`, protected by `prevent_destroy`).
