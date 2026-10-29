# Terraform

```
infra/
  .tflint.hcl
  bootstrap/              remote-state bucket (S3 lockfile locking, prevent_destroy); local state
  environments/dev/       the AWS validation lab (single root module)
    tests/dev.tftest.hcl  plan-only tests, mocked provider
  modules/secure_bucket/  private, encrypted, versioned, TLS-only bucket (used twice)
    tests/
```

Only one module exists because only the bucket pattern is reused. Lambdas, Glue jobs and Bronze
tables are `for_each` over small maps inside the root module.

## Ownership

| Resource | Owner | Created / destroyed by |
|---|---|---|
| State bucket | `infra/bootstrap` | created once; **kept** when the lab is destroyed (`prevent_destroy`) |
| Data + Athena results buckets, IAM, Lambda, Glue jobs, SNS, EventBridge, alarms, budget, workgroup | `infra/environments/dev` | `terraform apply` / `destroy` |
| Glue databases `erl_dev_{bronze,silver,gold}` and Bronze tables | `infra/environments/dev` | Terraform (Bronze tables generated from `bronze_contract.json`) |
| Silver/Gold tables and views, seed table | **dbt** (`dbt build --target athena`) | dbt creates/replaces; `scripts/teardown_aws.py` drops them before `destroy` |
| Objects under `lakehouse/` (Iceberg data/metadata) | dbt | emptied by `scripts/teardown_aws.py` |
| Objects under `landing/`, `bronze/`, `manifests/`, `quarantine/`, `publish/` | pipeline | emptied by `scripts/teardown_aws.py` |

Terraform never declares Silver/Gold tables, so `terraform plan` does not fight dbt.

## Safety controls in code

* `allowed_account_ids = [var.allowed_account_id]` (12-digit validated) on every provider.
* `environment` must be `dev`; no production environment exists.
* Region explicit (`sa-east-1` default). Default tags: Project, Environment, Purpose,
  ManagedBy, Repository.
* Buckets: public access block (4/4), ACLs disabled, SSE-S3, versioning, TLS-only policy,
  explicit lifecycle (tmp 1 d, landing 30 d, Athena results 7 d, noncurrent versions 7 d),
  `force_destroy = false`.
* Athena workgroup: enforced configuration, engine v3, encrypted results, 100 MB scan cutoff
  per query (minimum allowed: 10 MB).
* Glue jobs: Glue 5.0, G.1X, 2 workers (validated 2–4), 15 min timeout, 0 retries (Airflow
  owns retries), max 1 concurrent run, security configuration (SSE-S3).
* Lambda: python3.12, 512 MB, 60 s, log groups with explicit retention.
* IAM: one role per Lambda (only its own source's prefixes), one role per Glue pattern, a
  pipeline-runner role assumable only by the account's SSO roles. No `s3:*`, `glue:*`,
  `Action: *` or `Resource: *` (asserted in `terraform test`). Wildcards only for object keys
  under scoped prefixes, and `/aws-glue/*` log groups whose names Glue controls.
* Budget USD 5/month with e-mail notifications (informational: budgets do **not** cap spend).

## Checks (all run locally on 2026-10-29 and in CI)

| Check | Command | Local result |
|---|---|---|
| format | `terraform fmt -check -recursive` | pass |
| init without backend + validate | `terraform -chdir=<dir> init -backend=false && terraform validate` | pass (3 dirs) |
| tests (mocked, plan only) | `terraform -chdir=<dir> test` | 11 passed (3 module + 8 dev) |
| TFLint 0.65.0 + AWS ruleset 0.50.0 | `tflint --chdir=<dir> --config=$PWD/.tflint.hcl` | 0 issues |
| Checkov 3.4.0 | `uvx checkov@3.4.0 -d infra --framework terraform` | 148 passed, 0 failed, 33 skipped (below) |

The mocked tests never call AWS: `mock_provider "aws"` with `override_during = plan`, and every
run block uses `command = plan`.

## Checkov exceptions (inline `#checkov:skip` with reason, per resource)

| Check | Resources | Justification |
|---|---|---|
| CKV_AWS_145 (S3 KMS) | 3 buckets | SSE-S3 is enabled; a customer-managed KMS key adds a fixed monthly cost to a temporary lab. Revisit for production. |
| CKV_AWS_144 (cross-region replication) | 3 buckets | single-region temporary lab with synthetic data |
| CKV_AWS_18 (access logging) | 3 buckets | no extra log bucket in the lab; CloudTrail management events cover API activity |
| CKV2_AWS_62 (event notifications) | 3 buckets | Lambdas are invoked synchronously by the pipeline, not by S3 events |
| CKV_AWS_117 (Lambda in VPC) | 3 Lambdas | they only call S3; a VPC would require NAT/endpoints (cost) |
| CKV_AWS_116 (DLQ) | 3 Lambdas | synchronous invocation; the Airflow task receives the error and retries |
| CKV_AWS_173 (env var KMS) | 3 Lambdas | variables are a bucket name and log level, no secrets |
| CKV_AWS_272 (code signing) | 3 Lambdas | out of scope; package is built reproducibly by `scripts/build_lambda.sh` |
| CKV_AWS_158 (log group KMS) | 4 log groups | synthetic data only; AWS-managed encryption |
| CKV_AWS_338 (1-year log retention) | 4 log groups | short explicit retention is intentional for cost and teardown |
| CKV_AWS_99 (Glue security config all-KMS) | 1 | S3 encryption on; CloudWatch/bookmark encryption need a CMK; bookmarks unused |

## Cost model (estimate — verify before apply)

Prices vary by region and change over time; confirm with the AWS Pricing Calculator for
`sa-east-1` before authorising an apply. Per full demo (3 reference dates):

| Service | Usage per demo | Driver |
|---|---|---|
| Glue (Spark, G.1X × 2) | ~9 job runs × 2 DPU × ≈1–2 min (1-min minimum) ≈ 0.3–0.6 DPU-hour | DPU-hour price of the region |
| Athena | ~300 dbt queries × 10 MB minimum billing ≈ 3 GB scanned | per-TB scanned price (≈ cents) |
| Lambda | ~15 invocations × < 2 s × 512 MB | free tier / fractions of a cent |
| S3, CloudWatch Logs, SNS, EventBridge | MB of data, few requests | fractions of a cent |
| Budgets | 1 budget | free (first budgets per account) |

Expected order of magnitude: well under USD 3 per demo, dominated by Glue. Persistent cost
while idle: S3 storage of a few MB and the state bucket (negligible). Nothing in this stack
bills by the hour when idle (no NAT, RDS, MWAA, EC2, ECS/EKS, clusters).

## Apply / destroy procedure

See [AWS_VALIDATION.md](AWS_VALIDATION.md). Apply and destroy are always human-run after
reviewing a saved plan; no script in this repository runs them.
