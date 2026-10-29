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
| Glue databases `erl_dev_{bronze,silver,gold}`, Bronze tables and `ingestion_deliveries` | `infra/environments/dev` | Terraform (business tables generated from `bronze_contract.json`; manifest control table uses Ion JSON SerDe) |
| Silver/Gold tables and views, seed table | **dbt** (`dbt build --target athena`) | dbt creates/replaces; `scripts/teardown_aws.py` drops them before `destroy` |
| Objects under `lakehouse/` (Iceberg data/metadata) | dbt | emptied by `scripts/teardown_aws.py` |
| Objects under `landing/`, `bronze/`, `manifests/`, `quarantine/`, `publish/` | pipeline | emptied by `scripts/teardown_aws.py` |

Terraform never declares Silver/Gold tables, so `terraform plan` does not fight dbt.

## Safety controls in code

* `allowed_account_ids = [var.allowed_account_id]` (12-digit validated) on every provider.
* Providers explicitly select `aws_profile` (default `data-dev`, `default` rejected). The
  backend example also selects a profile and allowed account; backend credentials are separate.
* `environment` must be `dev`; no production environment exists.
* Region restricted to `sa-east-1` by validation. Default tags: Project, Environment, Purpose,
  ManagedBy, Repository.
* Buckets: public access block (4/4), ACLs disabled, SSE-S3, versioning, TLS-only policy,
  explicit lifecycle (tmp 1 d, landing 30 d, Athena results 7 d, noncurrent versions 7 d),
  `force_destroy = false`.
* Athena workgroup: enforced configuration, engine v3, encrypted results, 100 MB scan cutoff
  per query (minimum allowed: 10 MB).
* Glue jobs: Glue 5.0, G.1X, 2 workers (validated 2–4), 15 min timeout, 0 retries (Airflow
  owns retries), max 1 concurrent run, security configuration (SSE-S3).
  The complete Python SDK dependency closure is pinned in `glue/requirements.txt`, aligned
  with `uv.lock`, and installed through `--additional-python-modules`. Glue 5.0's bundled
  boto3 1.34.131 predates the conditional S3 writes used here. Dependency installation still
  requires validation in Glue. Logs use the 5.0 `--custom-logGroup-prefix` with two managed
  groups (`/error`, `/output`), each with explicit retention.
* Lambda: python3.12, 512 MB, 60 s, log groups with explicit retention.
* IAM: one role per Lambda (only its own source's prefixes), roles for Glue DB ingestion and
  Gold load (the GB/TB jobs share the ingestion role), a
  pipeline-runner role assumable only by the account's SSO roles. No `s3:*`, `glue:*`,
  or `Action: *`. Resource `*` is allowed only for `cloudwatch:PutMetricData`, which has no
  resource ARN, constrained to namespace `Glue`; mocked tests enforce that exception.
  Object keys and Glue log group names otherwise use scoped prefixes.
* SNS alerts deliberately omit topic-level KMS encryption in this synthetic lab. The AWS
  managed SNS key cannot grant CloudWatch/EventBridge the permissions needed to deliver
  alerts; production requires a customer-managed key and publisher permissions. The SNS
  policy grants EventBridge its service principal on the exact topic, following the documented
  resource-policy pattern. No alert may contain credentials or real business data.
* Budget USD 5/month with e-mail notifications (informational: budgets do **not** cap spend).

## Checks (all run locally on 2026-10-29 and in CI)

| Check | Command | Local result |
|---|---|---|
| format | `terraform fmt -check -recursive` | pass |
| init without backend + validate | `terraform -chdir=<dir> init -backend=false && terraform validate` | pass (3 dirs) |
| tests (mocked, plan only) | `terraform -chdir=<dir> test` | 13 passed (3 module + 10 dev), independent review |
| TFLint 0.65.0 + AWS ruleset 0.50.0 | `tflint --chdir=<dir> --config=$PWD/.tflint.hcl` | 0 issues |
| Checkov 3.4.0 | `uvx checkov@3.4.0 -d infra --framework terraform` | 148 passed, 0 failed, 36 skipped (below) |

Independent review evidence: [../evidence/review/final_terraform.txt](../evidence/review/final_terraform.txt),
[TFLint](../evidence/review/final_tflint.txt), [Checkov](../evidence/review/final_checkov.txt).
The check runner now propagates format, TFLint and Checkov failures (previously masked by
shell `&&`/`||` handling); regression tests inject nonzero exits for each tool.

The mocked tests never call AWS: `mock_provider "aws"` with `override_during = plan`, and every
run block uses `command = plan`.

## Checkov exceptions (inline `#checkov:skip` with reason, per resource)

18 inline annotations now produce 36 skipped checks because the bucket module is instantiated twice
(plus the state bucket) and the Lambda/log-group resources use `for_each` over three sources.
The independent review reassessed all original 33 skips. Their stated lab trade-offs remain
applicable; CKV_AWS_158/338 each gain one instance for the additional Glue log group, and
CKV_AWS_26 adds one explicit SNS exception. Reproduced on 2026-10-29 with Checkov 3.4.0.
Every exception is a cost/scope trade-off of a **temporary lab with synthetic data**; none is
acceptable as-is for production, and the last column says what production would need.

| Check | # | Resources | Why it is skipped in the lab | Residual risk accepted | Production would need |
|---|---|---|---|---|---|
| CKV_AWS_145 S3 encryption with KMS CMK | 3 | state, data, athena-results buckets | SSE-S3 (AES-256) is on; a customer-managed key adds a fixed monthly cost | key policy cannot restrict decrypt separately from S3 access | SSE-KMS with a CMK and key policy |
| CKV_AWS_144 cross-region replication | 3 | same 3 buckets | single-region lab; replication doubles storage | regional outage loses lab data (state is versioned in-region) | CRR or backup for state and curated data |
| CKV_AWS_18 server access logging | 3 | same 3 buckets | no extra log bucket in the lab | **object-level reads/writes are not audited** (CloudTrail management events do not include S3 data events) | access logs or CloudTrail S3 data events |
| CKV2_AWS_62 event notifications | 3 | same 3 buckets | the ingestion Lambdas are invoked synchronously by the orchestrator, not by S3 events | none (no consumer would receive the events) | only if event-driven ingestion is adopted |
| CKV_AWS_117 Lambda inside a VPC | 3 | `ingest["market_data"/"internal_xml"/"external_xml"]` | the functions only call S3/CloudWatch; a VPC needs NAT or endpoints (cost) | traffic to S3 uses public AWS endpoints (TLS, IAM-scoped) | VPC + S3/Logs gateway/interface endpoints |
| CKV_AWS_116 dead-letter queue | 3 | same 3 Lambdas | synchronous `RequestResponse` invocation: the error returns to the Airflow task, which retries and alerts | none for sync calls | DLQ/on-failure destination if invoked asynchronously |
| CKV_AWS_173 env variables encrypted with CMK | 3 | same 3 Lambdas | variables hold a bucket name and log level only; encrypted with the AWS-managed key | none while no secret is placed there | CMK, and secrets only in Secrets Manager |
| CKV_AWS_272 code signing | 3 | same 3 Lambdas | AWS Signer is out of the lab's scope; the zip is built deterministically by `scripts/build_lambda.sh` | an altered package would not be rejected by signature | Signer profile + code-signing config |
| CKV_AWS_158 log group KMS | 5 | 2 Glue log groups, 3 Lambda log groups | logs carry no secrets or personal data (synthetic); AWS-managed encryption | as CKV_AWS_145 | CMK-encrypted log groups |
| CKV_AWS_338 log retention ≥ 1 year | 5 | same 5 log groups | short explicit retention (`var.log_retention_days`) for cost and clean teardown | logs disappear after the retention window | retention per audit policy |
| CKV_AWS_99 Glue security configuration all-KMS | 1 | `aws_glue_security_configuration.this` | S3 encryption is on; CloudWatch/bookmark encryption require a CMK; bookmarks are unused | Glue job logs use AWS-managed encryption | CMK for CloudWatch and bookmarks |
| CKV_AWS_26 SNS topic encryption | 1 | `aws_sns_topic.alerts` | synthetic alerts only; `alias/aws/sns` prevents service-originated alerts, a compatible CMK adds recurring cost | no application-level KMS encryption of alert payloads at rest | CMK with CloudWatch, EventBridge and runner key permissions |

Compatibility references: [SNS KMS publisher requirements](https://docs.aws.amazon.com/sns/latest/dg/sns-key-management.html),
[EventBridge resource policies](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-use-resource-based.html),
[Glue 5.0 logging](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging.html),
[Glue Python libraries](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html).

## Cost model (estimate — verify before apply)

Prices vary by region and change over time; confirm with the AWS Pricing Calculator for
`sa-east-1` before authorising an apply. Per full demo (3 reference dates):

| Service | Usage per demo | Driver |
|---|---|---|
| Glue (Spark, G.1X × 2) | ~9 job runs × 2 DPU × assumed 2–5 min ≈ 0.6–1.5 DPU-hour; first date only: 0.2–0.5 DPU-hour | DPU-hour price of the region; actual startup/install/runtime unmeasured |
| Athena | ~300 dbt queries × 10 MB minimum billing ≈ 3 GB scanned | per-TB scanned price (≈ cents) |
| Lambda | ~15 invocations × < 2 s × 512 MB | free tier / fractions of a cent |
| S3, CloudWatch Logs, SNS, EventBridge | MB of data, few requests | fractions of a cent |
| Budgets | 1 budget | free (first budgets per account) |

For the first date, reserve a provisional USD 1–3 including small retries/diagnostics; this is
not a measured regional quote or spending cap. The [Glue pricing examples](https://aws.amazon.com/glue/pricing/)
use USD 0.44/DPU-hour, implying USD 0.09–0.22 for the assumed first-date compute; confirm
sa-east-1 rates before apply. The [Athena pricing examples](https://aws.amazon.com/athena/pricing/)
use USD 5/TB; actual billed scans, requests and query counts still need measurement.
Idle costs can also include CloudWatch alarms
and retained logs, beyond S3 and the state bucket. There is no NAT, RDS, MWAA, EC2, ECS/EKS or
always-on compute cluster. Confirm current monitoring/storage prices before authorising a run;
see [CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/).
With `alert_email = null`, the budget has no notification recipients and SNS has no e-mail
subscription; set and confirm a recipient when validating human alert delivery.

## Apply / destroy procedure

See [AWS_VALIDATION.md](AWS_VALIDATION.md). Apply and destroy are always human-run after
reviewing a saved plan; no script in this repository runs them.
