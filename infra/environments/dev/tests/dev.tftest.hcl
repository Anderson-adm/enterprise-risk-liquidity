# Plan-only tests against a mocked AWS provider: no credentials, no real resources.

mock_provider "aws" {
  override_during = plan

  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::erl-dev-mock", id = "erl-dev-mock" }
  }
  mock_resource "aws_lambda_function" {
    defaults = { arn = "arn:aws:lambda:sa-east-1:111111111111:function:erl-dev-mock" }
  }
  mock_resource "aws_glue_job" {
    defaults = { arn = "arn:aws:glue:sa-east-1:111111111111:job/erl-dev-mock" }
  }
  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:sa-east-1:111111111111:erl-dev-alerts" }
  }
  mock_resource "aws_athena_workgroup" {
    defaults = { arn = "arn:aws:athena:sa-east-1:111111111111:workgroup/erl-dev" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:sa-east-1:111111111111:log-group:/mock" }
  }
  mock_resource "aws_cloudwatch_event_rule" {
    defaults = { arn = "arn:aws:events:sa-east-1:111111111111:rule/erl-dev-mock" }
  }

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "111111111111"
    }
  }
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"sts:AssumeRole\",\"Principal\":{\"Service\":\"glue.amazonaws.com\"}}]}"
    }
  }
}

variables {
  allowed_account_id      = "111111111111"
  lambda_package_path     = "tests/fixtures/lambda.zip"
  lambda_pandas_layer_arn = "arn:aws:lambda:sa-east-1:336392948345:layer:AWSSDKPandas-Python312:1"
}

run "secure_and_cost_controlled_defaults" {
  command = plan

  assert {
    condition     = var.aws_region == "sa-east-1"
    error_message = "Default region must be sa-east-1."
  }

  assert {
    condition = (
      aws_athena_workgroup.this.configuration[0].enforce_workgroup_configuration &&
      aws_athena_workgroup.this.configuration[0].bytes_scanned_cutoff_per_query == 104857600 &&
      aws_athena_workgroup.this.configuration[0].engine_version[0].selected_engine_version == "Athena engine version 3" &&
      aws_athena_workgroup.this.configuration[0].result_configuration[0].encryption_configuration[0].encryption_option == "SSE_S3"
    )
    error_message = "Athena workgroup must enforce config, engine v3, a scan cutoff and encrypted results."
  }

  assert {
    condition     = length(aws_glue_catalog_table.bronze) == 13
    error_message = "One Bronze table per contract entity is expected."
  }

  assert {
    condition     = alltrue([for t in aws_glue_catalog_table.bronze : t.parameters["projection.enabled"] == "true"])
    error_message = "Bronze tables must use partition projection (no crawlers / MSCK)."
  }

  assert {
    condition     = alltrue([for f in aws_lambda_function.ingest : f.runtime == "python3.12" && f.timeout <= 60])
    error_message = "Lambdas must use python3.12 and a short timeout."
  }

  assert {
    condition = alltrue([for j in aws_glue_job.this :
      j.glue_version == "5.0" && j.number_of_workers <= 4 && j.max_retries == 0 && j.timeout <= 15
      && j.execution_property[0].max_concurrent_runs == 1 && j.security_configuration == "erl-dev"
    ])
    error_message = "Glue jobs must be small, bounded, single-run and use the security configuration."
  }

  assert {
    condition     = alltrue([for g in aws_cloudwatch_log_group.lambda : g.retention_in_days == 14]) && aws_cloudwatch_log_group.glue.retention_in_days == 14
    error_message = "Log groups must have explicit retention."
  }

  assert {
    condition     = aws_budgets_budget.monthly.limit_amount == "5.00"
    error_message = "Budget defaults to USD 5."
  }

  assert {
    condition     = aws_sns_topic.alerts.kms_master_key_id == "alias/aws/sns"
    error_message = "Alert topic must be encrypted."
  }
}

run "iam_policies_have_no_service_wildcards" {
  command = plan

  assert {
    condition = alltrue([for p in concat(
      [for r in aws_iam_role_policy.lambda : r.policy],
      [aws_iam_role_policy.glue_db_ingest.policy, aws_iam_role_policy.glue_gold_load.policy, aws_iam_role_policy.runner.policy],
      ) : !strcontains(p, "\"s3:*\"") && !strcontains(p, "\"glue:*\"") && !strcontains(p, "\"Action\":\"*\"") && !strcontains(p, "\"Resource\":\"*\"")
    ])
    error_message = "Inline policies must not grant service-wide or resource-wide wildcards."
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.lambda["market_data"].policy, "landing/files/internal_xml")
    error_message = "Each Lambda may only read its own source's landing prefix."
  }

  assert {
    condition     = strcontains(aws_iam_role.runner.assume_role_policy, "AWSReservedSSO_")
    error_message = "Runner role must only be assumable by the account's SSO roles by default."
  }
}

run "alert_email_enables_subscription_and_budget_notifications" {
  command = plan

  variables {
    alert_email = "alerts@example.com"
  }

  assert {
    condition     = length(aws_sns_topic_subscription.email) == 1 && length(aws_budgets_budget.monthly.notification) == 2
    error_message = "Alert e-mail must subscribe to SNS and receive budget notifications."
  }
}

run "rejects_prod_environment" {
  command = plan
  variables {
    environment = "prod"
  }
  expect_failures = [var.environment]
}

run "rejects_malformed_account_id" {
  command = plan
  variables {
    allowed_account_id = "data-dev"
  }
  expect_failures = [var.allowed_account_id]
}

run "rejects_too_small_athena_cutoff" {
  command = plan
  variables {
    athena_bytes_scanned_cutoff = 1024
  }
  expect_failures = [var.athena_bytes_scanned_cutoff]
}

run "rejects_untrusted_lambda_layer" {
  command = plan
  variables {
    lambda_pandas_layer_arn = "arn:aws:lambda:sa-east-1:999999999999:layer:AWSSDKPandas-Python312:1"
  }
  expect_failures = [var.lambda_pandas_layer_arn]
}

run "rejects_oversized_glue_jobs" {
  command = plan
  variables {
    glue_workers = { db_gb = 2, db_tb = 10, gold_load = 2 }
  }
  expect_failures = [var.glue_workers]
}
