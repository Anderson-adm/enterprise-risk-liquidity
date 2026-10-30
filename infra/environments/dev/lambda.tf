# Ingestion Lambdas for file-delivered sources (design: "Lambda B3 diaria", "XML internos",
# "XML externos"). Invoked synchronously by the pipeline (RequestResponse), so failures
# surface in the calling Airflow task; there is no async queue to dead-letter.

resource "aws_cloudwatch_log_group" "lambda" {
  #checkov:skip=CKV_AWS_158:Logs contain no secrets or personal data (synthetic); AWS-managed encryption avoids CMK cost
  #checkov:skip=CKV_AWS_338:Temporary lab: short explicit retention (var.log_retention_days) is intentional for cost and teardown
  for_each = local.file_sources

  name              = "/aws/lambda/${var.name_prefix}-${each.value.function}"
  retention_in_days = var.log_retention_days
}

# One package for the three functions. It bundles its own PyArrow/lxml pinned to uv.lock: the
# AWS-managed pandas layer's PyArrow could not write the Zstandard codec of the Bronze contract
# (failed AWS attempt 2026-10-30). The ZIP exceeds the 50 MB direct-upload limit, hence S3.
resource "aws_s3_object" "lambda_package" {
  bucket      = module.artifacts_bucket.id
  key         = local.lambda_package_key
  source      = local.lambda_package
  source_hash = filebase64sha256(local.lambda_package)
}

resource "aws_lambda_function" "ingest" {
  #checkov:skip=CKV_AWS_117:No VPC by design: the functions only call S3 and must not need NAT (cost); see docs/TERRAFORM.md
  #checkov:skip=CKV_AWS_116:Invoked synchronously (RequestResponse); errors return to the Airflow task that retries, there is no async event to dead-letter
  #checkov:skip=CKV_AWS_173:Environment holds only a bucket name and log level, no secrets; encrypted at rest with the AWS-managed key
  #checkov:skip=CKV_AWS_272:Code signing (AWS Signer profile) out of scope for the lab; package built reproducibly and hashed by scripts/build
  for_each = local.file_sources

  function_name     = "${var.name_prefix}-${each.value.function}"
  description       = "Validate and append ${each.key} deliveries to Bronze"
  role              = aws_iam_role.lambda[each.key].arn
  runtime           = "python3.12"
  architectures     = ["x86_64"]
  handler           = each.value.handler
  s3_bucket         = module.artifacts_bucket.id
  s3_key            = aws_s3_object.lambda_package.key
  s3_object_version = aws_s3_object.lambda_package.version_id
  source_code_hash  = filebase64sha256(local.lambda_package)
  memory_size       = 512
  timeout           = 60

  reserved_concurrent_executions = var.lambda_reserved_concurrency

  environment {
    variables = {
      ERL_S3_BUCKET = module.data_bucket.id
      LOG_LEVEL     = "INFO"
    }
  }

  logging_config {
    log_format = "Text" # the handler already emits JSON lines
    log_group  = aws_cloudwatch_log_group.lambda[each.key].name
  }

  tracing_config {
    mode = "PassThrough"
  }
}
