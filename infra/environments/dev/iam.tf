# Least-privilege roles. Every S3 statement is scoped to the prefixes the component writes or
# reads; the only wildcards are (a) object keys under those prefixes and (b) the Glue-managed
# /aws-glue/* log groups, whose names Glue chooses. See docs/TERRAFORM.md.

locals {
  data_arn    = module.data_bucket.arn
  results_arn = module.athena_results_bucket.arn
  catalog_arn = "arn:aws:glue:${var.aws_region}:${local.account_id}:catalog"
  db_arn      = { for k, db in aws_glue_catalog_database.layer : k => "arn:aws:glue:${var.aws_region}:${local.account_id}:database/${db.name}" }
  table_arn   = { for k, db in aws_glue_catalog_database.layer : k => "arn:aws:glue:${var.aws_region}:${local.account_id}:table/${db.name}/*" }
  glue_logs   = "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*"
  glue_metrics = {
    Sid       = "GlueMetrics"
    Effect    = "Allow"
    Action    = ["cloudwatch:PutMetricData"]
    Resource  = ["*"] # PutMetricData has no resource ARN; constrain the namespace instead.
    Condition = { StringEquals = { "cloudwatch:namespace" = "Glue" } }
  }

  runner_principals = length(var.runner_trusted_principal_arns) > 0 ? var.runner_trusted_principal_arns : [
    "arn:aws:iam::${local.account_id}:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_*",
    "arn:aws:iam::${local.account_id}:role/aws-reserved/sso.amazonaws.com/*/AWSReservedSSO_*",
  ]
}

# ------------------------------------------------------------------ Lambda (one role per source)

resource "aws_iam_role" "lambda" {
  for_each = local.file_sources

  name = "${var.name_prefix}-${each.value.function}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  for_each = local.file_sources

  name = "ingest-${each.key}"
  role = aws_iam_role.lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadOwnLanding"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${local.data_arn}/landing/files/${each.key}/*"]
      },
      {
        Sid    = "WriteOwnBronzeManifestsQuarantine"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = [
          "${local.data_arn}/bronze/raw/${each.key}/*",
          "${local.data_arn}/bronze/structured/${each.key}/*",
          "${local.data_arn}/manifests/ingestion/${each.key}/*",
          "${local.data_arn}/quarantine/${each.key}/*",
        ]
      },
      {
        Sid      = "ReadOwnManifestsForDuplicateCheck"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${local.data_arn}/manifests/ingestion/${each.key}/*"]
      },
      {
        Sid       = "ListOwnManifests"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = [local.data_arn]
        Condition = { StringLike = { "s3:prefix" = ["manifests/ingestion/${each.key}/*"] } }
      },
      {
        Sid      = "OwnLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.lambda[each.key].arn}:*"]
      },
    ]
  })
}

# ------------------------------------------------------------------ Glue: database ingestion

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_db_ingest" {
  name               = "${var.name_prefix}-glue-db-ingest"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy" "glue_db_ingest" {
  name = "db-ingest"
  role = aws_iam_role.glue_db_ingest.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.glue_metrics,
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [local.data_arn]
      },
      {
        Sid      = "ReadOwnCode"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${module.artifacts_bucket.arn}/case01/glue/extract-liquidez/*", "${module.artifacts_bucket.arn}/case01/glue/extract-enquadramento/*"]
      },
      {
        Sid    = "ReadLandingManifests"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${local.data_arn}/landing/db/*",
          "${local.data_arn}/manifests/ingestion/*",
          "${local.data_arn}/tmp/glue/*",
        ]
      },
      {
        Sid    = "WriteBronzeAndManifests"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = concat(
          [for s in local.db_sources : "${local.data_arn}/bronze/structured/${s}/*"],
          [for s in local.db_sources : "${local.data_arn}/manifests/ingestion/${s}/*"],
          ["${local.data_arn}/tmp/glue/*"],
        )
      },
      {
        Sid      = "CleanOwnTemporaryOutputAndAbortedCommits"
        Effect   = "Allow"
        Action   = ["s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${local.data_arn}/tmp/glue/*"]
      },
      {
        Sid      = "AbortConditionalBronzeCopy"
        Effect   = "Allow"
        Action   = ["s3:AbortMultipartUpload"]
        Resource = [for s in local.db_sources : "${local.data_arn}/bronze/structured/${s}/*"]
      },
      {
        Sid       = "ListScopedPrefixes"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = [local.data_arn]
        Condition = { StringLike = { "s3:prefix" = ["landing/db/*", "manifests/ingestion/*", "tmp/glue/*", "bronze/structured/*"] } }
      },
      {
        Sid      = "GlueManagedLogGroups"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = [local.glue_logs, "${local.glue_logs}:*"]
      },
    ]
  })
}

# ------------------------------------------------------------------ Glue: Gold load (export package)

resource "aws_iam_role" "glue_gold_export" {
  name               = "${var.name_prefix}-glue-export-risk-analytics"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy" "glue_gold_export" {
  name = "export-risk-analytics"
  role = aws_iam_role.glue_gold_export.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      local.glue_metrics,
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [local.data_arn]
      },
      {
        Sid      = "ReadGoldCatalog"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        Resource = [local.catalog_arn, local.db_arn["gold"], local.table_arn["gold"]]
      },
      {
        Sid      = "ReadOwnCode"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${module.artifacts_bucket.arn}/case01/glue/export-risk-analytics/*"]
      },
      {
        Sid      = "ReadIcebergFiles"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${local.data_arn}/lakehouse/*"]
      },
      {
        Sid       = "ListLakehouse"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = [local.data_arn]
        Condition = { StringLike = { "s3:prefix" = ["lakehouse/*", "tmp/glue/*"] } }
      },
      {
        Sid      = "WritePublicationPackage"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"]
        Resource = ["${local.data_arn}/publish/*"] # conditional copy + checksum of its own products
      },
      {
        Sid      = "SparkTemporaryOutput"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${local.data_arn}/tmp/glue/*"]
      },
      {
        Sid      = "GlueManagedLogGroups"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = [local.glue_logs, "${local.glue_logs}:*"]
      },
    ]
  })
}

# ------------------------------------------------------------------ Pipeline runner (local Airflow / CLI)

resource "aws_iam_role" "runner" {
  name                 = "${var.name_prefix}-pipeline-runner"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
      Action    = "sts:AssumeRole"
      Condition = { ArnLike = { "aws:PrincipalArn" = local.runner_principals } }
    }]
  })
}

resource "aws_iam_role_policy" "runner" {
  name = "pipeline-runner"
  role = aws_iam_role.runner.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "UploadLanding"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${local.data_arn}/landing/*"]
      },
      {
        Sid    = "ReadLakeForReportsAndAthena"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${local.data_arn}/landing/*",
          "${local.data_arn}/bronze/*",
          "${local.data_arn}/manifests/*",
          "${local.data_arn}/publish/*",
        ]
      },
      {
        Sid      = "DbtIcebergTables"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${local.data_arn}/lakehouse/*"]
      },
      {
        Sid       = "ListScopedPrefixes"
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = [local.data_arn]
        Condition = { StringLike = { "s3:prefix" = ["landing/*", "bronze/*", "manifests/*", "publish/*", "lakehouse/*"] } }
      },
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [local.data_arn, local.results_arn]
      },
      {
        Sid      = "AthenaResults"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:AbortMultipartUpload"]
        Resource = [local.results_arn, "${local.results_arn}/athena/*"]
      },
      {
        Sid    = "AthenaWorkgroup"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
          "athena:StopQueryExecution", "athena:GetWorkGroup", "athena:BatchGetQueryExecution",
        ]
        Resource = [aws_athena_workgroup.this.arn]
      },
      {
        Sid      = "CatalogReadAll"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"]
        Resource = concat([local.catalog_arn], values(local.db_arn), values(local.table_arn))
      },
      {
        Sid    = "DbtOwnsSilverGoldTables"
        Effect = "Allow"
        Action = [
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:BatchDeleteTable",
          "glue:GetTableVersions", "glue:DeleteTableVersion", "glue:BatchDeleteTableVersion",
        ]
        Resource = [local.catalog_arn, local.db_arn["silver"], local.db_arn["gold"], local.table_arn["silver"], local.table_arn["gold"]]
      },
      {
        Sid      = "InvokeIngestionLambdas"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [for f in aws_lambda_function.ingest : f.arn]
      },
      {
        Sid      = "RunGlueJobs"
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
        Resource = [for j in aws_glue_job.this : j.arn]
      },
      {
        Sid      = "PublishAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.alerts.arn]
      },
    ]
  })
}
