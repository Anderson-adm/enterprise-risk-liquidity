# Glue jobs (design: "Glue job - Volume em GB", "Glue job - Volume em TB", "Glue job - Load").
# One db_ingest script implements both ingestion patterns; the TB pattern repartitions the
# write and can run with more workers. Spark is required for the TB pattern; the lab uses
# the minimum of 2 G.1X workers and tiny inputs.

locals {
  glue_jobs = {
    db-gb = {
      script  = "db_ingest.py"
      role    = aws_iam_role.glue_db_ingest.arn
      workers = var.glue_workers["db_gb"]
      args    = { "--volume_profile" = "gb" }
    }
    db-tb = {
      script  = "db_ingest.py"
      role    = aws_iam_role.glue_db_ingest.arn
      workers = var.glue_workers["db_tb"]
      args    = { "--volume_profile" = "tb" }
    }
    gold-load = {
      script  = "gold_load.py"
      role    = aws_iam_role.glue_gold_load.arn
      workers = var.glue_workers["gold_load"]
      args = {
        "--datalake-formats"         = "iceberg"
        "--gold_database"            = aws_glue_catalog_database.layer["gold"].name
        "--consumer_contract_s3_key" = aws_s3_object.contract["consumer_contract.json"].key
        "--conf"                     = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=s3://${module.data_bucket.id}/lakehouse/"
      }
    }
  }
  glue_scripts = toset(["db_ingest.py", "gold_load.py"])
}

resource "aws_s3_object" "glue_script" {
  for_each = local.glue_scripts

  bucket = module.data_bucket.id
  key    = "scripts/glue/${each.key}"
  source = "${path.module}/../../../glue/jobs/${each.key}"
  etag   = filemd5("${path.module}/../../../glue/jobs/${each.key}")
}

resource "aws_s3_object" "contract" {
  for_each = toset(["bronze_contract.json", "consumer_contract.json"])

  bucket = module.data_bucket.id
  key    = "scripts/contracts/${each.key}"
  source = "${path.module}/../../../src/erl/contracts/${each.key}"
  etag   = filemd5("${path.module}/../../../src/erl/contracts/${each.key}")
}

resource "aws_cloudwatch_log_group" "glue" {
  #checkov:skip=CKV_AWS_158:Logs contain no secrets or personal data (synthetic); AWS-managed encryption avoids CMK cost
  #checkov:skip=CKV_AWS_338:Temporary lab: short explicit retention (var.log_retention_days) is intentional for cost and teardown
  for_each          = toset(["error", "output"])
  name              = "/aws-glue/${var.name_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_glue_security_configuration" "this" {
  #checkov:skip=CKV_AWS_99:S3 encryption enabled (SSE-S3); CloudWatch/bookmark encryption require a KMS CMK (cost) and bookmarks are unused
  name = var.name_prefix

  encryption_configuration {
    s3_encryption {
      s3_encryption_mode = "SSE-S3"
    }
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "DISABLED" # would require a customer-managed KMS key (cost)
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "DISABLED" # bookmarks are not used
    }
  }
}

resource "aws_glue_job" "this" {
  for_each = local.glue_jobs

  name                   = "${var.name_prefix}-${each.key}"
  description            = "Case 01 ${each.key}"
  role_arn               = each.value.role
  glue_version           = "5.0"
  worker_type            = "G.1X"
  number_of_workers      = each.value.workers
  timeout                = 15 # minutes
  max_retries            = 0  # retries are owned by Airflow
  security_configuration = aws_glue_security_configuration.this.name

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${module.data_bucket.id}/${aws_s3_object.glue_script[each.value.script].key}"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = merge({
    "--additional-python-modules" = join(",", [for line in split("\n", file("${path.module}/../../../glue/requirements.txt")) : trimspace(line) if trimspace(line) != "" && !startswith(trimspace(line), "#")])
    "--job-language"              = "python"
    "--custom-logGroup-prefix"    = "/aws-glue/${var.name_prefix}"
    "--enable-metrics"            = "true"
    "--job-bookmark-option"       = "job-bookmark-disable"
    "--contract_s3_key"           = aws_s3_object.contract["bronze_contract.json"].key
    "--TempDir"                   = "s3://${module.data_bucket.id}/tmp/glue/spark/"
  }, each.value.args)
}
