# Remote state bucket for the Case 01 environments. Created once, kept when the lab is
# destroyed (prevent_destroy). Locking uses S3 native lock files (use_lockfile = true),
# so no DynamoDB table is needed.

provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.allowed_account_id]

  default_tags {
    tags = {
      Project     = "enterprise-risk-liquidity"
      Environment = "shared"
      Purpose     = "terraform-state"
      ManagedBy   = "terraform"
    }
  }
}

resource "aws_s3_bucket" "state" {
  #checkov:skip=CKV_AWS_145:SSE-S3 default encryption; a KMS CMK adds fixed monthly cost to a temporary lab (docs/TERRAFORM.md)
  #checkov:skip=CKV_AWS_144:Single-region temporary lab; replication would double storage for state files
  #checkov:skip=CKV_AWS_18:Server access logging not enabled for the lab (no extra log bucket); CloudTrail management events cover API activity
  #checkov:skip=CKV2_AWS_62:No event-driven consumers: ingestion Lambdas are invoked synchronously by the pipeline
  bucket = "${var.name_prefix}-tfstate-${var.allowed_account_id}"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = aws_s3_bucket.state.id

  rule {
    id     = "keep-90-days-of-state-history"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  depends_on = [aws_s3_bucket_versioning.state]
}

resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.state]
}
