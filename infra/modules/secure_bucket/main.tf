# A private, encrypted, versioned bucket that only accepts TLS requests.
# Used for the lab data lake and the Athena query results bucket.

resource "aws_s3_bucket" "this" {
  #checkov:skip=CKV_AWS_145:SSE-S3 default encryption; a KMS CMK adds fixed monthly cost to a temporary lab (docs/TERRAFORM.md)
  #checkov:skip=CKV_AWS_144:Single-region temporary lab; replication would double storage for synthetic data
  #checkov:skip=CKV_AWS_18:Server access logging not enabled for the lab (no extra log bucket); CloudTrail management events cover API activity
  #checkov:skip=CKV2_AWS_62:No event-driven consumers: ingestion Lambdas are invoked synchronously by the pipeline
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    object_ownership = "BucketOwnerEnforced" # disables ACLs
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      # SSE-S3: no per-request KMS cost for a lab; see docs/TERRAFORM.md (CKV_AWS_145).
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    id     = "noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  dynamic "rule" {
    for_each = var.prefix_expiration_days

    content {
      id     = "expire-${trimsuffix(replace(rule.key, "/", "-"), "-")}"
      status = "Enabled"

      filter {
        prefix = rule.key
      }

      expiration {
        days = rule.value
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.this.arn, "${aws_s3_bucket.this.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.this]
}
