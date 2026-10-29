mock_provider "aws" {
  override_during = plan

  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::erl-dev-test-bucket", id = "erl-dev-test-bucket" }
  }
}

variables {
  bucket_name = "erl-dev-test-bucket"
}

run "secure_defaults" {
  command = plan

  assert {
    condition = alltrue([
      aws_s3_bucket_public_access_block.this.block_public_acls,
      aws_s3_bucket_public_access_block.this.block_public_policy,
      aws_s3_bucket_public_access_block.this.ignore_public_acls,
      aws_s3_bucket_public_access_block.this.restrict_public_buckets,
    ])
    error_message = "All four public access block settings must be enabled."
  }

  assert {
    condition     = one(one(aws_s3_bucket_server_side_encryption_configuration.this.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256"
    error_message = "Default encryption must be enabled."
  }

  assert {
    condition     = one(aws_s3_bucket_versioning.this.versioning_configuration).status == "Enabled"
    error_message = "Versioning must be enabled."
  }

  assert {
    condition     = aws_s3_bucket.this.force_destroy == false
    error_message = "force_destroy must default to false."
  }

  assert {
    condition     = one(aws_s3_bucket_ownership_controls.this.rule).object_ownership == "BucketOwnerEnforced"
    error_message = "ACLs must be disabled."
  }

  assert {
    condition     = strcontains(aws_s3_bucket_policy.this.policy, "aws:SecureTransport")
    error_message = "Bucket policy must deny non-TLS access."
  }
}

run "prefix_retention_rules" {
  command = plan

  variables {
    prefix_expiration_days = { "tmp/" = 1, "landing/" = 30 }
  }

  assert {
    condition     = length(aws_s3_bucket_lifecycle_configuration.this.rule) == 3
    error_message = "Expected the noncurrent-version rule plus one rule per prefix."
  }
}

run "invalid_bucket_name_is_rejected" {
  command = plan

  variables {
    bucket_name = "Invalid_Bucket"
  }

  expect_failures = [var.bucket_name]
}
