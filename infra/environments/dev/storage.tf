# Data lake bucket: landing/, bronze/, manifests/, quarantine/, lakehouse/ (Iceberg, owned
# by dbt), publish/ and tmp/. Code lives in the artifacts bucket below. Bronze has no expiry:
# teardown empties it explicitly.
module "data_bucket" {
  source = "../../modules/secure_bucket"

  bucket_name = local.data_bucket
  prefix_expiration_days = {
    "tmp/"     = 1
    "landing/" = var.landing_retention_days
  }
}

# Athena query results: separate bucket because retention (7 days) and access differ.
module "athena_results_bucket" {
  source = "../../modules/secure_bucket"

  bucket_name                       = local.results_bucket
  noncurrent_version_retention_days = 1
  prefix_expiration_days            = { "athena/" = 7 }
}

# Deployment artifacts (Lambda ZIP; Glue scripts and library) built and smoke-tested by
# scripts/build/ BEFORE any plan. Separate from the data lake, so runtime roles can never
# overwrite code, and from the Terraform state bucket (infra/bootstrap). Stable keys under
# case01/; the bucket is versioned and non-current versions expire after a recovery window.
module "artifacts_bucket" {
  source = "../../modules/secure_bucket"

  bucket_name                       = local.artifacts_bucket
  noncurrent_version_retention_days = var.artifact_noncurrent_retention_days
}
