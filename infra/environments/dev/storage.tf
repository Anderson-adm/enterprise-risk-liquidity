# Data lake bucket: landing/, bronze/, manifests/, quarantine/, lakehouse/ (Iceberg, owned
# by dbt), publish/, scripts/ and tmp/. Bronze has no expiry: teardown empties it explicitly.
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
