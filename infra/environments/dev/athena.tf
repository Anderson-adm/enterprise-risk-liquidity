resource "aws_athena_workgroup" "this" {
  name        = var.name_prefix
  description = "Case 01 dbt + ad-hoc queries (engine v3, per-query scan limit)"
  state       = "ENABLED"

  # Only deletes saved queries / history of this workgroup, never data.
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3" # required for Iceberg MERGE
    }

    result_configuration {
      output_location = "s3://${module.athena_results_bucket.id}/athena/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
