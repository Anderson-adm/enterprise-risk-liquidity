output "data_bucket" {
  description = "ERL_S3_BUCKET"
  value       = module.data_bucket.id
}

output "athena_results_s3" {
  description = "ERL_ATHENA_RESULTS_S3"
  value       = "s3://${module.athena_results_bucket.id}/athena/"
}

output "lakehouse_s3" {
  description = "ERL_LAKEHOUSE_S3 (dbt s3_data_dir; Iceberg tables owned by dbt)"
  value       = "s3://${module.data_bucket.id}/lakehouse/"
}

output "athena_workgroup" {
  description = "ERL_ATHENA_WORKGROUP"
  value       = aws_athena_workgroup.this.name
}

output "glue_databases" {
  description = "Glue Catalog databases per layer"
  value       = { for k, db in aws_glue_catalog_database.layer : k => db.name }
}

output "runner_role_arn" {
  description = "Role the local pipeline assumes (role_arn in the AWS profile)"
  value       = aws_iam_role.runner.arn
}

output "alerts_topic_arn" {
  description = "ERL_ALERTS_TOPIC_ARN"
  value       = aws_sns_topic.alerts.arn
}

output "lambda_functions" {
  description = "Ingestion Lambda names"
  value       = { for k, f in aws_lambda_function.ingest : k => f.function_name }
}

output "glue_jobs" {
  description = "Glue job names"
  value       = { for k, j in aws_glue_job.this : k => j.name }
}
