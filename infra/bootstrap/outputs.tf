output "state_bucket" {
  description = "Use as `bucket` in infra/environments/dev/backend.hcl."
  value       = aws_s3_bucket.state.id
}
