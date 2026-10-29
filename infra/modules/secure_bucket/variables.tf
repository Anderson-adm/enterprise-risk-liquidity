variable "bucket_name" {
  description = "Globally unique bucket name."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be a valid S3 bucket name."
  }
}

variable "noncurrent_version_retention_days" {
  description = "Days to keep overwritten/deleted object versions before expiry."
  type        = number
  default     = 7

  validation {
    condition     = var.noncurrent_version_retention_days >= 1
    error_message = "noncurrent_version_retention_days must be >= 1."
  }
}

variable "prefix_expiration_days" {
  description = "Explicit retention per key prefix (prefix => days). Prefixes not listed are kept until teardown."
  type        = map(number)
  default     = {}

  validation {
    condition     = alltrue([for d in values(var.prefix_expiration_days) : d >= 1])
    error_message = "Expiration days must be >= 1."
  }
}

variable "force_destroy" {
  description = "Allow Terraform to delete a non-empty bucket. Keep false; use scripts/teardown_aws.sh to empty buckets explicitly."
  type        = bool
  default     = false
}
