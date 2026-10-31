variable "aws_profile" {
  description = "Explicit SSO profile for Terraform; never the default profile."
  type        = string
  default     = "data-dev"
  validation {
    condition     = trimspace(var.aws_profile) != "" && var.aws_profile != "default"
    error_message = "Select an explicit non-default SSO profile."
  }
}

variable "aws_region" {
  description = "AWS region of the lab."
  type        = string
  default     = "sa-east-1"

  validation {
    condition     = var.aws_region == "sa-east-1"
    error_message = "The lab is restricted to sa-east-1."
  }
}

variable "allowed_account_id" {
  description = "AWS account id of data-dev. The provider refuses to operate on any other account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.allowed_account_id))
    error_message = "allowed_account_id must be a 12-digit AWS account id."
  }
}

variable "environment" {
  description = "Environment name. Only dev is provisioned in this phase."
  type        = string
  default     = "dev"

  validation {
    condition     = var.environment == "dev"
    error_message = "Only the dev environment may be provisioned (no data-prod in this phase)."
  }
}

variable "name_prefix" {
  description = "Prefix of every resource name (also used by the pipeline: ERL_NAME_PREFIX)."
  type        = string
  default     = "erl-dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.name_prefix))
    error_message = "name_prefix must be 3-21 lowercase letters, digits or dashes."
  }
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit enforced by the Athena workgroup (bytes). Minimum allowed by Athena is 10 MB."
  type        = number
  default     = 104857600 # 100 MB; the lab scans kilobytes

  validation {
    condition     = var.athena_bytes_scanned_cutoff >= 10485760 && var.athena_bytes_scanned_cutoff <= 1073741824
    error_message = "athena_bytes_scanned_cutoff must be between 10 MB and 1 GB for this lab."
  }
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for Lambda and Glue log groups."
  type        = number
  default     = 14

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30], var.log_retention_days)
    error_message = "log_retention_days must be one of 1, 3, 5, 7, 14, 30 for the lab."
  }
}

variable "landing_retention_days" {
  description = "Retention of landing objects (raw deliveries are also copied to Bronze)."
  type        = number
  default     = 30
}

variable "artifacts_dir" {
  description = "dist/ directory produced by scripts/build/build_artifacts.py (relative to this module). Terraform only uploads these files; it never builds them."
  type        = string
  default     = "../../../dist"
}

variable "artifact_noncurrent_retention_days" {
  description = "Days a replaced artifact version stays recoverable in the artifacts bucket."
  type        = number
  default     = 30

  validation {
    condition     = var.artifact_noncurrent_retention_days >= 7 && var.artifact_noncurrent_retention_days <= 90
    error_message = "Keep replaced artifacts between 7 and 90 days."
  }
}

variable "lambda_reserved_concurrency" {
  description = "Reserved concurrency per ingestion Lambda. null = unreserved (new accounts often have a 10-execution account limit that cannot be reserved from)."
  type        = number
  default     = null
}

variable "glue_workers" {
  description = "Workers per named Glue execution unit (G.1X). 2 is the minimum for Spark jobs."
  type        = map(number)
  default     = { extract-liquidez = 2, extract-enquadramento = 2, export-risk-analytics = 2 }

  validation {
    condition     = alltrue([for n in values(var.glue_workers) : n >= 2 && n <= 4])
    error_message = "Glue workers must be between 2 and 4 in the lab (cost control)."
  }
}

variable "glue_max_product_rows" {
  description = "Gold Load refuses to write a consumer product larger than this (one Parquet object per product; products hold one row per portfolio and date)."
  type        = number
  default     = 100000

  validation {
    condition     = var.glue_max_product_rows >= 1 && var.glue_max_product_rows <= 10000000
    error_message = "glue_max_product_rows must be between 1 and 10 000 000."
  }
}

variable "runner_trusted_principal_arns" {
  description = "IAM principal ARN patterns allowed to assume the pipeline runner role (default: the account's SSO roles)."
  type        = list(string)
  default     = []
}

variable "alert_email" {
  description = "Optional e-mail subscribed to the alert topic and budget notifications (confirmation required)."
  type        = string
  default     = null
}

variable "monthly_budget_usd" {
  description = "Cost budget for the account (notification only; it does not stop spending)."
  type        = number
  default     = 5
}
