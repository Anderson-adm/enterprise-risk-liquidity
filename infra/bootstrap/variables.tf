variable "aws_profile" {
  description = "Explicit SSO profile for bootstrap; never default."
  type        = string
  default     = "data-dev"
  validation {
    condition     = trimspace(var.aws_profile) != "" && var.aws_profile != "default"
    error_message = "Select an explicit non-default SSO profile."
  }
}

variable "aws_region" {
  description = "Region for the Terraform state bucket."
  type        = string
  default     = "sa-east-1"
  validation {
    condition     = var.aws_region == "sa-east-1"
    error_message = "The lab is restricted to sa-east-1."
  }
}

variable "allowed_account_id" {
  description = "The data-dev AWS account id. Terraform refuses to run against any other account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.allowed_account_id))
    error_message = "allowed_account_id must be a 12-digit AWS account id."
  }
}

variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "erl"
}
