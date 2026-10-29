provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.allowed_account_id] # refuse any account but data-dev

  default_tags {
    tags = local.tags
  }
}
