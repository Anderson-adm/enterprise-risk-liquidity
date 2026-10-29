terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.71"
    }
  }

  # Bootstrap state is local by design (chicken-and-egg). It holds no secrets; keep the
  # terraform.tfstate file out of git (see .gitignore) and back it up after apply.
}
