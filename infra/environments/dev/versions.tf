terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.71"
    }
  }

  # Partial configuration: bucket/key/region come from backend.hcl (not versioned).
  #   terraform init -backend-config=backend.hcl
  # CI and tests use `terraform init -backend=false`.
  backend "s3" {
    use_lockfile = true
    encrypt      = true
  }
}
