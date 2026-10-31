data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  tags = {
    Project     = "enterprise-risk-liquidity"
    Environment = var.environment
    Purpose     = "case01-validation-lab"
    ManagedBy   = "terraform"
    Repository  = "github.com/Anderson-adm/enterprise-risk-liquidity"
  }

  data_bucket      = "${var.name_prefix}-data-${local.account_id}"
  results_bucket   = "${var.name_prefix}-athena-results-${local.account_id}"
  artifacts_bucket = "${var.name_prefix}-artifacts-${local.account_id}"

  # Independent artifacts built before planning; no code generation in Terraform.
  glue_units = { for name in ["db-gb", "db-tb", "gold-load"] : name => jsondecode(file("${var.artifacts_dir}/glue/${name}/manifest.json")) }
  glue_artifacts = merge([for unit, manifest in local.glue_units : {
    for filename, artifact in manifest.artifacts : "${unit}/${filename}" => {
      path   = "glue/${unit}/${filename}"
      key    = artifact.s3_key
      sha256 = artifact.sha256
    }
  }]...)
  glue_db_prefix = replace(var.name_prefix, "-", "_")

  # Bronze contract shared with the Python ingestion code (single source of truth).
  bronze_contract = jsondecode(file("${path.module}/../../../contracts/bronze_contract.json"))
  bronze_entities = local.bronze_contract.entities

  file_sources = {
    market_data  = { unit = "market-data", function = "ingest-market-data", handler = "handler.handler" }
    internal_xml = { unit = "internal-xml", function = "ingest-internal-xml", handler = "handler.handler" }
    external_xml = { unit = "external-xml", function = "ingest-external-xml", handler = "handler.handler" }
  }
  db_sources = ["fund_ops", "portfolio_risk", "legacy_finance", "exposure_compliance"]
}
