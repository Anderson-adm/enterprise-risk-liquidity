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

  # Built by scripts/build (dist/manifest.json lists the same keys and SHA-256 values).
  lambda_package     = "${var.artifacts_dir}/lambda/bronze/file_ingestion.zip"
  lambda_package_key = "case01/lambda/bronze/file_ingestion.zip"
  glue_db_prefix     = replace(var.name_prefix, "-", "_")

  # Bronze contract shared with the Python ingestion code (single source of truth).
  bronze_contract = jsondecode(file("${path.module}/../../../src/erl/contracts/bronze_contract.json"))
  bronze_entities = local.bronze_contract.entities

  file_sources = {
    market_data  = { function = "ingest-market-data", handler = "erl.aws.lambda_handlers.market_data_handler" }
    internal_xml = { function = "ingest-internal-xml", handler = "erl.aws.lambda_handlers.internal_xml_handler" }
    external_xml = { function = "ingest-external-xml", handler = "erl.aws.lambda_handlers.external_xml_handler" }
  }
  db_sources = ["fund_ops", "portfolio_risk", "legacy_finance", "exposure_compliance"]
}
