# Ownership: Terraform owns the Glue databases and the Bronze tables (external, Parquet,
# partition projection, generated from the Bronze contract). dbt owns every Silver/Gold
# table and view it materialises inside the silver/gold databases.

# Commit markers make partial/uncommitted Bronze writes invisible to dbt. Ion accepts
# the existing pretty-printed JSON manifests as well as single-line JSON.
resource "aws_glue_catalog_table" "ingestion_deliveries" {
  name          = "ingestion_deliveries"
  database_name = aws_glue_catalog_database.layer["bronze"].name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { "classification" = "json", "EXTERNAL" = "TRUE" }

  storage_descriptor {
    location      = "s3://${module.data_bucket.id}/manifests/ingestion/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "com.amazon.ionhiveserde.IonHiveSerDe"
    }
    dynamic "columns" {
      for_each = ["source_system", "entity", "run_id", "input_object", "status"]
      content {
        name = columns.value
        type = "string"
      }
    }
  }
}

resource "aws_glue_catalog_database" "layer" {
  for_each = toset(["bronze", "silver", "gold"])

  name        = "${local.glue_db_prefix}_${each.key}"
  description = "Case 01 ${each.key} layer"
}

resource "aws_glue_catalog_table" "bronze" {
  for_each = local.bronze_entities

  name          = each.key
  database_name = aws_glue_catalog_database.layer["bronze"].name
  table_type    = "EXTERNAL_TABLE"
  description   = "Bronze ${each.value.source_system}.${each.value.entity} (append-only, values as received)"

  parameters = {
    "classification"                                 = "parquet"
    "EXTERNAL"                                       = "TRUE"
    "projection.enabled"                             = "true"
    "projection.bronze_reference_date.type"          = "date"
    "projection.bronze_reference_date.format"        = "yyyy-MM-dd"
    "projection.bronze_reference_date.range"         = "2026-01-01,NOW"
    "projection.bronze_reference_date.interval"      = "1"
    "projection.bronze_reference_date.interval.unit" = "DAYS"
    "storage.location.template"                      = "s3://${module.data_bucket.id}/bronze/structured/${each.value.source_system}/${each.value.entity}/bronze_reference_date=$${bronze_reference_date}/"
  }

  partition_keys {
    name = "bronze_reference_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${module.data_bucket.id}/bronze/structured/${each.value.source_system}/${each.value.entity}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = concat(
        [for c in each.value.columns : { name = c, type = "string" }],
        local.bronze_contract.metadata_columns,
      )
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
