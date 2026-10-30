-- fund_ops.source_identity_map: identifier of each portfolio in every source system
SELECT source_system, external_identifier, portfolio_id, valid_from, valid_to
FROM source_identity_map
ORDER BY source_system, external_identifier
