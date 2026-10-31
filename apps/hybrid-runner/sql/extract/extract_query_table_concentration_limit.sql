-- exposure_compliance.concentration_limit: full register
SELECT rule_id, rule_scope, limit_pct, excluded_issuer_type, valid_from, valid_to
FROM concentration_limit
ORDER BY rule_id
