select rule_id, rule_scope, limit_pct, excluded_issuer_type, valid_from, valid_to
from {{ ref('stg_compliance__concentration_limit') }}
