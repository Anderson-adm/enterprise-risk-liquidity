-- Synthetic, configurable concentration limits (source 4). Demonstrative only.
with l as ({{ latest_delivery(source('bronze', 'exposure_compliance__concentration_limit'), scope='latest') }})
select
    rule_id,
    upper(trim(rule_scope))                   as rule_scope,
    {{ to_decimal('limit_pct', 9, 6) }}       as limit_pct,
    upper(trim(excluded_issuer_type))         as excluded_issuer_type,
    {{ to_date('valid_from') }}               as valid_from,
    {{ to_date('valid_to') }}                 as valid_to
from l
