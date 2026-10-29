-- Identifier crosswalk maintained by the operational system (source 1).
-- Lookups normalise both sides with upper(trim()).
with m as ({{ latest_delivery(source('bronze', 'fund_ops__source_identity_map'), scope='latest') }})
select
    trim(source_system)                   as source_system,
    upper(trim(external_identifier))      as external_identifier,
    upper(trim(portfolio_id))             as portfolio_id,
    {{ to_date('valid_from') }}           as valid_from,
    {{ to_date('valid_to') }}             as valid_to
from m
