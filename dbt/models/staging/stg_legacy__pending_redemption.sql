-- Redemption requests open on the delivery's reference date (view vw_resgate_pendente, source 3).
with r as ({{ latest_delivery(source('bronze', 'legacy_finance__pending_redemption')) }})
select
    upper(trim(cod_cart))                     as source_portfolio_ref,
    delivery_reference_date                   as reference_date,
    try_cast(id_resgate as bigint)            as redemption_id,
    {{ to_decimal('vl_resgate', 20, 2) }}     as redemption_value,
    meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from r
