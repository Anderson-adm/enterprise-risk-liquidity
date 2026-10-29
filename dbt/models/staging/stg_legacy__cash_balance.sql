-- Treasury cash balances from the legacy system's view vw_saldo_caixa (source 3).
with c as ({{ latest_delivery(source('bronze', 'legacy_finance__cash_balance')) }})
select
    upper(trim(cod_cart))                     as source_portfolio_ref,
    {{ to_date('dt_ref') }}                   as reference_date,
    {{ to_decimal('vl_saldo', 20, 2) }}       as cash_value,
    upper(trim(cd_moeda))                     as currency,
    meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from c
