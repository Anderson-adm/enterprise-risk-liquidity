-- Stock (inventory) positions used for exposure checks (source 4), keyed by the compliance
-- system's own fund reference.
with s as ({{ latest_delivery(source('bronze', 'exposure_compliance__stock_position')) }})
select
    upper(trim(fund_ref))                     as source_portfolio_ref,
    upper(trim(instrument_code))              as instrument_code,
    {{ to_date('position_date') }}            as reference_date,
    {{ to_decimal('quantity', 24, 6) }}       as quantity,
    try_cast(stock_id as bigint)              as source_record_id,
    meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from s
