-- Daily market data (source 5). Decimal comma normalised; duplicates kept here and
-- resolved (and reported) in silver.
with d as ({{ latest_delivery(source('bronze', 'market_data__daily_price')) }})
select
    {{ to_date('reference_date') }}                    as reference_date,
    upper(trim(instrument_code))                       as instrument_code,
    upper(trim(instrument_type))                       as instrument_type,
    {{ to_decimal('close_price', 20, 6) }}             as close_price,
    {{ to_decimal('avg_daily_volume', 24, 6) }}        as avg_daily_volume,
    upper(trim(currency))                              as currency,
    meta_source_system, meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from d
