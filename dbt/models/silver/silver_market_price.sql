-- Validated daily market prices: one row per (reference_date, instrument_code).
select
    reference_date, instrument_code, instrument_type, close_price, avg_daily_volume, currency,
    meta_run_id, meta_source_object
from {{ ref('int_market_price_checks') }}
where reject_reason is null
