-- Valid positions from each portfolio's system of record, latest processing version only.
select
    reference_date, portfolio_id, fund_id, instrument_code, quantity, market_value, currency,
    source_system, source_portfolio_ref, processing_version,
    meta_run_id, meta_source_object, meta_row_number
from {{ ref('int_position_checks') }}
where record_status = 'VALID'
