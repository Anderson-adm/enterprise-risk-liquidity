select
    reference_date, portfolio_id, instrument_code, issuer_code, issuer_type, quantity, close_price,
    exposure_value, source_portfolio_ref, meta_run_id, meta_source_object
from {{ ref('int_compliance_position_checks') }}
where reject_reason is null
