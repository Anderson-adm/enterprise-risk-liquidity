-- Record-level quarantine: every record excluded from Silver, with reason and lineage
-- (file-level rejections such as malformed XML are in quarantine/ + ingestion manifests).
select reference_date, 'portfolio_position' as entity, source_system, reject_reason,
       source_portfolio_ref || '/' || coalesce(instrument_code, '?') as record_ref,
       meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from {{ ref('int_position_checks') }} where record_status = 'REJECTED'
union all
select reference_date, 'daily_price', meta_source_system, reject_reason,
       coalesce(instrument_code, '?'),
       meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from {{ ref('int_market_price_checks') }} where reject_reason is not null
union all
select reference_date, 'cash_balance', 'legacy_finance', reject_reason, source_portfolio_ref,
       meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from {{ ref('int_cash_balance_checks') }} where reject_reason is not null
union all
select reference_date, 'pending_redemption', 'legacy_finance', reject_reason,
       source_portfolio_ref || '/' || coalesce(cast(redemption_id as varchar), '?'),
       meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from {{ ref('int_pending_redemption_checks') }} where reject_reason is not null
union all
select reference_date, 'stock_position', 'exposure_compliance', reject_reason,
       source_portfolio_ref || '/' || coalesce(instrument_code, '?'),
       meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from {{ ref('int_compliance_position_checks') }} where reject_reason is not null
