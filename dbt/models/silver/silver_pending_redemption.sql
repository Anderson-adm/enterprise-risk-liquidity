select reference_date, portfolio_id, redemption_id, redemption_value, meta_run_id, meta_source_object
from {{ ref('int_pending_redemption_checks') }}
where reject_reason is null
