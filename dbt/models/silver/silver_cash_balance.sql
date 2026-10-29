select reference_date, portfolio_id, cash_value, currency, meta_run_id, meta_source_object
from {{ ref('int_cash_balance_checks') }}
where reject_reason is null
