-- Consolidated portfolio register (operational system, source 1).
select portfolio_id, fund_id, fund_name, fund_type, position_source_system, status, meta_run_id
from {{ ref('stg_fund_ops__portfolio') }}
