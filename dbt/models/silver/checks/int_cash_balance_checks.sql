{{ config(materialized='view') }}
with c as (
    select
        s.*,
        m.portfolio_id,
        row_number() over (partition by s.source_portfolio_ref, s.reference_date order by s.meta_row_number) as dup_rn
    from {{ ref('stg_legacy__cash_balance') }} s
    left join {{ ref('stg_fund_ops__identity_map') }} m
      on m.source_system = 'legacy_finance'
     and m.external_identifier = s.source_portfolio_ref
     and m.valid_from <= s.reference_date
     and (m.valid_to is null or m.valid_to >= s.reference_date)
)
select
    c.*,
    case
        when reference_date is null or cash_value is null   then 'INVALID_VALUE'
        when portfolio_id is null                           then 'UNMAPPED_PORTFOLIO'
        when dup_rn > 1                                     then 'DUPLICATE_RECORD'
    end as reject_reason
from c
