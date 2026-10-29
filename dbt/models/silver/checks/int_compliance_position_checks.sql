{{ config(materialized='view') }}
-- Compliance stock positions valued with the day's market price.
with s as (
    select
        st.*,
        m.portfolio_id,
        row_number() over (partition by st.source_record_id order by st.meta_row_number) as dup_rn
    from {{ ref('stg_compliance__stock_position') }} st
    left join {{ ref('stg_fund_ops__identity_map') }} m
      on m.source_system = 'exposure_compliance'
     and m.external_identifier = st.source_portfolio_ref
     and m.valid_from <= st.reference_date
     and (m.valid_to is null or m.valid_to >= st.reference_date)
)
select
    s.*,
    i.issuer_code,
    i.issuer_type,
    px.close_price,
    cast(round(s.quantity * px.close_price, 2) as decimal(20, 2)) as exposure_value,
    case
        when s.reference_date is null or s.quantity is null or s.quantity < 0 then 'INVALID_VALUE'
        when s.portfolio_id is null                                           then 'UNMAPPED_PORTFOLIO'
        when s.dup_rn > 1                                                     then 'DUPLICATE_RECORD'
        when i.instrument_code is null                                        then 'UNKNOWN_INSTRUMENT'
        when px.instrument_code is null                                       then 'MISSING_MARKET_DATA'
    end as reject_reason
from s
left join {{ ref('stg_compliance__instrument') }} i on i.instrument_code = s.instrument_code
left join {{ ref('silver_market_price') }} px
  on px.instrument_code = s.instrument_code and px.reference_date = s.reference_date
