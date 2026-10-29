{{ config(materialized='view') }}
-- Positions from all systems of record with identity resolution and validation outcome.
-- record_status: VALID | REJECTED | SUPERSEDED (an older processing_version of a corrected position).
with u as (
    select * from {{ ref('stg_positions__unioned') }}
),
mapped as (
    select u.*, m.portfolio_id
    from u
    left join {{ ref('stg_fund_ops__identity_map') }} m
      on m.source_system = u.source_system
     and m.external_identifier = u.source_portfolio_ref
     and m.valid_from <= u.reference_date
     and (m.valid_to is null or m.valid_to >= u.reference_date)
),
w as (
    select
        mapped.*,
        max(processing_version) over (
            partition by source_system, source_portfolio_ref, instrument_code, reference_date) as max_version,
        row_number() over (
            partition by source_system, source_portfolio_ref, instrument_code, reference_date, processing_version
            order by source_record_id, meta_row_number) as dup_rn,
        min(quantity) over (
            partition by source_system, source_portfolio_ref, instrument_code, reference_date, processing_version) as min_qty,
        max(quantity) over (
            partition by source_system, source_portfolio_ref, instrument_code, reference_date, processing_version) as max_qty,
        min(market_value) over (
            partition by source_system, source_portfolio_ref, instrument_code, reference_date, processing_version) as min_mv,
        max(market_value) over (
            partition by source_system, source_portfolio_ref, instrument_code, reference_date, processing_version) as max_mv
    from mapped
),
checked as (
    select
        w.*,
        prt.fund_id,
        case
            when w.reference_date is null or w.instrument_code is null or w.quantity is null
              or w.market_value is null or w.quantity <= 0 or w.market_value < 0
              or w.processing_version is null                                  then 'INVALID_VALUE'
            when w.portfolio_id is null or prt.portfolio_id is null            then 'UNMAPPED_PORTFOLIO'
            when prt.position_source_system <> w.source_system                 then 'NOT_SOURCE_OF_RECORD'
            when w.processing_version < w.max_version                          then null
            when w.min_qty <> w.max_qty or w.min_mv <> w.max_mv                then 'CONFLICTING_RECORDS'
            when w.dup_rn > 1                                                  then 'DUPLICATE_RECORD'
            when px.instrument_code is null                                    then 'MISSING_MARKET_DATA'
        end as reject_reason
    from w
    left join {{ ref('stg_fund_ops__portfolio') }} prt on prt.portfolio_id = w.portfolio_id
    left join {{ ref('silver_market_price') }} px
      on px.instrument_code = w.instrument_code and px.reference_date = w.reference_date
)
select
    checked.*,
    case
        when reject_reason is not null              then 'REJECTED'
        when processing_version < max_version       then 'SUPERSEDED'
        else 'VALID'
    end as record_status
from checked
