{{ config(materialized='view') }}
-- One row per delivered market data record with its validation outcome.
with p as (
    select
        s.*,
        row_number() over (partition by reference_date, instrument_code order by meta_row_number) as dup_rn,
        min(close_price) over (partition by reference_date, instrument_code)       as min_price,
        max(close_price) over (partition by reference_date, instrument_code)       as max_price,
        min(avg_daily_volume) over (partition by reference_date, instrument_code)  as min_adv,
        max(avg_daily_volume) over (partition by reference_date, instrument_code)  as max_adv
    from {{ ref('stg_market__daily_price') }} s
)
select
    p.*,
    case
        when reference_date is null or instrument_code is null
          or close_price is null or close_price <= 0
          or avg_daily_volume is null or avg_daily_volume < 0          then 'INVALID_VALUE'
        when min_price <> max_price or min_adv <> max_adv              then 'CONFLICTING_RECORDS'
        when dup_rn > 1                                                then 'DUPLICATE_RECORD'
    end as reject_reason
from p
