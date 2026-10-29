{{ config(materialized='table') }}
-- Write-audit-publish: the candidate holds only the reference date being processed and is
-- tested (critical tests) before gold_liquidity_summary is touched.
--
-- Liquidity rule (SYNTHETIC, demonstrative only — see seeds/liquidity_rules.csv):
--   FULL   : whole market value is liquid (government bonds, cash)
--   FACTOR : market value x fixed factor
--   VOLUME : equities; sellable in one day = participation x average daily volume
with pos as (
    select * from {{ ref('silver_position') }} where reference_date = {{ reference_date() }}
),
px as (
    select * from {{ ref('silver_market_price') }} where reference_date = {{ reference_date() }}
),
valued as (
    select
        pos.portfolio_id, pos.fund_id, pos.source_system, pos.market_value,
        pos.meta_run_id, px.meta_run_id as px_run_id,
        case r.liquidity_method
            when 'FULL'   then pos.market_value
            when 'FACTOR' then cast(pos.market_value * r.liquidity_factor as decimal(20, 2))
            when 'VOLUME' then cast(pos.market_value * least(
                    cast(1 as decimal(38, 10)),
                    cast({{ var('equity_volume_participation') }} * px.avg_daily_volume / pos.quantity
                         as decimal(38, 10))) as decimal(20, 2))
            else cast(0 as decimal(20, 2))  -- unknown instrument type: conservatively illiquid
        end as liquid_value
    from pos
    join px on px.instrument_code = pos.instrument_code
    left join {{ ref('liquidity_rules') }} r on r.instrument_type = px.instrument_type
),
by_portfolio as (
    select
        portfolio_id, fund_id, source_system as position_source_system,
        sum(market_value) as positions_value,
        sum(liquid_value) as liquid_positions_value,
        count(*) as positions_count
    from valued
    group by portfolio_id, fund_id, source_system
),
cash as (
    select portfolio_id, sum(cash_value) as cash_value, max(meta_run_id) as meta_run_id
    from {{ ref('silver_cash_balance') }}
    where reference_date = {{ reference_date() }}
    group by portfolio_id
),
redemptions as (
    select portfolio_id, sum(redemption_value) as pending_redemptions, max(meta_run_id) as meta_run_id
    from {{ ref('silver_pending_redemption') }}
    where reference_date = {{ reference_date() }}
    group by portfolio_id
),
lineage as (
    select portfolio_id, source_system, meta_run_id from valued
    union all select portfolio_id, 'market_data', px_run_id from valued
    union all select portfolio_id, 'legacy_finance', meta_run_id from cash
    union all select portfolio_id, 'legacy_finance', meta_run_id from redemptions
    union all
    select b.portfolio_id, 'fund_ops', p.meta_run_id
    from by_portfolio b join {{ ref('silver_portfolio') }} p on p.portfolio_id = b.portfolio_id
),
lineage_agg as (
    select portfolio_id,
           {{ list_agg('source_system') }} as lineage_sources,
           {{ list_agg('meta_run_id') }}   as lineage_bronze_runs
    from lineage
    group by portfolio_id
),
totals as (
    select
        b.*,
        coalesce(c.cash_value, 0)                                   as cash_value,
        b.positions_value + coalesce(c.cash_value, 0)               as total_value,
        b.liquid_positions_value + coalesce(c.cash_value, 0)        as liquid_value,
        coalesce(r.pending_redemptions, 0)                          as pending_redemptions
    from by_portfolio b
    left join cash c on c.portfolio_id = b.portfolio_id
    left join redemptions r on r.portfolio_id = b.portfolio_id
)
select
    {{ reference_date() }}                                          as reference_date,
    t.portfolio_id,
    t.fund_id,
    t.position_source_system,
    cast(t.positions_value as decimal(20, 2))                       as positions_value,
    cast(t.cash_value as decimal(20, 2))                            as cash_value,
    cast(t.total_value as decimal(20, 2))                           as total_value,
    cast(t.liquid_value as decimal(20, 2))                          as liquid_value,
    cast(case when t.total_value = 0 then 0
              else t.liquid_value / t.total_value end as decimal(9, 6)) as liquidity_ratio,
    cast(t.pending_redemptions as decimal(20, 2))                   as pending_redemptions,
    cast(case when t.pending_redemptions > 0
              then t.liquid_value / t.pending_redemptions end as decimal(18, 6)) as redemption_coverage,
    cast(t.positions_count as integer)                              as positions_count,
    l.lineage_sources,
    l.lineage_bronze_runs,
    '{{ var("pipeline_run_id") }}'                                  as pipeline_run_id
from totals t
join lineage_agg l on l.portfolio_id = t.portfolio_id
