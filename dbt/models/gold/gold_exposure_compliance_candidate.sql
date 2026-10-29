{{ config(materialized='table') }}
-- Exposure & concentration checks for the reference date (write-audit-publish candidate).
-- The issuer concentration limit is SYNTHETIC and configurable (source 4,
-- concentration_limit); it is not a CVM/ANBIMA or any other regulatory rule.
with cp as (
    select * from {{ ref('silver_compliance_position') }} where reference_date = {{ reference_date() }}
),
lim as (
    select limit_pct, excluded_issuer_type
    from {{ ref('silver_concentration_limit') }}
    where rule_scope = 'ISSUER'
      and valid_from <= {{ reference_date() }}
      and (valid_to is null or valid_to >= {{ reference_date() }})
),
totals as (
    select portfolio_id, sum(exposure_value) as total_exposure from cp group by portfolio_id
),
issuers as (
    select cp.portfolio_id, cp.issuer_code, sum(cp.exposure_value) as issuer_exposure
    from cp cross join lim
    where cp.issuer_type <> coalesce(lim.excluded_issuer_type, '-')
    group by cp.portfolio_id, cp.issuer_code
),
top_issuer as (
    select * from (
        select issuers.*,
               row_number() over (partition by portfolio_id order by issuer_exposure desc, issuer_code) as rn
        from issuers
    ) ranked where rn = 1
),
-- reconciliation: compliance stock vs. positions from the system of record (Silver)
recon as (
    select
        coalesce(c.portfolio_id, p.portfolio_id) as portfolio_id,
        c.qty as compliance_qty,
        p.qty as position_qty
    from (select portfolio_id, instrument_code, sum(quantity) as qty from cp
          group by portfolio_id, instrument_code) c
    full outer join (
          select portfolio_id, instrument_code, sum(quantity) as qty
          from {{ ref('silver_position') }} where reference_date = {{ reference_date() }}
          group by portfolio_id, instrument_code) p
      on p.portfolio_id = c.portfolio_id and p.instrument_code = c.instrument_code
),
recon_agg as (
    select
        portfolio_id,
        sum(case when compliance_qty is null or position_qty is null or compliance_qty <> position_qty
                 then 1 else 0 end) as mismatched_positions,
        count(position_qty) as positions_available
    from recon group by portfolio_id
),
rejections as (
    select portfolio_id, count(*) as rejected_records from (
        select portfolio_id from {{ ref('int_position_checks') }}
        where record_status = 'REJECTED' and reference_date = {{ reference_date() }}
        union all
        select portfolio_id from {{ ref('int_compliance_position_checks') }}
        where reject_reason is not null and reference_date = {{ reference_date() }}
    ) r where portfolio_id is not null group by portfolio_id
),
lineage as (
    select portfolio_id, 'exposure_compliance' as source_system, meta_run_id from cp
    union all
    select cp.portfolio_id, 'market_data', px.meta_run_id
    from cp join {{ ref('silver_market_price') }} px
      on px.instrument_code = cp.instrument_code and px.reference_date = cp.reference_date
    union all
    select t.portfolio_id, 'fund_ops', p.meta_run_id
    from totals t join {{ ref('silver_portfolio') }} p on p.portfolio_id = t.portfolio_id
),
lineage_agg as (
    select portfolio_id,
           {{ list_agg('source_system') }} as lineage_sources,
           {{ list_agg('meta_run_id') }}   as lineage_bronze_runs
    from lineage group by portfolio_id
)
select
    {{ reference_date() }}                                           as reference_date,
    t.portfolio_id,
    p.fund_id,
    cast(t.total_exposure as decimal(20, 2))                         as total_exposure,
    ti.issuer_code                                                   as top_issuer_code,
    cast(coalesce(ti.issuer_exposure, 0) as decimal(20, 2))          as top_issuer_exposure,
    cast(case when t.total_exposure = 0 or ti.issuer_exposure is null then 0
              else {{ ratio('ti.issuer_exposure', 't.total_exposure') }} end as decimal(9, 6)) as top_issuer_pct,
    cast((select max(limit_pct) from lim) as decimal(9, 6))          as concentration_limit_pct,
    case when ti.issuer_exposure is not null and t.total_exposure > 0
              and {{ ratio('ti.issuer_exposure', 't.total_exposure') }} > (select max(limit_pct) from lim)
         then 'BREACH' else 'PASS' end                               as concentration_check,
    case when coalesce(ra.positions_available, 0) = 0 then 'NOT_AVAILABLE'
         when ra.mismatched_positions > 0             then 'MISMATCH'
         else 'MATCHED' end                                          as reconciliation_status,
    cast(coalesce(ra.mismatched_positions, 0) as integer)            as mismatched_positions,
    case when coalesce(ra.positions_available, 0) = 0
           or ra.mismatched_positions > 0
           or coalesce(rj.rejected_records, 0) > 0 then 'WARN' else 'OK' end as quality_status,
    l.lineage_sources,
    l.lineage_bronze_runs,
    '{{ var("pipeline_run_id") }}'                                   as pipeline_run_id
from totals t
join {{ ref('silver_portfolio') }} p on p.portfolio_id = t.portfolio_id
join lineage_agg l on l.portfolio_id = t.portfolio_id
left join top_issuer ti on ti.portfolio_id = t.portfolio_id
left join recon_agg ra on ra.portfolio_id = t.portfolio_id
left join rejections rj on rj.portfolio_id = t.portfolio_id
