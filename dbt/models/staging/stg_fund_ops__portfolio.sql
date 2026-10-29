-- Portfolio register (source 1) joined to its fund; latest delivered snapshot of the register.
with p as ({{ latest_delivery(source('bronze', 'fund_ops__portfolio'), scope='latest') }}),
     f as ({{ latest_delivery(source('bronze', 'fund_ops__fund'), scope='latest') }})
select
    upper(trim(p.portfolio_id))           as portfolio_id,
    upper(trim(p.fund_id))                as fund_id,
    f.fund_name,
    f.fund_type,
    trim(p.position_source_system)        as position_source_system,
    upper(trim(p.status))                 as status,
    p.meta_run_id
from p
left join f on upper(trim(f.fund_id)) = upper(trim(p.fund_id))
