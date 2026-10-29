{#- Critical gate: if too large a share of the day's position-like records was rejected, the
    products would be materially incomplete, so the run must not publish. -#}
{% test rejection_ratio_within(model, max_ratio) %}
with records as (
    select record_status as status from {{ ref('int_position_checks') }}
    where reference_date = {{ reference_date() }} and record_status <> 'SUPERSEDED'
    union all
    select case when reject_reason is null then 'VALID' else 'REJECTED' end
    from {{ ref('int_compliance_position_checks') }}
    where reference_date = {{ reference_date() }}
),
agg as (
    select
        sum(case when status = 'REJECTED' then 1 else 0 end) as rejected,
        count(*) as total
    from records
)
select rejected, total
from agg
where total = 0 or cast(rejected as double) / total > {{ max_ratio }}
{% endtest %}
