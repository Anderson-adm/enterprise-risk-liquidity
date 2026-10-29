{#- Critical reconciliation: every Gold total must equal the independent sum of valid Silver
    positions plus cash for the same portfolio and date (exact decimal comparison). -#}
{% test reconciles_liquidity_totals(model) %}
with expected as (
    select portfolio_id, sum(v) as expected_total from (
        select portfolio_id, market_value as v from {{ ref('silver_position') }}
        where reference_date = {{ reference_date() }}
        union all
        select portfolio_id, cash_value from {{ ref('silver_cash_balance') }}
        where reference_date = {{ reference_date() }}
          and portfolio_id in (select portfolio_id from {{ ref('silver_position') }}
                               where reference_date = {{ reference_date() }})
    ) x group by portfolio_id
)
select coalesce(g.portfolio_id, e.portfolio_id) as portfolio_id, g.total_value, e.expected_total
from {{ model }} g
full outer join expected e on e.portfolio_id = g.portfolio_id
where g.total_value is null or e.expected_total is null or g.total_value <> e.expected_total
{% endtest %}
