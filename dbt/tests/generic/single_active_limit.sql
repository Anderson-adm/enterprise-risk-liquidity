{% test single_active_limit(model) %}
select count(*) as active_limits
from {{ ref('silver_concentration_limit') }}
where rule_scope = 'ISSUER'
  and valid_from <= {{ reference_date() }}
  and (valid_to is null or valid_to >= {{ reference_date() }})
having count(*) <> 1
{% endtest %}
