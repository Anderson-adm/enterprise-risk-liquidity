{#- Row-level boolean assertion (same idea as dbt_utils.expression_is_true, without a package
    download). -#}
{% test expression_is_true(model, expression) %}
select * from {{ model }} where not ({{ expression }})
{% endtest %}
