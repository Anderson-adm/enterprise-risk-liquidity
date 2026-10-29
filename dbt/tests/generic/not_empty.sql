{#- A product with zero rows for the reference date means a source or join failed upstream. -#}
{% test not_empty(model) %}
select 1 as failure from (select count(*) as n from {{ model }}) c where c.n = 0
{% endtest %}
