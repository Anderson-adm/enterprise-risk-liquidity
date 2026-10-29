{#- Engine-specific SQL is isolated here. Everything else is written in the common subset of
    DuckDB (local) and Trino/Athena engine v3. -#}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- elif target.type == 'athena' -%}
    {{ env_var('ERL_GLUE_DB_PREFIX', 'erl_dev') }}_{{ custom_schema_name | trim }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}


{#- Distinct, ordered, comma-separated list aggregation. -#}
{% macro list_agg(expr) -%}
  {{ return(adapter.dispatch('list_agg', 'erl')(expr)) }}
{%- endmacro %}

{% macro default__list_agg(expr) -%}
  string_agg(distinct {{ expr }}, ',' order by {{ expr }})
{%- endmacro %}

{% macro athena__list_agg(expr) -%}
  array_join(array_sort(array_distinct(array_agg({{ expr }}))), ',')
{%- endmacro %}


{#- Text (as received in Bronze) to exact decimal. Accepts '.' or ',' as decimal separator.
    Returns NULL when the text is not a number, so Silver can reject the record with a reason. -#}
{% macro to_decimal(expr, precision, scale) -%}
  try_cast(replace(trim({{ expr }}), ',', '.') as decimal({{ precision }}, {{ scale }}))
{%- endmacro %}

{% macro to_date(expr) -%}
  try_cast(trim({{ expr }}) as date)
{%- endmacro %}

{% macro reference_date() -%}
  cast('{{ var("reference_date") }}' as date)
{%- endmacro %}


{#- Bronze is append-only: every delivery is kept. A "snapshot" is the unit a new delivery
    replaces (e.g. one day of market data, one portfolio's XML statement for one day).
    scope='snapshot': keep the most recent delivery of every snapshot key.
    scope='latest'  : keep only the most recent delivery overall (reference registers). -#}
{% macro latest_delivery(relation, scope='snapshot') -%}
  select * from (
    select
      b.*,
      cast(b.bronze_reference_date as date) as delivery_reference_date,
      dense_rank() over (
        {% if scope == 'snapshot' %}partition by b.meta_snapshot_key{% endif %}
        order by b.meta_ingested_at desc, b.meta_run_id desc
      ) as delivery_rank
    from {{ relation }} b
  ) ranked
  where delivery_rank = 1
{%- endmacro %}
