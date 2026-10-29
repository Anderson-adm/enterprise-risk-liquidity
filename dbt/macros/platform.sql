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


{#- Replace a date's complete snapshot, including disappeared keys, in ONE MERGE.
    Tombstones use an impossible run id and exist only in the MERGE source, never in Gold. -#}
{% macro gold_snapshot_config() -%}
  {{ config(
      materialized='incremental', unique_key=['reference_date', 'portfolio_id'],
      incremental_strategy='merge', on_schema_change='fail',
      partitioned_by=(['reference_date'] if target.type == 'athena' else none)
  ) }}
  {% if target.type == 'athena' %}
    {{ config(delete_condition="src.pipeline_run_id = '__erl_deleted__'",
              insert_condition="src.pipeline_run_id <> '__erl_deleted__'") }}
  {% else %}
    {{ config(merge_clauses={
        'when_matched': [
            {'action': 'delete', 'condition': "DBT_INTERNAL_SOURCE.pipeline_run_id = '__erl_deleted__'"},
            {'action': 'update', 'mode': 'by_name'}
        ],
        'when_not_matched': [
            {'action': 'insert', 'mode': 'by_name',
             'condition': "DBT_INTERNAL_SOURCE.pipeline_run_id <> '__erl_deleted__'"}
        ]
    }) }}
  {% endif %}
{%- endmacro %}

{% macro gold_snapshot(candidate) -%}
  select * from {{ candidate }}
  {% if is_incremental() %}
    union all
    select
      {% for col in adapter.get_columns_in_relation(candidate) %}
        {% if col.name == 'pipeline_run_id' %}'__erl_deleted__'{% else %}previous.{{ adapter.quote(col.name) }}{% endif %}
        {% if not loop.last %}, {% endif %}
      {% endfor %}
    from {{ this }} previous
    where previous.reference_date = {{ reference_date() }}
      and not exists (
        select 1 from {{ candidate }} current
        where current.reference_date = previous.reference_date
          and current.portfolio_id = previous.portfolio_id
      )
  {% endif %}
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

{#- Trino decimal division retains max(operand scales): casting only the RESULT
    cannot recover digits lost by dividing two money values with scale 2. -#}
{% macro ratio(numerator, denominator) -%}
  {% if target.type == 'athena' %}
    (cast(({{ numerator }}) as decimal(38, 12)) / cast(({{ denominator }}) as decimal(38, 12)))
  {% else %}
    {#- DuckDB '/' returns DOUBLE even for decimals. Scale exact nonnegative inputs
        to integers, divide with half-up rounding to 12 places, then restore scale.
        Input scale 8 covers money, quantity and quantity * participation; HUGEINT
        holds the intermediate for values within the consumer's DECIMAL(20,2) range. -#}
    {% set n = 'cast(cast((' ~ numerator ~ ') as decimal(28, 8)) * 100000000 as hugeint)' %}
    {% set d = 'cast(cast((' ~ denominator ~ ') as decimal(28, 8)) * 100000000 as hugeint)' %}
    (cast((({{ n }} * 1000000000000 + {{ d }} // 2) // {{ d }}) as decimal(38, 0))
       * cast(0.000000000001 as decimal(13, 12)))
  {% endif %}
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
    where exists (
      select 1 from {{ source('bronze', 'ingestion_deliveries') }} committed
      where committed.status = 'ACCEPTED'
        and committed.source_system = b.meta_source_system
        and committed.entity = b.meta_entity
        and committed.run_id = b.meta_run_id
        and committed.input_object = b.meta_source_object
    )
  ) ranked
  where delivery_rank = 1 and meta_row_number > 0
{%- endmacro %}
