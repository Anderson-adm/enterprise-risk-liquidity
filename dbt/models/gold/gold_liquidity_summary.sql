{{ config(
    materialized='incremental',
    unique_key=['reference_date', 'portfolio_id'],
    incremental_strategy=('merge' if target.type == 'athena' else 'delete+insert'),
    on_schema_change='fail',
    partitioned_by=(['reference_date'] if target.type == 'athena' else none)
) }}
-- depends_on: {{ ref('gold_exposure_compliance_candidate') }}
-- The explicit dependency on the *other* product's candidate makes the reference date's
-- critical tests one gate: if either candidate fails, dbt build skips BOTH Gold tables.
-- Consumption table (Iceberg on Athena). Upserts the audited candidate for the reference
-- date by (reference_date, portfolio_id); other dates are never touched. Rows that vanish
-- upstream keep their old pipeline_run_id, and the Load step only exports rows written by
-- the current run, so stale rows are never published.
select * from {{ ref('gold_liquidity_summary_candidate') }}
