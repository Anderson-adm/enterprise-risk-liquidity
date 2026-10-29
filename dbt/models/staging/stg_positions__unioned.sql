-- Positions from the three possible systems of record, in one normalised shape.
--   portfolio_risk (DB, source 2): own lowercase codes, versioned reprocessing
--   internal_xml   (source 6)    : canonical portfolio ids, market value given
--   external_xml   (source 7)    : administrator-scoped ids, quantity x unit price
with risk as ({{ latest_delivery(source('bronze', 'portfolio_risk__processed_position')) }}),
     ixml as ({{ latest_delivery(source('bronze', 'internal_xml__portfolio_position')) }}),
     exml as ({{ latest_delivery(source('bronze', 'external_xml__transfer_in_position')) }})
select
    'portfolio_risk'                                   as source_system,
    upper(trim(portfolio_code))                        as source_portfolio_ref,
    upper(trim(instrument_code))                       as instrument_code,
    {{ to_date('reference_date') }}                    as reference_date,
    {{ to_decimal('quantity', 24, 6) }}                as quantity,
    {{ to_decimal('market_value', 20, 2) }}            as market_value,
    upper(trim(currency))                              as currency,
    try_cast(processing_version as integer)            as processing_version,
    try_cast(position_id as bigint)                    as source_record_id,
    meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from risk
union all
select
    'internal_xml',
    upper(trim(portfolio_code)),
    upper(trim(instrument_code)),
    {{ to_date('reference_date') }},
    {{ to_decimal('quantity', 24, 6) }},
    {{ to_decimal('market_value', 20, 2) }},
    upper(trim(currency)),
    1,
    meta_row_number,
    meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from ixml
union all
select
    'external_xml',
    upper(trim(administrator_code)) || ':' || upper(trim(external_fund_id)),
    upper(trim(asset_code)),
    delivery_reference_date,  -- dd/MM/yyyy AsOf validated at ingestion; partition holds the ISO date
    {{ to_decimal('qty', 24, 6) }},
    cast(round({{ to_decimal('qty', 24, 6) }} * {{ to_decimal('price', 20, 6) }}, 2) as decimal(20, 2)),
    upper(trim(ccy)),
    1,
    meta_row_number,
    meta_run_id, meta_source_object, meta_row_number, meta_row_hash
from exml
