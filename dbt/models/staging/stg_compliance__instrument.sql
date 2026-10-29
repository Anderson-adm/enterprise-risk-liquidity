-- Instrument and issuer register of the exposure & compliance system (source 4).
with i as ({{ latest_delivery(source('bronze', 'exposure_compliance__instrument_registry'), scope='latest') }})
select
    upper(trim(instrument_code))  as instrument_code,
    instrument_name,
    upper(trim(instrument_type))  as instrument_type,
    upper(trim(issuer_code))      as issuer_code,
    issuer_name,
    upper(trim(issuer_type))      as issuer_type
from i
