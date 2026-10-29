select instrument_code, instrument_name, instrument_type, issuer_code, issuer_name, issuer_type
from {{ ref('stg_compliance__instrument') }}
