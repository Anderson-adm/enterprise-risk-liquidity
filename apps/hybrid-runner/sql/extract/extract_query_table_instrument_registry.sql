-- exposure_compliance.instrument_registry: full register
SELECT instrument_code, instrument_name, instrument_type, issuer_code, issuer_name, issuer_type
FROM instrument_registry
ORDER BY instrument_code
