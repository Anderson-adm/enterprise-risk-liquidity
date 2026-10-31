-- portfolio_risk.processed_position: every processing version of the reference date
SELECT position_id, portfolio_code, instrument_code, reference_date, quantity, market_value, currency,
       processing_status, processing_version, processed_at
FROM processed_position
WHERE reference_date = :reference_date
ORDER BY position_id
