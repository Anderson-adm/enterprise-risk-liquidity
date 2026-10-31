-- exposure_compliance.stock_position: compliance stock of the reference date
SELECT stock_id, fund_ref, instrument_code, position_date, quantity, book_value
FROM stock_position
WHERE position_date = :reference_date
ORDER BY stock_id
