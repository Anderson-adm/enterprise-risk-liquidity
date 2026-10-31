-- fund_ops.fund: full register (no reference date)
SELECT fund_id, fund_name, fund_type, base_currency, inception_date, status
FROM fund
ORDER BY fund_id
