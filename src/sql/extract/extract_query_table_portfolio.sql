-- fund_ops.portfolio: full register
SELECT portfolio_id, fund_id, portfolio_name, position_source_system, status, opened_on
FROM portfolio
ORDER BY portfolio_id
