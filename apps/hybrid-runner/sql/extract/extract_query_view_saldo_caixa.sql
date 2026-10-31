-- legacy_finance cash balance through the legacy view
SELECT cod_cart, dt_ref, vl_saldo, cd_moeda
FROM vw_saldo_caixa
WHERE dt_ref = :reference_date
ORDER BY cod_cart
