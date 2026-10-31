-- legacy_finance redemptions open on the reference date: requested up to it, settling after it
SELECT id_resgate, cod_cart, dt_pedido, dt_cotizacao, vl_resgate, st_resgate
FROM vw_resgate_pendente
WHERE dt_pedido <= :reference_date AND dt_cotizacao > :reference_date
ORDER BY id_resgate
