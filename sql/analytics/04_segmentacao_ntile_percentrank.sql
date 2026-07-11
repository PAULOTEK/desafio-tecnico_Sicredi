-- =====================================================================
-- NTILE / PERCENT_RANK: segmentacao de clientes por valor liquido
-- transacionado no periodo. Divide a base em quartis (NTILE 4) e calcula
-- o ranking percentual (PERCENT_RANK) de cada cliente.
-- =====================================================================

WITH gasto_cliente AS (
    SELECT
        id_cliente,
        SUM(valor_liquido_total) AS valor_liquido_periodo,
        SUM(qtd_transacoes) AS qtd_transacoes
    FROM ouro.gold_cliente_mes
    GROUP BY id_cliente
)
SELECT
    id_cliente,
    valor_liquido_periodo,
    qtd_transacoes,
    NTILE(4) OVER (ORDER BY valor_liquido_periodo) AS quartil_gasto,
    ROUND(PERCENT_RANK() OVER (ORDER BY valor_liquido_periodo), 4) AS percentil_gasto,
    CASE
        WHEN NTILE(4) OVER (ORDER BY valor_liquido_periodo) = 4 THEN 'alto_valor'
        WHEN NTILE(4) OVER (ORDER BY valor_liquido_periodo) = 1 THEN 'baixo_valor'
        ELSE 'medio_valor'
    END AS faixa_valor
FROM gasto_cliente
ORDER BY valor_liquido_periodo DESC;
