-- =====================================================================

-- meses consecutivos "variacao absoluta e percentual do valor liquido".
-- =====================================================================

WITH base AS (
    SELECT
        id_cliente,
        ano_mes,
        valor_liquido_total,
        qtd_transacoes
    FROM ouro.gold_cliente_mes
),
comparativo AS (
    SELECT
        id_cliente,
        ano_mes,
        valor_liquido_total,
        qtd_transacoes,
        LAG(valor_liquido_total) OVER (
            PARTITION BY id_cliente ORDER BY ano_mes
        ) AS valor_mes_anterior,
        LEAD(valor_liquido_total) OVER (
            PARTITION BY id_cliente ORDER BY ano_mes
        ) AS valor_mes_seguinte
    FROM base
)
SELECT
    id_cliente,
    ano_mes,
    valor_liquido_total,
    valor_mes_anterior,
    (valor_liquido_total - valor_mes_anterior) AS variacao_absoluta,
    ROUND(
        (valor_liquido_total - valor_mes_anterior)
        / NULLIF(valor_mes_anterior, 0) * 100,
        2
    ) AS variacao_percentual
FROM comparativo
ORDER BY id_cliente, ano_mes;
