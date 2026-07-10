-- =====================================================================

-- Para cada cliente/mes calcula:
--   #  movel do proprio cliente ate o mes (historico);
--   # grupo cidade+segmento no mes (benchmark de pares);
--   # desvio relativo do cliente frente ao grupo.
-- =====================================================================

WITH base AS (
    SELECT
        f.id_cliente,
        f.cidade_cliente,
        f.segmento_cliente,
        f.ano_mes,
        SUM(f.valor_liquido) AS valor_liquido_mes,
        COUNT(*) AS qtd_transacoes
    FROM ouro.gold_fato_transacao f
    WHERE f.id_cliente IS NOT NULL
      AND NOT f.flag_cartao_cancelado
    GROUP BY f.id_cliente, f.cidade_cliente, f.segmento_cliente, f.ano_mes
),
com_historico AS (
    SELECT
        b.*,
        AVG(valor_liquido_mes) OVER (
            PARTITION BY id_cliente
            ORDER BY ano_mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS media_historica_cliente
    FROM base b
),
com_grupo AS (
    SELECT
        h.*,
        AVG(valor_liquido_mes) OVER (
            PARTITION BY cidade_cliente, segmento_cliente, ano_mes
        ) AS media_grupo_cidade_segmento
    FROM com_historico h
)
SELECT
    id_cliente,
    cidade_cliente,
    segmento_cliente,
    ano_mes,
    valor_liquido_mes,
    ROUND(media_historica_cliente, 2) AS media_historica_cliente,
    ROUND(media_grupo_cidade_segmento, 2) AS media_grupo,
    ROUND(
        (valor_liquido_mes - media_grupo_cidade_segmento)
        / NULLIF(media_grupo_cidade_segmento, 0) * 100,
        2
    ) AS desvio_pct_vs_grupo
FROM com_grupo
ORDER BY id_cliente, ano_mes;
