-- =====================================================================

-- =====================================================================

WITH stats_cliente AS (
    SELECT
        id_cliente,
        AVG(valor) AS media_valor,
        STDDEV(valor) AS desvio_valor
    FROM ouro.gold_fato_transacao
    WHERE id_cliente IS NOT NULL
    GROUP BY id_cliente
),
transacoes_marcadas AS (
    SELECT
        f.id_transacao,
        f.id_cliente,
        f.data_transacao,
        f.valor,
        f.flag_cartao_cancelado,
        f.flag_fraude,
        f.flag_chargeback,
        s.media_valor,
        s.desvio_valor,
        (f.valor - s.media_valor) / NULLIF(s.desvio_valor, 0) AS z_score
    FROM ouro.gold_fato_transacao f
    JOIN stats_cliente s USING (id_cliente)
)
SELECT
    id_transacao,
    id_cliente,
    data_transacao,
    valor,
    ROUND(z_score, 2) AS z_score,
    CASE
        WHEN flag_fraude OR flag_chargeback THEN 'evento_risco'
        WHEN flag_cartao_cancelado THEN 'cartao_cancelado'
        WHEN ABS(z_score) > 3 THEN 'valor_atipico'
        ELSE 'normal'
    END AS tipo_anomalia
FROM transacoes_marcadas
WHERE flag_fraude
   OR flag_chargeback
   OR flag_cartao_cancelado
   OR ABS(z_score) > 3
ORDER BY id_cliente, data_transacao;
