-- =====================================================================
-- estabelecimento em que cada cliente transacionou, alem do valor da
-- primeira e da ultima transacao (ordenadas por data).
-- =====================================================================

WITH transacoes AS (
    SELECT
        id_cliente,
        data_transacao,
        estabelecimento,
        valor
    FROM ouro.gold_fato_transacao
    WHERE id_cliente IS NOT NULL
      AND NOT flag_cartao_cancelado
)
SELECT DISTINCT
    id_cliente,
    FIRST_VALUE(estabelecimento) OVER janela AS primeiro_estabelecimento,
    FIRST_VALUE(valor) OVER janela AS valor_primeira_transacao,
    LAST_VALUE(estabelecimento) OVER janela AS ultimo_estabelecimento,
    LAST_VALUE(valor) OVER janela AS valor_ultima_transacao
FROM transacoes
WINDOW janela AS (
    PARTITION BY id_cliente
    ORDER BY data_transacao
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ORDER BY id_cliente;
