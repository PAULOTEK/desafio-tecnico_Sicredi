-- =====================================================================
-- =====================================================================

-- Tabela alvo (criada uma unica vez).
CREATE TABLE IF NOT EXISTS ouro.gold_snapshot_cartao (
    id_cartao INT,
    id_conta INT,
    tipo_cartao STRING,
    limite DOUBLE,
    status_cartao STRING,
    atualizado_em TIMESTAMP
) USING DELTA;

-- MERGE a partir da versao vigente do SCD2.
MERGE INTO ouro.gold_snapshot_cartao AS destino
USING (
    SELECT
        id_cartao,
        id_conta,
        tipo_cartao,
        limite,
        status_cartao,
        current_timestamp() AS atualizado_em
    FROM prata.cartoes
    WHERE flag_vigente = true
) AS origem
ON destino.id_cartao = origem.id_cartao
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
