-- =====================================================================
-- ROW_NUMBER: selecao do registro vigente por cliente a partir do
-- historico SCD2 (deduplicacao/versionamento).
--
-- Recupera a versao mais recente de cada cliente ordenando as versoes
-- por data de inicio de vigencia. Equivale a filtrar flag_vigente = true,
-- mas demonstra explicitamente o padrao de versionamento com ROW_NUMBER.
-- =====================================================================

WITH versoes AS (
    SELECT
        id_cliente,
        nome,
        cidade,
        segmento,
        renda,
        data_inicio_vigencia,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente
            ORDER BY data_inicio_vigencia DESC
        ) AS rn
    FROM prata.clientes
)
SELECT
    id_cliente,
    nome,
    cidade,
    segmento,
    renda,
    data_inicio_vigencia
FROM versoes
WHERE rn = 1
ORDER BY id_cliente;
