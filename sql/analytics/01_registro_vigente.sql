-- =====================================================================
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
