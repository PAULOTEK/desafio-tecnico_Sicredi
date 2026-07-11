"""Utilitarios de metadados tecnicos das camadas.

Concentra a logica de enriquecimento de metadados exigida na camada Bronze
(arquivo_origem, data_ingestao, timestamp_ingestao, batch_id, hash_linha,
schema_version), reutilizavel por qualquer ingestao.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Colunas tecnicas adicionadas na Bronze. Mantidas em constante para que a
# Prata consiga remove-las/renomea-las de forma consistente.
COLUNAS_METADADOS = [
    "arquivo_origem",
    "data_ingestao",
    "timestamp_ingestao",
    "batch_id",
    "hash_linha",
    "schema_version",
]


def _coluna_arquivo_origem(df: DataFrame) -> Column:
    """Retorna a coluna com o caminho do arquivo de origem.

    Usa a coluna oculta ``_metadata.file_path`` (compativel com Spark Connect /
    Databricks Serverless, ao contrario de ``input_file_name()``). Quando o
    DataFrame nao vem de uma fonte de arquivo (ex.: dados em memoria nos testes),
    ``_metadata`` nao existe — nesse caso devolvemos ``NULL``.
    """

    coluna = F.col("_metadata.file_path")
    try:
        # Em Spark classico a analise e imediata; se _metadata nao resolver,
        # cai no fallback. Em Spark Connect a fonte e sempre de arquivo aqui.
        df.select(coluna)
        return coluna
    except Exception:
        return F.lit(None).cast("string")


def _hash_linha(colunas_negocio: list[str]) -> Column:
    """Gera um hash SHA-256 estavel a partir das colunas de negocio.

    Usado para deduplicacao idempotente: a mesma linha ingerida em cargas
    diferentes produz o mesmo hash, permitindo descartar duplicatas exatas.
    """

    # coalesce para que nulos nao "sumam" da concatenacao e alterem o hash.
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("<nulo>")) for c in colunas_negocio]
    return F.sha2(F.concat_ws("||", *partes), 256)


def adicionar_metadados_bronze(
    df: DataFrame,
    *,
    batch_id: str,
    schema_version: str = "v1",
) -> DataFrame:
    """Adiciona as colunas tecnicas de auditoria da camada Bronze.

    ``arquivo_origem`` vem da coluna oculta ``_metadata.file_path`` das fontes de
    arquivo do Spark, que registra o arquivo fisico lido para cada linha. Usamos
    ``_metadata`` (e nao ``input_file_name()``) por ser compativel com o Spark
    Connect / Databricks Serverless, onde ``input_file_name()`` nao e suportado.
    """

    colunas_negocio = [c for c in df.columns if c not in COLUNAS_METADADOS]

    return (
        df.withColumn("arquivo_origem", _coluna_arquivo_origem(df))
        .withColumn("data_ingestao", F.current_date())
        .withColumn("timestamp_ingestao", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("hash_linha", _hash_linha(colunas_negocio))
        .withColumn("schema_version", F.lit(schema_version))
    )
