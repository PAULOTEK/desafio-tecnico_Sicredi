"""Utilitarios de metadados tecnicos das camadas.

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

    ``arquivo_origem`` e derivado da funcao interna ``input_file_name`` do
    Spark, que registra o arquivo fisico lido para cada linha.
    """

    colunas_negocio = [c for c in df.columns if c not in COLUNAS_METADADOS]

    return (
        df.withColumn("arquivo_origem", F.input_file_name())
        .withColumn("data_ingestao", F.current_date())
        .withColumn("timestamp_ingestao", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("hash_linha", _hash_linha(colunas_negocio))
        .withColumn("schema_version", F.lit(schema_version))
    )
