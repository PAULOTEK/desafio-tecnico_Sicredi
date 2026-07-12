"""Camada Bronze: ingestao incremental de arquivos para Delta Lake.

Preserva os dados brutos exatamente como chegam (inclusive campos inesperados
em caso de evolucao de schema, gracas ao ``mergeSchema``) e adiciona metadados
tecnicos de auditoria.

Estrategia de incremental sem Auto Loader
-----------------------------------------
Mantemos uma tabela de controle (``bronze._controle_ingestao``) com o caminho
de cada arquivo ja processado. A cada execucao, apenas arquivos ainda nao
registrados sao lidos. Isso torna a ingestao **idempotente**: reprocessar a
mesma carga nao duplica dados, pois os arquivos ja constam no controle.

Em producao no Databricks usariamos **Auto Loader** (``cloudFiles``), que
resolve deteccao incremental, checkpoint e evolucao de schema de forma
gerenciada e escalavel (ver docs/decisoes-tecnicas.md).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from novarota.common.logging_config import obter_logger
from novarota.common.metadados import adicionar_metadados_bronze
from novarota.config import Config

logger = obter_logger("novarota.bronze")

_TABELA_CONTROLE = "_controle_ingestao"

_SCHEMA_CONTROLE = StructType(
    [
        StructField("fonte", StringType(), False),
        StructField("arquivo_origem", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("timestamp_ingestao", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class FonteBronze:
    """Descreve uma fonte de dados a ser ingerida na Bronze."""

    nome: str  # nome logico e tabela destino (ex.: "clientes")
    formato: str  # "json" ou "csv"
    subpasta: str  # subpasta em landing (ex.: "clientes")
    schema_version: str = "v1"


FONTES_PADRAO: list[FonteBronze] = [
    FonteBronze("clientes", "json", "clientes"),
    FonteBronze("contas", "json", "contas"),
    FonteBronze("cartoes", "json", "cartoes"),
    FonteBronze("transacoes", "csv", "transacoes"),
    FonteBronze("eventos_risco", "csv", "eventos_risco"),
    FonteBronze("estornos", "csv", "estornos"),
]


def _garantir_schema(spark: SparkSession, config: Config) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema_qualificado(config.schema_bronze)}")


def _tabela_controle(config: Config) -> str:
    return config.tabela(config.schema_bronze, _TABELA_CONTROLE)


def _arquivos_ja_ingeridos(spark: SparkSession, config: Config, fonte: str) -> set[str]:
    tabela = _tabela_controle(config)
    # Use SQL-based check for Serverless compatibility (tableExists() not whitelisted)
    try:
        linhas = (
            spark.table(tabela)
            .where(F.col("fonte") == fonte)
            .select("arquivo_origem")
            .collect()
        )
        return {linha["arquivo_origem"] for linha in linhas}
    except Exception:
        # Table doesn't exist yet
        return set()


def _listar_arquivos(config: Config, fonte: FonteBronze) -> list[Path]:
    base = config.dir_landing / fonte.subpasta
    if not base.exists():
        return []
    extensao = f"*.{fonte.formato}"
    return sorted(base.rglob(extensao))


def _para_uri(caminho: Path) -> str:
    """Converte o caminho local para file URI, alinhado ao input_file_name."""

    return str(caminho)


def _ler_arquivos(spark: SparkSession, fonte: FonteBronze, arquivos: list[Path]) -> DataFrame:
    caminhos = [str(p) for p in arquivos]
    if fonte.formato == "json":
        return spark.read.json(caminhos)
    if fonte.formato == "csv":
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(caminhos)
        )
    raise ValueError(f"Formato nao suportado: {fonte.formato}")


def _registrar_controle(
    spark: SparkSession,
    config: Config,
    fonte: str,
    arquivos: list[Path],
) -> None:
    tabela = _tabela_controle(config)
    dados = [
        {
            "fonte": fonte,
            "arquivo_origem": _para_uri(p),
            "batch_id": config.batch_id,
        }
        for p in arquivos
    ]
    df = (
        spark.createDataFrame(dados, schema=StructType(_SCHEMA_CONTROLE[:3]))
        .withColumn("timestamp_ingestao", F.current_timestamp())
    )
    df.write.format("delta").mode("append").saveAsTable(tabela)


def ingerir_fonte(spark: SparkSession, config: Config, fonte: FonteBronze) -> int:
    """Ingere incrementalmente os arquivos novos de uma fonte.

    Retorna a quantidade de linhas ingeridas nesta execucao.
    """

    _garantir_schema(spark, config)

    candidatos = _listar_arquivos(config, fonte)
    if not candidatos:
        logger.warning("fonte=%s sem_arquivos_em=%s", fonte.nome, config.dir_landing / fonte.subpasta)
        return 0

    ja_ingeridos = _arquivos_ja_ingeridos(spark, config, fonte.nome)
    novos = [p for p in candidatos if _para_uri(p) not in ja_ingeridos]

    if not novos:
        logger.info("fonte=%s nada_novo arquivos_existentes=%d", fonte.nome, len(candidatos))
        return 0

    logger.info("fonte=%s arquivos_novos=%d", fonte.nome, len(novos))

    df_bruto = _ler_arquivos(spark, fonte, novos)
    df = adicionar_metadados_bronze(
        df_bruto, batch_id=config.batch_id, schema_version=fonte.schema_version
    )

    tabela = config.tabela(config.schema_bronze, fonte.nome)
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")  # preserva campos inesperados (schema evolution)
        .saveAsTable(tabela)
    )

    _registrar_controle(spark, config, fonte.nome, novos)

    qtd = df.count()
    logger.info("fonte=%s linhas_ingeridas=%d tabela=%s", fonte.nome, qtd, tabela)
    return qtd


def executar_bronze(spark: SparkSession, config: Config) -> dict[str, int]:
    """Executa a ingestao Bronze para todas as fontes padrao."""

    logger.info("=== BRONZE inicio batch_id=%s modo=%s ===", config.batch_id, config.modo_execucao)
    resumo: dict[str, int] = {}
    for fonte in FONTES_PADRAO:
        resumo[fonte.nome] = ingerir_fonte(spark, config, fonte)
    logger.info("=== BRONZE fim resumo=%s ===", resumo)
    return resumo
