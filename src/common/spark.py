"""Fabrica de SparkSession habilitada para Delta Lake.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.config import Config

# Versao alinhada ao PySpark 3.5.x (ver requirements.txt).
_PACOTE_DELTA = "io.delta:delta-spark_2.12:3.2.0"


def criar_spark(app_name: str, config: Config | None = None) -> SparkSession:
    """Cria (ou reaproveita) uma SparkSession com suporte a Delta Lake."""

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", _PACOTE_DELTA)
        # Boas praticas de performance/qualidade em Delta.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
    )

    if config is not None:
        # Warehouse e metastore em caminhos fixos (independentes do CWD) para que
        # os schemas/tabelas persistam entre execucoes e entre jobs separados.
        # Em Databricks isso e gerenciado pelo Unity Catalog.
        warehouse = config.dir_warehouse.resolve()
        warehouse.mkdir(parents=True, exist_ok=True)
        metastore = (config.dir_dados / "metastore_db").resolve()
        builder = (
            builder.config("spark.sql.warehouse.dir", str(warehouse))
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={metastore};create=true",
            )
            .enableHiveSupport()
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def obter_spark(app_name: str, config: Config | None = None) -> SparkSession:
    """Retorna a SparkSession adequada ao ambiente.

    Em Databricks (ou qualquer ambiente que ja tenha uma sessao ativa) reaproveita
    a sessao existente, com Delta/Unity Catalog gerenciados pelo cluster. Fora dele
    (execucao local), cria a sessao configurada por :func:`criar_spark`.
    """

    ativa = SparkSession.getActiveSession()
    if ativa is not None:
        return ativa
    return criar_spark(app_name, config)
