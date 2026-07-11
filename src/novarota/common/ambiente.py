"""Deteccao de ambiente e preparo do Unity Catalog no Databricks.

Concentra o setup que cada notebook de camada precisa fazer no Databricks
(catalogo/schemas/Volume, selecao do catalogo e caminho do landing), evitando
duplicar essa logica em cada notebook. Em execucao local nao faz nada — os
nomes ficam em ``schema.tabela`` e os caminhos permanecem os locais.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from novarota.config import Config


def preparar_unity_catalog(spark: SparkSession, config: Config) -> None:
    """Prepara o Unity Catalog e ajusta a ``config`` para o Databricks.

    Idempotente (usa ``CREATE ... IF NOT EXISTS``), portanto pode ser chamado no
    inicio de qualquer notebook de camada. Efeitos:

    * liga ``config.usar_catalogo`` (nomes ``catalogo.schema.tabela``);
    * cria catalogo, schemas (bronze/prata/ouro) e o Volume ``landing``;
    * seleciona o catalogo (``USE CATALOG``) para resolver ``schema.tabela``;
    * aponta ``config.dir_landing`` para o Volume de entrada.
    """

    config.usar_catalogo = True
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalogo}")
    for schema in (config.schema_bronze, config.schema_prata, config.schema_ouro):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalogo}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.catalogo}.{config.schema_bronze}.landing")
    spark.sql(f"USE CATALOG {config.catalogo}")
    config.dir_landing = Path(f"/Volumes/{config.catalogo}/{config.schema_bronze}/landing")
