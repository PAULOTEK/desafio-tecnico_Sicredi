# Databricks notebook source

# COMMAND ----------

# MAGIC %md ## Instalação do pacote

# COMMAND ----------

# MAGIC %pip install -e /Workspace/Repos/pauloalexandre820@gmail.com/desafio-tecnico_Sicredi
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pathlib import Path

from src.common.spark import obter_spark
from src.config import Config
from src.ingestao.bronze import executar_bronze
from src.ingestao.gerador_dados import gerar_massa
from src.transformacao.ouro import executar_ouro
from src.transformacao.prata import executar_prata

# COMMAND ----------

# MAGIC %md ## Configuração e ambiente (Unity Catalog + Volume)

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"

# No Databricks reaproveita a sessao nativa; localmente cria a sessao com Delta.
spark = obter_spark("novarota-pipeline", config)

# Detecta Databricks para aplicar Unity Catalog + Volumes (necessario no Serverless).
try:
    dbutils  # noqa: F821
    EM_DATABRICKS = True
except NameError:
    EM_DATABRICKS = False

if EM_DATABRICKS:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalogo}")
    for _schema in (config.schema_bronze, config.schema_prata, config.schema_ouro):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalogo}.{_schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.catalogo}.{config.schema_bronze}.landing")
    # Seleciona o catalogo -> os nomes schema.tabela resolvem dentro dele.
    spark.sql(f"USE CATALOG {config.catalogo}")
    # Serverless le/escreve em Volumes (nao no Workspace).
    config.dir_landing = Path(f"/Volumes/{config.catalogo}/{config.schema_bronze}/landing")

print(config)

# COMMAND ----------

# MAGIC %md ## Geração da massa sintética (apenas em ambiente de demonstração)

# COMMAND ----------

gerar_massa(config.dir_landing)

# COMMAND ----------

# MAGIC %md ## Execução das camadas

# COMMAND ----------

executar_bronze(spark, config)
executar_prata(spark, config)
executar_ouro(spark, config)

# COMMAND ----------

# MAGIC %md ## Conferência rápida

# COMMAND ----------

display(spark.table(config.tabela(config.schema_ouro, "gold_fato_transacao")))

# COMMAND ----------

display(spark.table(config.tabela(config.schema_ouro, "gold_features_cliente")))