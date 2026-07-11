# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Camada Bronze
# MAGIC
# MAGIC Ingestão **incremental** e **idempotente** dos arquivos de `landing` para
# MAGIC tabelas Delta gerenciadas, preservando o dado bruto (+ `mergeSchema`) e
# MAGIC adicionando metadados técnicos. Pré-requisito: rodar `00_setup_e_massa`.

# COMMAND ----------

import sys

_raiz = ""
try:
    dbutils.widgets.text("bundle_root", "")  # noqa: F821
    _raiz = dbutils.widgets.get("bundle_root")  # noqa: F821
except Exception:  # noqa: BLE001
    _raiz = ""

if not _raiz:
    try:
        _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        _nb = _ctx.notebookPath().get()
        _raiz = "/Workspace" + _nb.rsplit("/notebooks/", 1)[0]
    except Exception:  # noqa: BLE001
        _raiz = ""

if _raiz and f"{_raiz}/src" not in sys.path:
    sys.path.insert(0, f"{_raiz}/src")

# COMMAND ----------

from novarota.common.ambiente import preparar_unity_catalog
from novarota.common.spark import obter_spark
from novarota.config import Config
from novarota.ingestao.bronze import executar_bronze

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"
spark = obter_spark("novarota-bronze", config)

try:
    dbutils  # noqa: F821
    preparar_unity_catalog(spark, config)
except NameError:
    pass

# COMMAND ----------

resumo = executar_bronze(spark, config)
print("Bronze:", resumo)

# COMMAND ----------

display(spark.table(config.tabela(config.schema_bronze, "transacoes")))  # noqa: F821
