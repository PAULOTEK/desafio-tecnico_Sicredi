# Databricks notebook source
# MAGIC %md
# MAGIC # Gold
# MAGIC


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
from novarota.transformacao.ouro import executar_ouro

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"
spark = obter_spark("novarota-ouro", config)

try:
    dbutils  # noqa: F821
    preparar_unity_catalog(spark, config)
except NameError:
    pass

# COMMAND ----------

executar_ouro(spark, config)

# COMMAND ----------

# Conferência: T0003 (estorno total) -> valor_liquido 0; T0021 (parcial) -> 2800;
# T0022 -> flag_cartao_cancelado.
display(spark.table(config.tabela(config.schema_ouro, "gold_fato_transacao")))  # noqa: F821

# COMMAND ----------

display(spark.table(config.tabela(config.schema_ouro, "gold_features_cliente")))  # noqa: F821
