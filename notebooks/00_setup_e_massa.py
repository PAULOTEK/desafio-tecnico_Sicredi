# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente + massa sintética
# MAGIC
# MAGIC Prepara o **Unity Catalog** (catálogo, schemas e Volume `landing`) e gera a
# MAGIC massa sintética de demonstração dentro do Volume. Rode **uma vez** antes das
# MAGIC camadas Bronze/Prata/Ouro. A lógica vive no pacote `novarota` (em `src/`).

# COMMAND ----------

# MAGIC %md ## Disponibiliza o pacote `novarota` para import
# MAGIC Em Job/CI (Asset Bundle) usa o parâmetro `bundle_root`; em uso interativo
# MAGIC (Repos) deduz a raiz pela localização do notebook. Sem `%pip` manual.

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
from novarota.ingestao.gerador_dados import gerar_massa

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"
spark = obter_spark("novarota-setup", config)

# No Databricks prepara o Unity Catalog e aponta o landing para o Volume.
try:
    dbutils  # noqa: F821
    preparar_unity_catalog(spark, config)
except NameError:
    pass

print(config)

# COMMAND ----------

# MAGIC %md ## Geração da massa sintética (apenas em demonstração)

# COMMAND ----------

gerar_massa(config.dir_landing)
print("Massa gerada em:", config.dir_landing)
