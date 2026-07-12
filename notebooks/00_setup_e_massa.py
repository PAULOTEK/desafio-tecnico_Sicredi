# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente + massa sintética
# MAGIC
# MAGIC Prepara o **Unity Catalog** (catálogo, schemas e Volume `landing`) e gera a
# MAGIC massa sintética de demonstração dentro do Volume. Rode **uma vez** antes das
# MAGIC camadas Bronze/Prata/Ouro. A lógica vive no pacote `src/`.

# COMMAND ----------

# MAGIC %md ## Disponibiliza o pacote `src` para import
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
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _raiz = "/Workspace" + _nb.rsplit("/notebooks/", 1)[0]

if f"{_raiz}/src" not in sys.path:
    sys.path.insert(0, f"{_raiz}/src")

# COMMAND ----------

from src.novarota.common.ambiente  import preparar_unity_catalog
from src.novarota.config import Config
from src.novarota.ingestao.gerador_dados import gerar_massa

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"

# Prepara o Unity Catalog (catálogo, schemas, Volume) e aponta o landing p/ o Volume.
preparar_unity_catalog(spark, config)  # noqa: F821
print(config)

# COMMAND ----------

# MAGIC %md ## Geração da massa sintética (apenas em demonstração)

# COMMAND ----------

gerar_massa(config.dir_landing)
print("Massa gerada em:", config.dir_landing)
