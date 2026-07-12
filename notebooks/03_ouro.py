# Databricks notebook source
# MAGIC %md
# MAGIC # Gold
# MAGIC
# MAGIC Esta é a camada onde **tudo se junta**. O `gold_fato_transacao` cruza cada
# MAGIC transação com o cadastro **vigente na data** (join temporal usando o SCD2 da
# MAGIC Prata), soma estornos/eventos e aplica as regras de negócio. Dele derivam
# MAGIC `gold_cliente_mes`, `gold_indicadores_risco`, `gold_features_cliente` e as
# MAGIC dimensões. Aqui eu executo e **confiro as regras** com a massa.
# MAGIC
# MAGIC Pré-requisito: ter rodado o `02_prata`.

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

from pyspark.sql import functions as F
from src.novarota.common.ambiente import preparar_unity_catalog
from src.novarota.config import Config
from src.novarota.transformacao.ouro import construir_fato_transacao, executar_ouro

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"
preparar_unity_catalog(spark, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. O join temporal (coração da Ouro)
# MAGIC `construir_fato_transacao` casa a transação com cartão/conta/cliente **na
# MAGIC versão vigente na data** (`data >= inicio AND data < fim`), agrega estornos e
# MAGIC eventos, e calcula `valor_liquido`, `flag_cartao_cancelado`, `flag_fraude`
# MAGIC etc. Construo aqui para inspecionar antes de materializar.

# COMMAND ----------

fato = construir_fato_transacao(spark, config)
display(
    fato.select(
        "id_transacao", "data_transacao", "id_cliente", "cidade_cliente",
        "status_cartao_vigente", "valor", "valor_estornado", "valor_liquido",
        "flag_estornada", "flag_cartao_cancelado",
    ).orderBy("id_transacao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conferência das regras de negócio
# MAGIC Checo na marra os casos que o desafio pede:
# MAGIC - **T0003**: estorno total -> `valor_liquido` deve ser **0**;
# MAGIC - **T0021**: estorno parcial (4800 − 2000) -> `valor_liquido` **2800**;
# MAGIC - **T0022**: transação em cartão **cancelado** -> `flag_cartao_cancelado` true.

# COMMAND ----------

casos = {r["id_transacao"]: r for r in fato.collect()}
t3, t21, t22 = casos["T0003"], casos["T0021"], casos["T0022"]
print("T0003 valor_liquido        :", t3["valor_liquido"], "(esperado 0)")
print("T0021 valor_liquido        :", t21["valor_liquido"], "(esperado 2800)")
print("T0022 flag_cartao_cancelado:", t22["flag_cartao_cancelado"], "(esperado True)")
assert t3["valor_liquido"] == 0.0
assert t21["valor_liquido"] == 2800.0
assert t22["flag_cartao_cancelado"] is True
print("OK — regras de estorno e cartão cancelado conferem.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Materializa a Ouro inteira
# MAGIC `executar_ouro` grava o fato + dimensões + as visões analíticas. Cartão
# MAGIC cancelado **preserva histórico no fato**, mas fica **fora** das métricas
# MAGIC mensais.

# COMMAND ----------

executar_ouro(spark, config)

fato_gravado = spark.table(config.tabela(config.schema_ouro, "gold_fato_transacao"))
print("gold_fato_transacao — registros:", fato_gravado.count())

# COMMAND ----------

# MAGIC %md ## 4. cliente mes (comportamento mensal)

# COMMAND ----------

display(
    spark.table(config.tabela(config.schema_ouro, "gold_cliente_mes"))
    .orderBy("id_cliente", "ano_mes")
)

# COMMAND ----------

# MAGIC %md ## 5. indicadores risco (fraude / chargeback / estorno)

# COMMAND ----------

display(
    spark.table(config.tabela(config.schema_ouro, "gold_indicadores_risco"))
    .orderBy(F.col("qtd_eventos_risco").desc())
)

# COMMAND ----------

# MAGIC %md ## 6. Features de cliente (consumo por Data Science)

# COMMAND ----------

display(spark.table(config.tabela(config.schema_ouro, "gold_features_cliente")))
