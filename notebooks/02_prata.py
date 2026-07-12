# Databricks notebook source
# MAGIC %md
# MAGIC # Silver
# MAGIC
# MAGIC Aqui a tratamento de dados e tipo as 6 entidades, aplico **regras de qualidade** (mandando
# MAGIC o inválido para **quarentena**) e materializo o histórico de
# MAGIC clientes/contas/cartões com **MERGE** idempotente. As regras e o algoritmo
# MAGIC
# MAGIC Pré-requisito: ter rodado o `01_bronze`.

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
from src.novarota.common.metadados import COLUNAS_METADADOS
from src.novarota.config import Config
from src.novarota.qualidade.regras import aplicar_regras, regras_clientes
from src.novarota.transformacao.prata import executar_prata

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"
preparar_unity_catalog(spark, config)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. O motor de qualidade + quarentena (demonstração)
# MAGIC Antes de rodar a Prata inteira, eu gosto de mostrar como o `aplicar_regras`
# MAGIC funciona numa entidade. Pego `bronze.clientes`, tiro os metadados técnicos e
# MAGIC aplico as regras de cliente: quem falha vai para a quarentena com o
# MAGIC `motivo_quarentena` preenchido (na massa, o cliente com CPF `ABC123` e renda
# MAGIC negativa deve cair aqui).

# COMMAND ----------

clientes_bruto = spark.table(config.tabela(config.schema_bronze, "clientes")).drop(
    *[c for c in COLUNAS_METADADOS if c != "arquivo_origem"]
)
resultado = aplicar_regras(clientes_bruto, regras_clientes())
print("clientes válidos   :", resultado.validos.count())
print("clientes quarentena:", resultado.quarentena.count())
display(resultado.quarentena.select("id_cliente", "cpf", "renda", "motivo_quarentena"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Executa a Prata completa
# MAGIC `executar_prata` faz isso para as 6 entidades **na ordem certa** de
# MAGIC dependência (clientes → contas → cartões → transações → eventos → estornos),
# MAGIC aplicando qualidade, integridade referencial, SCD2 e o MERGE idempotente.

# COMMAND ----------

executar_prata(spark, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD Tipo 2 — histórico preservado
# MAGIC O cliente 1 muda de cidade/segmento entre as cargas. No SCD2 isso vira
# MAGIC **duas versões**: a antiga fechada (`flag_vigente = false`) e a nova aberta
# MAGIC até `9999-12-31`. É assim que a Ouro consegue casar cada transação com o
# MAGIC cadastro **vigente na data**.

# COMMAND ----------

clientes = spark.table(config.tabela(config.schema_prata, "clientes"))
display(
    clientes.select(
        "id_cliente", "cidade", "segmento",
        "data_inicio_vigencia", "data_fim_vigencia", "flag_vigente",
    ).orderBy("id_cliente", "data_inicio_vigencia")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicação de transações
# MAGIC O mesmo `id_transacao` aparece em cargas diferentes (ex.: T0004) e há
# MAGIC duplicata exata (T0001). Na Prata a transação fica **única** por
# MAGIC `id_transacao`. Confirmo que não sobrou duplicata:

# COMMAND ----------

transacoes = spark.table(config.tabela(config.schema_prata, "transacoes"))
duplicadas = (
    transacoes.groupBy("id_transacao").count().where(F.col("count") > 1).count()
)
print("transações na Prata     :", transacoes.count())
print("id_transacao duplicados :", duplicadas)
assert duplicadas == 0, "há id_transacao duplicado na Prata!"
display(transacoes.orderBy("id_transacao"))
