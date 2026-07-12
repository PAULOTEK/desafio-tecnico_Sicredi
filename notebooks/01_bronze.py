# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze
# MAGIC
# MAGIC  ingestão dos arquivos da `landing` para tabelas Delta **de forma
# MAGIC incremental e idempotente**, preservando o dado bruto e anexando metadados
# MAGIC técnicos. A lógica de verdade mora no pacote `novarota.ingestao.bronze`
# MAGIC (testável); neste notebook eu **executo e mostro** cada etapa acontecendo.
# MAGIC
# MAGIC Pré-requisito: ter rodado o `00_setup_e_massa`.

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
from novarota.ingestao.bronze import (
    FONTES_PADRAO,
    executar_bronze,
    ingerir_fonte,
)

# display() existe no Databricks.
# eu troco por show() para o notebook rodar igual no ambiente local.
try:
    display  # noqa: F821
except NameError:
    def display(df, n: int = 20):
        df.show(n, truncate=False)

# COMMAND ----------

config = Config.carregar()
config.modo_execucao = "full"
spark = obter_spark("novarota-bronze", config)

# No Databricks liga o Unity Catalog e aponta o landing para o Volume.
try:
    dbutils  # noqa: F821
    preparar_unity_catalog(spark, config)
except NameError:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. O que tem na landing?
# MAGIC Antes de ingestão eu gosto de olhar o que chegou: são 6 fontes (clientes,
# MAGIC contas, cartões em JSON estilo CDC; transações, eventos e estornos em CSV),
# MAGIC divididas em duas cargas para eu conseguir exercitar o incremental.

# COMMAND ----------

for fonte in FONTES_PADRAO:
    base = config.dir_landing / fonte.subpasta
    arquivos = sorted(base.rglob(f"*.{fonte.formato}")) if base.exists() else []
    print(f"{fonte.nome:15s} formato={fonte.formato:4s} arquivos={len(arquivos)}")
    for arq in arquivos:
        print("   -", arq.relative_to(config.dir_landing))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestão incremental de uma fonte
# MAGIC Faço primeiro só as `transacoes` para enxergar o mecanismo: `ingerir_fonte`
# MAGIC lê apenas os arquivos ainda **não** registrados em `bronze._controle_ingestao`,
# MAGIC anexa os metadados (`arquivo_origem`, `hash_linha`, `batch_id`, ...) e grava
# MAGIC na tabela Delta.

# COMMAND ----------

linhas = ingerir_fonte(spark, config, FONTES_PADRAO[3])  # transacoes
print("linhas ingeridas de transacoes nesta execução:", linhas)
display(spark.table(config.tabela(config.schema_bronze, "transacoes")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingestão de todas as fontes
# MAGIC Agora rodo o `executar_bronze`, que repete esse fluxo para as 6 fontes.
# MAGIC (As `transacoes` já foram ingeridas acima, então aparecem como `nada_novo` —
# MAGIC exatamente a idempotência funcionando.)

# COMMAND ----------

resumo = executar_bronze(spark, config)
print("Bronze — linhas ingeridas por fonte nesta execução:")
for fonte, qtd in resumo.items():
    print(f"  {fonte:15s} {qtd}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tabela de controle = idempotência
# MAGIC Cada arquivo lido fica registrado aqui. É isso que garante que reprocessar
# MAGIC a mesma carga **não duplica** dados (em produção o Auto Loader/`cloudFiles`
# MAGIC faria esse controle via checkpoint — ver `docs/decisoes-tecnicas.md`).

# COMMAND ----------

display(spark.table(config.tabela(config.schema_bronze, "_controle_ingestao")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Prova de idempotência
# MAGIC Se eu rodar a Bronze de novo sem chegar arquivo novo, tudo deve dar
# MAGIC `0` (nada_novo). É a checagem que faço sempre antes de confiar no incremental.

# COMMAND ----------

resumo2 = executar_bronze(spark, config)
print("Reexecução (esperado tudo 0):", resumo2)
assert all(v == 0 for v in resumo2.values()), "idempotência violada: reingeriu algo!"
print("OK — reexecução não ingeriu nada novo.")
