# Databricks notebook source
# MAGIC %md
# MAGIC # Consultas SQL — NovaRota
# MAGIC

# MAGIC do Databricks.

# COMMAND ----------

# MAGIC %md ## Registro vigente por cliente (ROW_NUMBER)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH versoes AS (
# MAGIC   SELECT id_cliente, nome, cidade, segmento, renda, data_inicio_vigencia,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_inicio_vigencia DESC) AS rn
# MAGIC   FROM prata.clientes
# MAGIC )
# MAGIC SELECT id_cliente, nome, cidade, segmento, renda FROM versoes WHERE rn = 1 ORDER BY id_cliente;

# COMMAND ----------

# MAGIC %md ## Comparativo mensal (LAG/LEAD)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (SELECT id_cliente, ano_mes, valor_liquido_total FROM ouro.gold_cliente_mes)
# MAGIC SELECT id_cliente, ano_mes, valor_liquido_total,
# MAGIC        LAG(valor_liquido_total) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS mes_anterior
# MAGIC FROM base ORDER BY id_cliente, ano_mes;

# COMMAND ----------

# MAGIC %md ## Segmentação por percentil (NTILE / PERCENT_RANK)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH gasto AS (
# MAGIC   SELECT id_cliente, SUM(valor_liquido_total) AS valor FROM ouro.gold_cliente_mes GROUP BY id_cliente
# MAGIC )
# MAGIC SELECT id_cliente, valor,
# MAGIC        NTILE(4) OVER (ORDER BY valor) AS quartil,
# MAGIC        ROUND(PERCENT_RANK() OVER (ORDER BY valor), 4) AS percentil
# MAGIC FROM gasto ORDER BY valor DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Consultas completas (anomalias, primeiro/último comportamento, comparação
# MAGIC cliente vs histórico e cidade/segmento, MERGE incremental) em
# MAGIC `sql/analytics/`.
