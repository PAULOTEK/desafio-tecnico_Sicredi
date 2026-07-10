-- =====================================================================
-- DDL: catalogo e schemas da arquitetura Medallion.
-- Em Databricks/Unity Catalog usariamos: CREATE CATALOG novarota; e o
-- padrao de tres niveis catalog.schema.tabela. No metastore local o
-- catalogo e implicito (spark_catalog), portanto criamos apenas os schemas.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS bronze
  COMMENT 'Dados brutos ingeridos (append-only), formato Delta';

CREATE DATABASE IF NOT EXISTS prata
  COMMENT 'Dados limpos, tipados, historizados (SCD2) e em quarentena';

CREATE DATABASE IF NOT EXISTS ouro
  COMMENT 'Modelo analitico: fato, dimensoes e visoes para consumo';
