-- =====================================================================
-- =====================================================================

CREATE DATABASE IF NOT EXISTS bronze
  COMMENT 'Dados brutos ingeridos (append-only), formato Delta';

CREATE DATABASE IF NOT EXISTS prata
  COMMENT 'Dados limpos, tipados, historizados (SCD2) e em quarentena';

CREATE DATABASE IF NOT EXISTS ouro
  COMMENT 'Modelo analitico: fato, dimensoes e visoes para consumo';
