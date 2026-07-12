# Data Transacional — Cooperativa NovaRota

Lakehouse em arquitetura **Medallion** (Bronze → Prata → Ouro) sobre **Delta
Lake**, construído para análise de comportamento transacional, prevenção de
perdas e consumo por Data Science. Implementado em **PySpark + Delta Lake** (o
mesmo motor do Databricks), com ingestão incremental, qualidade de dados,
histórico de dimensões (SCD Tipo 2), SQL avançado, testes e documentação das
decisões técnicas.



## Sumário

- [Arquitetura](#arquitetura)
- [Modelo de dados](#modelo-de-dados)
- [Stack](#stack)
- [Executando no Databricks](#executando-no-databricks)
- [Execução ponta a ponta](#execução-ponta-a-ponta)
- [Consultas SQL](#consultas-sql-avançadas)
- [Testes e lint](#testes-e-lint)
- [Modelo de dados (Ouro)](#modelo-de-dados-ouro)
- [Idempotência e reprocessamento](#idempotência-e-reprocessamento)
- [Evidências de execução](#evidências-de-execução)
- [Premissas e limitações](#premissas-e-limitações)
- [Próximos passos](#próximos-passos)

## Arquitetura

![Arquitetura](https://raw.githubusercontent.com/PAULOTEK/desafio-tecnico_Sicredi/main/docs/diagramas/img.png)

## Modelo de dados

![Modelo de Dados](https://raw.githubusercontent.com/PAULOTEK/desafio-tecnico_Sicredi/main/docs/diagramas/modelagem_do_dado.png)

## Stack

| Camada | Tecnologia |
|---|---|
| Plataforma | Databricks Serverless + Unity Catalog |
| Processamento | Apache Spark 3.5 (PySpark) |
| Formato / ACID | Delta Lake 3.2 |
| Linguagem | Python 3.10+ |
| Configuração | YAML + variáveis de ambiente |
| Testes / Lint | pytest, ruff |
| CI/CD | GitHub Actions + Databricks Asset Bundle |

## Estrutura do projeto

```
desafio-engenharia-dados2/
├── config/config.yaml            # parametrização (catálogo, schemas, landing, modo)
├── databricks.yml                # Databricks Asset Bundle (Job Serverless)
├── docs/                         # arquitetura + decisões técnicas + diagramas
├── evidencias/                   # logs/saídas reais de execução
├── notebooks/                    # camada de execução/demonstração (um por camada)
├── sql/
│   ├── ddl/                      # criação de schemas
│   └── analytics/                # SQL avançado (CTEs, window functions, MERGE)
├── src/novarota/
│   ├── config.py                 # configuração parametrizável
│   ├── common/                   # ambiente (Unity Catalog), logging, metadados
│   ├── ingestao/                 # gerador de massa + camada Bronze
│   ├── qualidade/                # regras de qualidade + quarentena
│   └── transformacao/            # SCD2, camada Prata, camada Ouro
├── tests/                        # testes unitários e de transformação
├── requirements.txt / pyproject.toml
└── .github/workflows/            # CI (lint+testes) + CD (pipeline no Databricks)
```


## Executando no Databricks

Ambiente-alvo: **Serverless + Unity Catalog** (também roda em cluster **DBR 15.x
LTS**).Serve em
workspace corporativo, trial ou **Free Edition**.

1. **Compute**: **Serverless** ou cluster **DBR 15.x LTS** (Spark 3.5 + Delta).
2. **Código**: Workspace → **Repos** → *Add Repo* → cole a URL deste repositório
   (ou **Git → Pull** se já clonou).
3. **Executar**: rode os notebooks **na ordem**, um por camada. Cada um localiza
   o pacote `novarota` sozinho (adiciona `src` ao `sys.path`, sem `%pip`) e
   prepara o Unity Catalog:
   `00_setup_e_massa` → `01_bronze` → `02_prata` → `03_ouro` → `04_analytics_sql`.

   **Detalhes de compatibilidade com Serverless/UC:**
   - Usa o `spark` **nativo** do notebook; nunca cria sessão manual nem acessa
     `sparkContext`/`_jvm` (indisponíveis no Serverless/Spark Connect).
   - Tabelas **gerenciadas pelo Unity Catalog** (`saveAsTable`). Sem *external
     location*; o único Volume é o `landing`, só para os arquivos de entrada.
   - `preparar_unity_catalog` liga `config.usar_catalogo = True`, então as
     tabelas ficam totalmente qualificadas (`novarota.bronze.clientes`, …).
   - O `arquivo_origem` da Bronze usa `_metadata.file_path` (e não
     `input_file_name()`, não suportado no Spark Connect/Serverless).
4. **Validar / evidências** — os prints destas células comprovam a execução:
   ```sql
   SELECT count(*) FROM novarota.ouro.gold_fato_transacao;                 -- 10
   SELECT id_cliente, count(*) FROM novarota.prata.clientes GROUP BY 1;    -- versões SCD2
   SELECT id_transacao, valor, valor_liquido, flag_cartao_cancelado
   FROM   novarota.ouro.gold_fato_transacao ORDER BY id_transacao;         -- T0003=0, T0021=2800, T0022 cancelado
   ```

> **Guia completo passo a passo** (Databricks Serverless e CI/CD):
> [`docs/como-rodar.md`](docs/como-rodar.md).

Parametrização (sobrescreve `config/config.yaml`) por variáveis de ambiente
`NOVAROTA_*`, por exemplo:

```bash
export NOVAROTA_MODO_EXECUCAO=incremental
export NOVAROTA_DATA_REFERENCIA=2024-03-01
```

## Consultas SQL avançadas

O notebook `notebooks/04_analytics_sql.py` executa as consultas; os arquivos-fonte
ficam em [`sql/analytics/`](sql/analytics) e podem ser rodados no editor SQL do
Databricks. Cobrem os requisitos obrigatórios:

| Arquivo | Técnica |
|---|---|
| `01_registro_vigente_row_number.sql` | `ROW_NUMBER` (registro vigente/dedup) |
| `02_comparativo_mensal_lag_lead.sql` | `LAG` / `LEAD` (comparação entre períodos) |
| `03_primeiro_ultimo_comportamento.sql` | `FIRST_VALUE` / `LAST_VALUE` |
| `04_segmentacao_ntile_percentrank.sql` | `NTILE` / `PERCENT_RANK` |
| `05_anomalias_transacionais.sql` | Detecção de anomalias (z-score) |
| `06_cliente_vs_historico_e_cidade_segmento.sql` | Cliente vs próprio histórico **e** vs cidade/segmento |
| `07_merge_incremental.sql` | `MERGE INTO` incremental idempotente |

Todas usam **CTEs encadeadas**. Há também `MERGE` em PySpark/Delta na
historização SCD2 (`src/novarota/transformacao/scd2.py`).

## CI/CD (GitHub Actions)

Dois workflows:

- **`.github/workflows/ci.yml`** — roda em todo push/PR: `lint` (ruff) →
  `testes` (pytest + Spark) → `validar-bundle` (YAML do Asset Bundle/workflows).
- **`.github/workflows/databricks.yml`** — a **cada push na `main`** (ou disparo
  manual) publica o **Databricks Asset Bundle** (`databricks.yml`) e **executa o
  Job** do pipeline Medallion no Databricks (Serverless), com uma task por camada
  encadeada: `setup → bronze → prata → ouro → analytics`.

Para o workflow do Databricks funcionar, cadastre em **Settings → Secrets and
variables → Actions** do repositório:

| Secret | Descrição |
|---|---|
| `DATABRICKS_HOST` | URL do workspace (ex.: `https://dbc-xxxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Personal Access Token do workspace (User Settings → Developer → Access tokens) |

O Job é definido pelo Asset Bundle e roda em **compute Serverless** (nenhum
cluster é declarado). Cada notebook importa o pacote `novarota` via `sys.path` a
partir de `${workspace.file_path}/src` (parâmetro `bundle_root`), sem `%pip`.

## Testes e lint

As regras de negócio, qualidade e SCD2 ficam no pacote `src/novarota` e são
cobertas por testes automatizados (rodam no CI, sem cluster):

```bash
python3 -m venv .venv
source .venv/bin/activate            
pip install -r requirements.txt
pytest -q                          
ruff check src tests conftest.py     
```

As fixtures sobem um Spark local com Delta apenas para exercitar a lógica das
transformações com massa pequena — é um harness de desenvolvimento, não o
caminho de execução do produto. Os testes cobrem regras de qualidade/quarentena,
metadados/hash, construção do SCD2 (vigências, delete, dedup, surrogate key) e
agregações da camada Ouro (exclusão de cartão cancelado, valor líquido,
indicadores de risco).

## Modelo de dados (Ouro)

| Tabela | Grão | Chave |
|---|---|---|
| `gold_fato_transacao` | 1 linha por transação válida | `id_transacao` |
| `gold_dim_cliente` / `gold_dim_conta` / `gold_dim_cartao` | 1 linha por entidade vigente | `id_*` |
| `gold_dim_estabelecimento` | estabelecimento + mcc | `id_estabelecimento` |
| `gold_cliente_mes` | cliente × ano_mes | (`id_cliente`, `ano_mes`) |
| `gold_indicadores_risco` | cliente × ano_mes | (`id_cliente`, `ano_mes`) |
| `gold_features_cliente` | 1 linha por cliente | `id_cliente` |

Regras de negócio: transações em **cartão cancelado na data** não compõem
métricas futuras (mas ficam no fato); **transações estornadas não somam no valor
líquido**; cadastro refletido é o **vigente na data da transação**.

## Idempotência e reprocessamento

- **Bronze**: tabela de controle de arquivos processados → reexecutar não
  duplica dados.
- **Prata (SCD2)**: `MERGE` por *surrogate key* determinística → histórico
  reconstruído de forma estável; **dados atrasados** apenas reordenam vigências.
- **Prata (fatos)** e **Ouro**: recomputados a partir das camadas anteriores;
  `MERGE`/overwrite garantem estado consistente a cada run.

Reexecutar o pipeline inteiro produz exatamente o mesmo resultado.

## Evidências de execução

Logs e amostras reais em [`evidencias/`](evidencias/README.md): execução do
pipeline, resultado das consultas SQL, amostras das tabelas e saída de
testes/lint.

## Premissas e limitações

- **Massa sintética** gerada por código (`src/novarota/ingestao/gerador_dados.py`),
  simulando os problemas de qualidade pedidos (duplicidade, CDC, dados atrasados,
  evolução de schema, integridade quebrada). Nenhum dado real, credencial ou
  token é utilizado.
- Adicionamos o campo `valor_estorno` em `estornos` para distinguir estorno
  parcial de total (ver [decisões técnicas](docs/decisoes-tecnicas.md)).
- A ingestão incremental usa tabela de controle no lugar do Auto Loader
  (justificado nas decisões técnicas).


## Sobre Reprocessamento

- **Bronze**: tabela de controle de arquivos processados → reexecutar não
  duplica dados.
- **Prata**: `MERGE` por *surrogate key* determinística → histórico
  reconstruído de forma estável; **dados atrasados** apenas reordenam vigências.
- **Ouro**: recomputados a partir das camadas anteriores;
  `MERGE`/overwrite garantem estado consistente a cada run.

