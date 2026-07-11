# Como rodar o pipeline

Guia ponta a ponta para executar o data product NovaRota em **dois ambientes**:

1. [Execução local](#1-execucao-local) (PySpark + Delta Lake na sua máquina);
2. [Databricks Serverless + Unity Catalog](#2-databricks-serverless--unity-catalog);
3. [Execução automática via CI/CD](#3-cicd-execucao-automatica-no-databricks).

O **mesmo código** roda nos dois lugares — só mudam a sessão Spark (nativa no
Databricks) e a qualificação dos nomes de tabela (2 níveis local, 3 níveis no
Unity Catalog). Nenhuma regra de negócio muda.

---

## 1. Execução local

### Pré-requisitos
- **Python 3.10+**
- **Java 17** (exigido pelo Spark) — confira com `java -version`.
- Internet na 1ª execução (o Spark baixa os JARs do Delta via Maven; depois usa
  o cache `~/.ivy2`).

### Setup
```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

### Rodar tudo de uma vez (gera a massa sintética + Bronze → Prata → Ouro)
```bash
python -m novarota.jobs.pipeline --gerar-dados --modo full
```

### Rodar camada por camada (útil para depurar/agendar)
```bash
python -m novarota.jobs.gerar_dados_job          # gera a massa em data/landing
python -m novarota.jobs.bronze_job --modo full   # ingestão Bronze (Delta)
python -m novarota.jobs.prata_job                # limpeza, qualidade, SCD2
python -m novarota.jobs.ouro_job                 # fato, dimensões, features
python -m novarota.jobs.analytics_job            # executa os SQL de sql/analytics
```

### Parâmetros (sobrescrevem `config/config.yaml`)
| Parâmetro | Descrição |
|---|---|
| `--config` | Caminho de um YAML alternativo |
| `--modo` | `full` ou `incremental` |
| `--data-referencia` | Data de referência (`YYYY-MM-DD`) |
| `--batch-id` | Identificador do batch |

Também dá para parametrizar por ambiente (prefixo `NOVAROTA_`):
```bash
export NOVAROTA_MODO_EXECUCAO=incremental
export NOVAROTA_DATA_REFERENCIA=2024-03-01
```

### Testes e lint
```bash
pytest -q                                  # 19 testes (unitários + transformação)
ruff check src tests conftest.py           # lint
```

### O que esperar (evidências)
- `gold_fato_transacao` = **10** linhas;
- clientes/contas/cartões com **múltiplas versões** (SCD2);
- estorno **total** (T0003) → `valor_liquido = 0`; estorno **parcial** (T0021)
  → `valor_liquido = 2800`; cartão **cancelado** (T0022) fora das métricas
  mensais, mas preservado no fato;
- **idempotência**: rodar de novo sem novos arquivos loga `nada_novo` e o fato
  continua com 10 linhas.

---

## 2. Databricks Serverless + Unity Catalog

> Serve em workspace corporativo, trial ou **Free Edition**. Não é preciso
> instalar Spark/Delta — já vêm no runtime.

### Passo 1 — Subir o código
Workspace → **Repos** → *Add Repo* → cole a URL deste repositório e faça o
clone. (Se já clonou antes, use **Git → Pull** para trazer a versão mais nova.)

### Passo 2 — Compute
Selecione **Serverless** (ou um cluster **DBR 15.x LTS**, que traz Spark 3.5 +
Delta nativos).

### Passo 3 — Executar o pipeline (um notebook por camada, na ordem)
Há **um notebook por camada** — rode-os na sequência abaixo. Cada um localiza o
pacote `novarota` (adiciona `src` ao `sys.path`, **sem** `%pip`) e, ao detectar o
Databricks, prepara o Unity Catalog (liga `usar_catalogo`, cria
catálogo/schemas/Volume `landing`, faz `USE CATALOG` e aponta o *landing* para o
Volume) via `novarota.common.ambiente.preparar_unity_catalog`.

| Ordem | Notebook | O que faz |
|---|---|---|
| 1 | `notebooks/00_setup_e_massa.py` | Prepara o Unity Catalog e gera a massa sintética no Volume |
| 2 | `notebooks/01_bronze.py` | Ingestão incremental/idempotente → tabelas Delta Bronze |
| 3 | `notebooks/02_prata.py` | Limpeza, qualidade/quarentena e SCD Tipo 2 (MERGE) |
| 4 | `notebooks/03_ouro.py` | Fato, dimensões e visões analíticas |
| 5 | `notebooks/04_analytics_sql.py` | Consultas SQL avançadas |

> Rodando via **Job/Asset Bundle** essa ordem é garantida pelo `depends_on`
> (`setup → bronze → prata → ouro → analytics`).

### Passo 4 — Validar (prints = evidência de execução)
```sql
SELECT count(*) FROM novarota.ouro.gold_fato_transacao;                 -- 10
SELECT id_cliente, count(*) FROM novarota.prata.clientes GROUP BY 1;    -- versões SCD2
SELECT id_transacao, valor, valor_liquido, flag_cartao_cancelado
FROM   novarota.ouro.gold_fato_transacao ORDER BY id_transacao;         -- T0003=0, T0021=2800, T0022 cancelado
```

### Por que funciona no Serverless (decisões de compatibilidade)
| Tema | Decisão |
|---|---|
| Sessão Spark | Usa o `spark` **nativo** do notebook (`obter_spark` reaproveita a sessão ativa). **Nunca** `criar_spark`/`sparkContext` — proibidos no Serverless (Spark Connect). |
| Armazenamento | Tabelas **gerenciadas** pelo Unity Catalog (`saveAsTable`). Sem *external location*; o único Volume é o `landing`, só para os arquivos de entrada. |
| Nomes de tabela | Flag `usar_catalogo`: `novarota.schema.tabela` no Databricks; `schema.tabela` local (metastore local não tem catálogo de 3 níveis). |
| `arquivo_origem` | Coluna oculta `_metadata.file_path` (o `input_file_name()` não é suportado no Spark Connect/Serverless). |
| Existência de tabela | `try/except DeltaTable.forName` no lugar de `spark.catalog.tableExists()` (não é permitido no Serverless). |

### Setup manual do Unity Catalog (opcional)
O notebook já cria tudo, mas se quiser preparar o ambiente antes (SQL editor):
```sql
CREATE CATALOG IF NOT EXISTS novarota;
CREATE SCHEMA  IF NOT EXISTS novarota.bronze;
CREATE SCHEMA  IF NOT EXISTS novarota.prata;
CREATE SCHEMA  IF NOT EXISTS novarota.ouro;
CREATE VOLUME  IF NOT EXISTS novarota.bronze.landing;   -- arquivos de entrada
```

---

## 3. CI/CD: execução automática no Databricks

Dois workflows do **GitHub Actions**:

- **`.github/workflows/ci.yml`** — em todo push/PR: `lint` (ruff) → `testes`
  (pytest) → `validar-bundle` (YAML).
- **`.github/workflows/databricks.yml`** — a **cada push na `main`** (ou disparo
  manual): publica o **Databricks Asset Bundle** (`databricks.yml`) e **executa
  o Job** do pipeline no workspace Serverless — uma task por camada encadeada:
  `setup → bronze → prata → ouro → analytics`.

### Secrets necessários
Em **Settings → Secrets and variables → Actions** do repositório:

| Secret | Descrição |
|---|---|
| `DATABRICKS_HOST` | URL do workspace (ex.: `https://dbc-xxxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Personal Access Token (User Settings → Developer → Access tokens) |

### Rodar o bundle manualmente (opcional, via Databricks CLI)
```bash
export DATABRICKS_HOST=...   DATABRICKS_TOKEN=...
databricks bundle validate -t prod
databricks bundle deploy   -t prod
databricks bundle run novarota_pipeline_medallion -t prod
```

Fluxo automático resumido:

```
push na main → GitHub Actions → bundle validate → deploy → run
            → Job Serverless: Bronze → Prata → Ouro → analytics SQL
```
