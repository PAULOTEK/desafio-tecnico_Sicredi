# Como rodar o pipeline

Guia ponta a ponta para executar o data product NovaRota no **Databricks
Serverless + Unity Catalog** — o ambiente-alvo do projeto.

1. [Databricks Serverless + Unity Catalog](#1-databricks-serverless--unity-catalog);
2. [Execução automática via CI/CD](#2-cicd-execucao-automatica-no-databricks);
3. [Desenvolvimento e testes](#3-desenvolvimento-e-testes).

O código de negócio vive no pacote `src/novarota` (modular e testável); os
notebooks são a **camada de execução/demonstração** e usam o `spark` nativo do
Databricks.

---

## 1. Databricks Serverless + Unity Catalog

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
pacote `novarota` (adiciona `src` ao `sys.path`, **sem** `%pip`) e prepara o
Unity Catalog (liga `usar_catalogo`, cria catálogo/schemas/Volume `landing`, faz
`USE CATALOG` e aponta o *landing* para /*o Volume) via
`novarota.common.ambiente.preparar_unity_catalog`.

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
SELECT count(*) FROM novarota.ouro.gold_fato_transacao;                 
SELECT id_cliente, count(*) FROM novarota.prata.clientes GROUP BY 1;    
SELECT id_transacao, valor, valor_liquido, flag_cartao_cancelado
FROM   novarota.ouro.gold_fato_transacao ORDER BY id_transacao;         
```

### Por que funciona no Serverless (decisões de compatibilidade)
| Tema | Decisão |
|---|---|
| Sessão Spark | Usa o `spark` **nativo** do notebook. **Nunca** cria sessão manual nem acessa `sparkContext`/`_jvm` — proibidos no Serverless (Spark Connect). |
| Armazenamento | Tabelas **gerenciadas** pelo Unity Catalog (`saveAsTable`). Sem *external location*; o único Volume é o `landing`, só para os arquivos de entrada. |
| Nomes de tabela | Flag `usar_catalogo` (ligada por `preparar_unity_catalog`) qualifica as tabelas como `novarota.schema.tabela`. |
| `arquivo_origem` | Coluna oculta `_metadata.file_path` (o `input_file_name()` não é suportado no Spark Connect/Serverless). |
| Existência de tabela | `try/except DeltaTable.forName` no lugar de `spark.catalog.tableExists()` (não é permitido no Serverless). |

### Setup manual do Unity Catalog (opcional)
O notebook já cria tudo, mas se quiser preparar o ambiente antes (SQL editor):
```sql
CREATE CATALOG IF NOT EXISTS novarota;
CREATE SCHEMA  IF NOT EXISTS novarota.bronze;
CREATE SCHEMA  IF NOT EXISTS novarota.prata;
CREATE SCHEMA  IF NOT EXISTS novarota.ouro;
CREATE VOLUME  IF NOT EXISTS novarota.bronze.landing;   ,
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

## 2. CI/CD: execução automática no Databricks

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

---

## 3. Desenvolvimento e testes

As **regras de negócio, qualidade e SCD2** ficam no pacote `src/novarota` e são
cobertas por testes automatizados (rodam no CI, sem cluster):

```bash
python3 -m venv .venv
source .venv/bin/activate            
pip install -r requirements.txt
pytest -q                           
ruff check src tests conftest.py     
```

As fixtures de teste sobem um Spark local com Delta apenas para exercitar a
lógica das transformações com massa pequena — não é o caminho de execução do
produto, que roda no Databricks.
