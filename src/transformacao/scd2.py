"""Historizacao de dimensoes com SCD Tipo 2 sobre Delta Lake.

Decisao de modelagem
--------------------
Para *clientes*, *contas* e *cartoes* aplicamos **SCD Tipo 2**, preservando o
historico completo de versoes. Isso e essencial para o desafio porque:

* precisamos refletir o cadastro vigente **na data da transacao** (histórico
  temporal);
* cartoes cancelados devem sair das metricas futuras mas manter o historico;
* auditoria/prevencao a perdas exige rastrear mudancas de status e limite.

Estrategia de construcao (idempotente)
--------------------------------------
O CDC completo fica preservado na Bronze/Prata. A cada execucao reconstruimos o
historico a partir de **todas** as versoes conhecidas usando funcoes de janela
(``ROW_NUMBER`` para deduplicar, ``LEAD`` para fechar a vigencia) e
materializamos o resultado via ``MERGE INTO`` por *surrogate key*. Como a chave
substituta e deterministica (hash de chave_negocio + inicio de vigencia), rodar
novamente a mesma carga nao gera duplicatas — garantindo idempotencia e o
tratamento correto de **dados atrasados** (uma versao que chega fora de ordem
apenas reordena as vigencias no proximo processamento).
"""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from src.common.logging_config import obter_logger
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = obter_logger("novarota.scd2")

# Fim de vigencia "aberto" (registro vigente). Evita nulos em joins temporais.
DATA_FIM_ABERTA = "9999-12-31 23:59:59"


@dataclass(frozen=True)
class DefinicaoSCD2:
    """Configura como uma dimensao sera historizada."""

    chave_negocio: str  # chave natural (ex.: id_cliente)
    colunas_atributos: list[str]  # atributos rastreados (mudanca => nova versao)
    coluna_data: str = "data_atualizacao"  # ordena as versoes
    coluna_operacao: str = "operacao"  # I/U/D


def construir_historico(df_cdc: DataFrame, definicao: DefinicaoSCD2) -> DataFrame:
    """Constroi o historico SCD2 a partir do CDC completo de uma entidade.

    Colunas adicionadas:
      * ``sk`` — surrogate key deterministica;
      * ``data_inicio_vigencia`` / ``data_fim_vigencia``;
      * ``flag_vigente`` — indica a versao corrente;
      * ``hash_atributos`` — hash dos atributos rastreados.
    """

    chave = definicao.chave_negocio
    data = definicao.coluna_data
    operacao = definicao.coluna_operacao

    # 1) Remove duplicatas exatas de (chave, data_atualizacao), mantendo 1 versao.
    janela_dedup = Window.partitionBy(chave, data).orderBy(F.col(operacao))
    df = (
        df_cdc.withColumn("_rn_dedup", F.row_number().over(janela_dedup))
        .where(F.col("_rn_dedup") == 1)
        .drop("_rn_dedup")
    )

    # 2) Ordena versoes por data e calcula o fim de vigencia com LEAD.
    janela_versao = Window.partitionBy(chave).orderBy(F.col(data).asc())
    df = (
        df.withColumn(
            "hash_atributos",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("<nulo>"))
                      for c in definicao.colunas_atributos],
                ),
                256,
            ),
        )
        .withColumn("data_inicio_vigencia", F.col(data).cast("timestamp"))
        .withColumn(
            "_proximo_inicio",
            F.lead(F.col(data).cast("timestamp")).over(janela_versao),
        )
        .withColumn("_rn_final", F.row_number().over(
            janela_versao.orderBy(F.col(data).desc())))
    )

    # 3) Define fim de vigencia e flag_vigente.
    #    - versao intermediaria: fecha no inicio da proxima versao;
    #    - versao mais recente e nao deletada: permanece aberta e vigente;
    #    - versao mais recente deletada (D): fecha na propria data e nao vigente.
    eh_ultima = F.col("_rn_final") == 1
    eh_delete = F.col(operacao) == "D"

    df = (
        df.withColumn(
            "data_fim_vigencia",
            F.when(F.col("_proximo_inicio").isNotNull(), F.col("_proximo_inicio"))
            .when(eh_ultima & eh_delete, F.col("data_inicio_vigencia"))
            .otherwise(F.lit(DATA_FIM_ABERTA).cast("timestamp")),
        )
        .withColumn("flag_vigente", eh_ultima & ~eh_delete)
        .withColumn(
            "sk",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col(chave).cast("string"),
                    F.col("data_inicio_vigencia").cast("string"),
                ),
                256,
            ),
        )
        .drop("_proximo_inicio", "_rn_final")
    )

    return df


def materializar_scd2(
    spark: SparkSession,
    df_historico: DataFrame,
    tabela: str,
) -> None:
    """Materializa o historico via MERGE INTO por surrogate key (idempotente)."""

    # Evita spark.catalog.tableExists() (nao suportado no Serverless): tenta abrir
    # a tabela Delta; se nao existir, cria na primeira carga.
    try:
        alvo = DeltaTable.forName(spark, tabela)
    except Exception:  # noqa: BLE001 - tabela ainda nao existe
        logger.info("scd2 criando_tabela=%s", tabela)
        df_historico.write.format("delta").mode("overwrite").saveAsTable(tabela)
        return

    (
        alvo.alias("destino")
        .merge(df_historico.alias("origem"), "destino.sk = origem.sk")
        .whenMatchedUpdateAll()  # atualiza vigencia de versoes ja conhecidas
        .whenNotMatchedInsertAll()  # insere versoes novas
        .execute()
    )
    logger.info("scd2 merge_concluido tabela=%s", tabela)
