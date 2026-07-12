"""Camada Prata: limpeza, tipagem, qualidade, quarentena e historizacao.

Responsabilidades:
* tipar e padronizar os dados brutos vindos da Bronze;
* aplicar regras de qualidade e segregar invalidos em quarentena;
* deduplicar (inclusive ``id_transacao`` repetido entre cargas);
* validar integridade referencial entre entidades;
* historizar clientes/contas/cartoes com SCD Tipo 2;
* materializar transacoes/eventos/estornos via MERGE idempotente.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from novarota.common.logging_config import obter_logger
from novarota.common.metadados import COLUNAS_METADADOS
from novarota.config import Config
from novarota.qualidade import regras
from novarota.qualidade.regras import ResultadoQualidade, aplicar_regras
from novarota.transformacao.scd2 import (
    DefinicaoSCD2,
    construir_historico,
    materializar_scd2,
)
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = obter_logger("novarota.prata")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _garantir_schema(spark: SparkSession, config: Config) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema_qualificado(config.schema_prata)}")


def _sem_metadados_bronze(df: DataFrame) -> DataFrame:
    """Remove colunas tecnicas da Bronze, preservando dado de negocio."""

    return df.drop(*[c for c in COLUNAS_METADADOS if c in df.columns])


def _tipar(df: DataFrame, casts: dict[str, str]) -> DataFrame:
    for coluna, tipo in casts.items():
        if coluna in df.columns:
            df = df.withColumn(coluna, F.col(coluna).cast(tipo))
    return df


def _gravar_quarentena(
    spark: SparkSession, config: Config, entidade: str, df: DataFrame
) -> int:
    tabela = config.tabela(config.schema_prata, f"quarentena_{entidade}")
    (
        df.withColumn("entidade", F.lit(entidade))
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela)
    )
    qtd = df.count()
    logger.info("quarentena entidade=%s registros=%d tabela=%s", entidade, qtd, tabela)
    return qtd


def _upsert_por_chave(
    spark: SparkSession, df: DataFrame, tabela: str, chave: str
) -> None:
    """MERGE idempotente por chave (usado nas tabelas nao-SCD2)."""

    # Evita spark.catalog.tableExists() (nao suportado no Serverless).
    try:
        alvo = DeltaTable.forName(spark, tabela)
    except Exception:  # noqa: BLE001 - tabela ainda nao existe
        df.write.format("delta").mode("overwrite").saveAsTable(tabela)
        return
    (
        alvo.alias("d")
        .merge(df.alias("o"), f"d.{chave} = o.{chave}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# --------------------------------------------------------------------------- #
# Dimensoes SCD2: clientes, contas, cartoes
# --------------------------------------------------------------------------- #
_CASTS_CLIENTES = {
    "id_cliente": "int", "cpf": "string", "nome": "string", "cidade": "string",
    "estado": "string", "renda": "double", "segmento": "string",
    "data_atualizacao": "timestamp", "operacao": "string",
}
_CASTS_CONTAS = {
    "id_conta": "int", "id_cliente": "int", "tipo_conta": "string",
    "status_conta": "string", "data_abertura": "date",
    "data_atualizacao": "timestamp", "operacao": "string",
}
_CASTS_CARTOES = {
    "id_cartao": "int", "id_conta": "int", "tipo_cartao": "string",
    "limite": "double", "status_cartao": "string",
    "data_atualizacao": "timestamp", "operacao": "string",
}


def _processar_dimensao(
    spark: SparkSession,
    config: Config,
    *,
    entidade: str,
    casts: dict[str, str],
    conjunto_regras: list,
    definicao: DefinicaoSCD2,
    condicao_ri: Column | None = None,
    motivo_ri: str | None = None,
) -> None:
    bronze = config.tabela(config.schema_bronze, entidade)
    df = _tipar(_sem_metadados_bronze(spark.table(bronze)), casts)

    # Qualidade estrutural.
    resultado: ResultadoQualidade = aplicar_regras(df, conjunto_regras)
    validos, quarentena = resultado.validos, resultado.quarentena

    # Integridade referencial (opcional).
    if condicao_ri is not None:
        invalidos_ri = validos.where(~condicao_ri).withColumn(
            "motivo_quarentena", F.lit(motivo_ri)
        )
        validos = validos.where(condicao_ri)
        quarentena = quarentena.unionByName(invalidos_ri, allowMissingColumns=True)

    _gravar_quarentena(spark, config, entidade, quarentena)

    # Historizacao SCD2.
    historico = construir_historico(validos, definicao)
    tabela = config.tabela(config.schema_prata, entidade)
    materializar_scd2(spark, historico, tabela)
    logger.info("prata dimensao=%s versoes=%d", entidade, historico.count())


def processar_clientes(spark: SparkSession, config: Config) -> None:
    _processar_dimensao(
        spark, config,
        entidade="clientes",
        casts=_CASTS_CLIENTES,
        conjunto_regras=regras.regras_clientes(),
        definicao=DefinicaoSCD2(
            chave_negocio="id_cliente",
            colunas_atributos=["cpf", "nome", "cidade", "estado", "renda", "segmento"],
        ),
    )


def processar_contas(spark: SparkSession, config: Config) -> None:
    # RI: id_cliente deve existir em prata.clientes.
    clientes = spark.table(config.tabela(config.schema_prata, "clientes"))
    ids_cliente = clientes.select("id_cliente").distinct()
    condicao = F.col("id_cliente").isin(
        [r["id_cliente"] for r in ids_cliente.collect()]
    )
    _processar_dimensao(
        spark, config,
        entidade="contas",
        casts=_CASTS_CONTAS,
        conjunto_regras=regras.regras_contas(),
        definicao=DefinicaoSCD2(
            chave_negocio="id_conta",
            colunas_atributos=["id_cliente", "tipo_conta", "status_conta", "data_abertura"],
        ),
        condicao_ri=condicao,
        motivo_ri="cliente_inexistente",
    )


def processar_cartoes(spark: SparkSession, config: Config) -> None:
    # RI: id_conta deve existir em prata.contas (cartao sem conta valida -> quarentena).
    contas = spark.table(config.tabela(config.schema_prata, "contas"))
    ids_conta = [r["id_conta"] for r in contas.select("id_conta").distinct().collect()]
    condicao = F.col("id_conta").isin(ids_conta)
    _processar_dimensao(
        spark, config,
        entidade="cartoes",
        casts=_CASTS_CARTOES,
        conjunto_regras=regras.regras_cartoes(),
        definicao=DefinicaoSCD2(
            chave_negocio="id_cartao",
            colunas_atributos=["id_conta", "tipo_cartao", "limite", "status_cartao"],
        ),
        condicao_ri=condicao,
        motivo_ri="conta_inexistente",
    )


# --------------------------------------------------------------------------- #
# Transacoes (fato limpo, deduplicado por id_transacao)
# --------------------------------------------------------------------------- #
_CASTS_TRANSACOES = {
    "id_transacao": "string", "id_cartao": "int", "data_transacao": "date",
    "valor": "double", "mcc": "int", "estabelecimento": "string",
    "canal": "string", "pais": "string", "moeda": "string", "dispositivo": "string",
}


def processar_transacoes(spark: SparkSession, config: Config) -> None:
    bronze = config.tabela(config.schema_bronze, "transacoes")
    df = _tipar(_sem_metadados_bronze(spark.table(bronze)), _CASTS_TRANSACOES)

    # Qualidade estrutural (valor > 0, chaves nao nulas, moeda).
    resultado = aplicar_regras(df, regras.regras_transacoes())
    validos, quarentena = resultado.validos, resultado.quarentena

    # Dedup por id_transacao (mesma transacao pode vir em cargas diferentes).
    janela = Window.partitionBy("id_transacao").orderBy(F.col("valor").desc())
    validos = (
        validos.withColumn("_rn", F.row_number().over(janela))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    _gravar_quarentena(spark, config, "transacoes", quarentena)

    tabela = config.tabela(config.schema_prata, "transacoes")
    _upsert_por_chave(spark, validos, tabela, "id_transacao")
    logger.info("prata transacoes=%d", validos.count())


# --------------------------------------------------------------------------- #
# Eventos de risco e Estornos (com RI contra transacoes validas)
# --------------------------------------------------------------------------- #
_CASTS_EVENTOS = {
    "id_evento": "string", "id_transacao": "string", "tipo_evento": "string",
    "severidade": "string", "data_evento": "date",
}
_CASTS_ESTORNOS = {
    "id_estorno": "string", "id_transacao": "string", "valor_estorno": "double",
    "data_estorno": "date", "motivo": "string",
}


def _processar_vinculado_transacao(
    spark: SparkSession,
    config: Config,
    *,
    entidade: str,
    casts: dict[str, str],
    conjunto_regras: list,
    chave: str,
) -> None:
    bronze = config.tabela(config.schema_bronze, entidade)
    df = _tipar(_sem_metadados_bronze(spark.table(bronze)), casts)

    resultado = aplicar_regras(df, conjunto_regras)
    validos, quarentena = resultado.validos, resultado.quarentena

    # RI: id_transacao deve existir em prata.transacoes.
    transacoes = spark.table(config.tabela(config.schema_prata, "transacoes"))
    ids_tx = [r["id_transacao"] for r in transacoes.select("id_transacao").distinct().collect()]
    condicao = F.col("id_transacao").isin(ids_tx)

    invalidos_ri = validos.where(~condicao).withColumn(
        "motivo_quarentena", F.lit("transacao_inexistente")
    )
    validos = validos.where(condicao)
    quarentena = quarentena.unionByName(invalidos_ri, allowMissingColumns=True)

    _gravar_quarentena(spark, config, entidade, quarentena)

    tabela = config.tabela(config.schema_prata, entidade)
    _upsert_por_chave(spark, validos, tabela, chave)
    logger.info("prata %s=%d", entidade, validos.count())


def processar_eventos_risco(spark: SparkSession, config: Config) -> None:
    _processar_vinculado_transacao(
        spark, config,
        entidade="eventos_risco",
        casts=_CASTS_EVENTOS,
        conjunto_regras=regras.regras_eventos_risco(),
        chave="id_evento",
    )


def processar_estornos(spark: SparkSession, config: Config) -> None:
    _processar_vinculado_transacao(
        spark, config,
        entidade="estornos",
        casts=_CASTS_ESTORNOS,
        conjunto_regras=regras.regras_estornos(),
        chave="id_estorno",
    )


# --------------------------------------------------------------------------- #
# Orquestracao
# --------------------------------------------------------------------------- #
def executar_prata(spark: SparkSession, config: Config) -> None:
    logger.info("=== PRATA inicio batch_id=%s ===", config.batch_id)
    _garantir_schema(spark, config)
    # Ordem respeita dependencias de integridade referencial.
    processar_clientes(spark, config)
    processar_contas(spark, config)
    processar_cartoes(spark, config)
    processar_transacoes(spark, config)
    processar_eventos_risco(spark, config)
    processar_estornos(spark, config)
    logger.info("=== PRATA fim ===")
