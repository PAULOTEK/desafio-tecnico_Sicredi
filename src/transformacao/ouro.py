"""Camada Ouro: fato, dimensoes e visoes analiticas.

Modelagem (grao e chaves documentados em docs/arquitetura.md):

* ``gold_fato_transacao`` — grao: 1 linha por transacao valida. Enriquecida com
  os atributos cadastrais **vigentes na data da transacao** (SCD2), com o valor
  liquido (descontando estornos) e flags de risco.
* ``gold_dim_cliente`` / ``gold_dim_conta`` / ``gold_dim_cartao`` — visao vigente.
* ``gold_dim_estabelecimento`` — estabelecimento + mcc distintos.
* ``gold_cliente_mes`` — grao: cliente x ano_mes. Metricas comportamentais.
* ``gold_indicadores_risco`` — grao: cliente x ano_mes. Eventos, estornos e
  chargebacks.
* ``gold_features_cliente`` — grao: 1 linha por cliente vigente. Consumo por DS.

Regras de negocio aplicadas:
* transacoes em cartoes **cancelados na data** nao compoem metricas futuras
  (``flag_cartao_cancelado``), mas permanecem no fato para preservar historico;
* ``valor_liquido = valor - valor_estornado`` (>= 0), portanto transacoes
  totalmente estornadas nao somam no liquido.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.logging_config import obter_logger
from src.config import Config

logger = obter_logger("novarota.ouro")


def _garantir_schema(spark: SparkSession, config: Config) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema_qualificado(config.schema_ouro)}")


def _salvar(df: DataFrame, tabela: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela)
    )


def construir_fato_transacao(spark: SparkSession, config: Config) -> DataFrame:
    tx = spark.table(config.tabela(config.schema_prata, "transacoes"))
    cartoes = spark.table(config.tabela(config.schema_prata, "cartoes"))
    contas = spark.table(config.tabela(config.schema_prata, "contas"))
    clientes = spark.table(config.tabela(config.schema_prata, "clientes"))
    estornos = spark.table(config.tabela(config.schema_prata, "estornos"))
    eventos = spark.table(config.tabela(config.schema_prata, "eventos_risco"))

    ts = F.col("data_transacao").cast("timestamp")

    # --- Cartao vigente na data da transacao ---
    c = cartoes.alias("c")
    fato = (
        tx.alias("t")
        .join(
            c,
            (F.col("t.id_cartao") == F.col("c.id_cartao"))
            & (ts >= F.col("c.data_inicio_vigencia"))
            & (ts < F.col("c.data_fim_vigencia")),
            "left",
        )
        .select(
            "t.*",
            F.col("c.id_conta").alias("id_conta"),
            F.col("c.tipo_cartao").alias("tipo_cartao"),
            F.col("c.limite").alias("limite_cartao"),
            F.col("c.status_cartao").alias("status_cartao_vigente"),
        )
    )

    # --- Conta vigente na data ---
    co = contas.alias("co")
    fato = (
        fato.alias("f")
        .join(
            co,
            (F.col("f.id_conta") == F.col("co.id_conta"))
            & (ts >= F.col("co.data_inicio_vigencia"))
            & (ts < F.col("co.data_fim_vigencia")),
            "left",
        )
        .select(
            "f.*",
            F.col("co.id_cliente").alias("id_cliente"),
            F.col("co.tipo_conta").alias("tipo_conta"),
            F.col("co.status_conta").alias("status_conta_vigente"),
        )
    )

    # --- Cliente vigente na data ---
    cl = clientes.alias("cl")
    fato = (
        fato.alias("f")
        .join(
            cl,
            (F.col("f.id_cliente") == F.col("cl.id_cliente"))
            & (ts >= F.col("cl.data_inicio_vigencia"))
            & (ts < F.col("cl.data_fim_vigencia")),
            "left",
        )
        .select(
            "f.*",
            F.col("cl.segmento").alias("segmento_cliente"),
            F.col("cl.cidade").alias("cidade_cliente"),
            F.col("cl.estado").alias("estado_cliente"),
            F.col("cl.renda").alias("renda_cliente"),
        )
    )

    # --- Estornos agregados por transacao (evita fan-out no join) ---
    estornos_agg = estornos.groupBy("id_transacao").agg(
        F.sum("valor_estorno").alias("valor_estornado"),
        F.count("*").alias("qtd_estornos"),
    )

    # --- Eventos de risco agregados por transacao ---
    eventos_agg = eventos.groupBy("id_transacao").agg(
        F.count("*").alias("qtd_eventos_risco"),
        F.max(F.when(F.col("tipo_evento") == "fraude", 1).otherwise(0)).alias("_fraude"),
        F.max(F.when(F.col("tipo_evento") == "chargeback", 1).otherwise(0)).alias("_chargeback"),
    )

    fato = (
        fato.join(estornos_agg, "id_transacao", "left")
        .join(eventos_agg, "id_transacao", "left")
        .withColumn("valor_estornado", F.coalesce("valor_estornado", F.lit(0.0)))
        .withColumn("qtd_estornos", F.coalesce("qtd_estornos", F.lit(0)))
        .withColumn("qtd_eventos_risco", F.coalesce("qtd_eventos_risco", F.lit(0)))
        .withColumn(
            "valor_liquido",
            F.greatest(F.col("valor") - F.col("valor_estornado"), F.lit(0.0)),
        )
        .withColumn("flag_estornada", F.col("qtd_estornos") > 0)
        .withColumn(
            "flag_cartao_cancelado",
            F.col("status_cartao_vigente") == "cancelado",
        )
        .withColumn("flag_fraude", F.coalesce(F.col("_fraude"), F.lit(0)) == 1)
        .withColumn("flag_chargeback", F.coalesce(F.col("_chargeback"), F.lit(0)) == 1)
        .withColumn("ano_mes", F.date_format("data_transacao", "yyyy-MM"))
        .drop("_fraude", "_chargeback")
    )

    return fato


def construir_dimensoes(spark: SparkSession, config: Config) -> dict[str, DataFrame]:
    def vigentes(entidade: str) -> DataFrame:
        return spark.table(config.tabela(config.schema_prata, entidade)).where(
            F.col("flag_vigente")
        )

    dim_cliente = vigentes("clientes").select(
        "id_cliente", "cpf", "nome", "cidade", "estado", "renda", "segmento",
        F.col("data_inicio_vigencia").alias("vigente_desde"),
    )
    dim_conta = vigentes("contas").select(
        "id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura",
    )
    dim_cartao = vigentes("cartoes").select(
        "id_cartao", "id_conta", "tipo_cartao", "limite", "status_cartao",
    )

    fato = spark.table(config.tabela(config.schema_ouro, "gold_fato_transacao"))
    dim_estab = (
        fato.select("estabelecimento", "mcc")
        .where(F.col("estabelecimento").isNotNull())
        .distinct()
        .withColumn(
            "id_estabelecimento",
            F.sha2(F.concat_ws("||", F.col("estabelecimento"), F.col("mcc").cast("string")), 256),
        )
    )
    return {
        "gold_dim_cliente": dim_cliente,
        "gold_dim_conta": dim_conta,
        "gold_dim_cartao": dim_cartao,
        "gold_dim_estabelecimento": dim_estab,
    }


def construir_cliente_mes(fato: DataFrame) -> DataFrame:
    """Metricas comportamentais mensais por cliente.

    Considera apenas transacoes que compoem metricas: exclui cartoes cancelados
    na data. Usa ``valor_liquido`` para refletir estornos.
    """

    base = fato.where(~F.col("flag_cartao_cancelado") & F.col("id_cliente").isNotNull())
    return base.groupBy("id_cliente", "ano_mes").agg(
        F.count("*").alias("qtd_transacoes"),
        F.round(F.sum("valor"), 2).alias("valor_bruto_total"),
        F.round(F.sum("valor_liquido"), 2).alias("valor_liquido_total"),
        F.round(F.avg("valor"), 2).alias("ticket_medio"),
        F.round(F.max("valor"), 2).alias("maior_transacao"),
        F.sum("qtd_estornos").alias("qtd_estornos"),
        F.round(F.sum("valor_estornado"), 2).alias("valor_estornado"),
        F.sum(F.when(F.col("canal") == "online", 1).otherwise(0)).alias("qtd_online"),
        F.countDistinct("estabelecimento").alias("qtd_estabelecimentos"),
        F.sum(F.col("qtd_eventos_risco")).alias("qtd_eventos_risco"),
    )


def construir_indicadores_risco(fato: DataFrame) -> DataFrame:
    """Indicadores de risco por cliente x mes (fraude, chargeback, estornos)."""

    base = fato.where(F.col("id_cliente").isNotNull())
    return base.groupBy("id_cliente", "ano_mes").agg(
        F.sum(F.col("flag_fraude").cast("int")).alias("qtd_fraudes"),
        F.sum(F.col("flag_chargeback").cast("int")).alias("qtd_chargebacks"),
        F.sum(F.col("qtd_eventos_risco")).alias("qtd_eventos_risco"),
        F.sum(F.col("flag_estornada").cast("int")).alias("qtd_transacoes_estornadas"),
        F.round(F.sum("valor_estornado"), 2).alias("valor_estornado"),
        F.round(F.sum("valor"), 2).alias("valor_transacionado"),
        F.round(
            F.sum("valor_estornado") / F.when(F.sum("valor") > 0, F.sum("valor")).otherwise(F.lit(None)),
            4,
        ).alias("taxa_estorno"),
    )


def construir_features_cliente(
    spark: SparkSession, config: Config, cliente_mes: DataFrame
) -> DataFrame:
    """Features agregadas por cliente para consumo de Data Science.

    Usa funcoes de janela (PERCENT_RANK/NTILE) para posicionar o cliente frente
    a base, alem de metricas consolidadas do periodo.
    """

    dim_cliente = spark.table(config.tabela(config.schema_ouro, "gold_dim_cliente"))

    agregado = cliente_mes.groupBy("id_cliente").agg(
        F.sum("qtd_transacoes").alias("total_transacoes"),
        F.round(F.sum("valor_liquido_total"), 2).alias("valor_liquido_total"),
        F.round(F.avg("ticket_medio"), 2).alias("ticket_medio"),
        F.countDistinct("ano_mes").alias("meses_ativos"),
        F.sum("qtd_online").alias("total_online"),
        F.sum("qtd_estornos").alias("total_estornos"),
        F.sum("qtd_eventos_risco").alias("total_eventos_risco"),
    ).withColumn(
        "pct_online",
        F.round(F.col("total_online") / F.when(F.col("total_transacoes") > 0,
                F.col("total_transacoes")).otherwise(F.lit(None)), 4),
    )

    janela = Window.orderBy(F.col("valor_liquido_total").asc())
    features = (
        dim_cliente.join(agregado, "id_cliente", "left")
        .na.fill(0)
        .withColumn("percentil_gasto", F.round(F.percent_rank().over(janela), 4))
        .withColumn("quartil_gasto", F.ntile(4).over(janela))
        .withColumn(
            "flag_cliente_risco",
            (F.col("total_eventos_risco") > 0) | (F.col("total_estornos") > 0),
        )
    )
    return features


def executar_ouro(spark: SparkSession, config: Config) -> None:
    logger.info("=== OURO inicio batch_id=%s ===", config.batch_id)
    _garantir_schema(spark, config)

    fato = construir_fato_transacao(spark, config)
    _salvar(fato, config.tabela(config.schema_ouro, "gold_fato_transacao"))
    logger.info("ouro gold_fato_transacao=%d", fato.count())

    for nome, df in construir_dimensoes(spark, config).items():
        _salvar(df, config.tabela(config.schema_ouro, nome))
        logger.info("ouro %s=%d", nome, df.count())

    fato = spark.table(config.tabela(config.schema_ouro, "gold_fato_transacao"))

    cliente_mes = construir_cliente_mes(fato)
    _salvar(cliente_mes, config.tabela(config.schema_ouro, "gold_cliente_mes"))
    logger.info("ouro gold_cliente_mes=%d", cliente_mes.count())

    indicadores = construir_indicadores_risco(fato)
    _salvar(indicadores, config.tabela(config.schema_ouro, "gold_indicadores_risco"))
    logger.info("ouro gold_indicadores_risco=%d", indicadores.count())

    cliente_mes = spark.table(config.tabela(config.schema_ouro, "gold_cliente_mes"))
    features = construir_features_cliente(spark, config, cliente_mes)
    _salvar(features, config.tabela(config.schema_ouro, "gold_features_cliente"))
    logger.info("ouro gold_features_cliente=%d", features.count())

    logger.info("=== OURO fim ===")
