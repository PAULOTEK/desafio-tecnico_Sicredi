"""Regras de qualidade de dados e segregacao em quarentena.

Cada entidade possui um conjunto de regras (``RegraQualidade``). Uma regra e
uma expressao booleana que deve ser **verdadeira** para o registro ser
considerado valido. Registros que violam qualquer regra sao segregados em uma
tabela de quarentena, com a coluna ``motivo_quarentena`` listando todas as
regras violadas (facilita a analise e o reprocessamento posterior).

As validacoes puramente estruturais (formato de cpf, dominio de status, sinal
de valor) ficam aqui. Validacoes de integridade referencial entre entidades
(ex.: cartao sem conta) sao aplicadas na camada Prata, quando as dimensoes ja
estao disponiveis.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class RegraQualidade:
    """Regra de qualidade: ``condicao`` deve ser True para registro valido."""

    nome: str
    condicao: Column


@dataclass
class ResultadoQualidade:
    """Resultado da aplicacao das regras: registros validos e em quarentena."""

    validos: DataFrame
    quarentena: DataFrame


def aplicar_regras(df: DataFrame, regras: list[RegraQualidade]) -> ResultadoQualidade:
    """Aplica as regras e separa validos de invalidos.

    A tabela de quarentena preserva todas as colunas originais e adiciona
    ``motivo_quarentena`` (lista de regras violadas, separadas por ``;``).
    """

    if not regras:
        return ResultadoQualidade(validos=df, quarentena=df.limit(0).withColumn(
            "motivo_quarentena", F.lit(None).cast("string")))

    # Marca, para cada regra, o nome quando a condicao FALHA.
    motivos = [
        F.when(~regra.condicao | regra.condicao.isNull(), F.lit(regra.nome))
        for regra in regras
    ]
    df_marcado = df.withColumn(
        "motivo_quarentena",
        F.concat_ws(";", F.array_compact(F.array(*motivos))),
    )

    validos = df_marcado.where(F.col("motivo_quarentena") == "").drop("motivo_quarentena")
    quarentena = df_marcado.where(F.col("motivo_quarentena") != "")
    return ResultadoQualidade(validos=validos, quarentena=quarentena)


# --------------------------------------------------------------------------- #
# Conjuntos de regras por entidade
# --------------------------------------------------------------------------- #
def regras_clientes() -> list[RegraQualidade]:
    return [
        RegraQualidade("id_cliente_nulo", F.col("id_cliente").isNotNull()),
        # cpf deve conter exatamente 11 digitos numericos.
        RegraQualidade("cpf_invalido", F.col("cpf").rlike(r"^\d{11}$")),
        RegraQualidade("renda_negativa", F.col("renda") >= 0),
        RegraQualidade("operacao_invalida", F.col("operacao").isin("I", "U", "D")),
    ]


def regras_contas() -> list[RegraQualidade]:
    return [
        RegraQualidade("id_conta_nulo", F.col("id_conta").isNotNull()),
        RegraQualidade("id_cliente_nulo", F.col("id_cliente").isNotNull()),
        RegraQualidade(
            "status_invalido",
            F.col("status_conta").isin("ativa", "encerrada", "bloqueada"),
        ),
        RegraQualidade("operacao_invalida", F.col("operacao").isin("I", "U", "D")),
    ]


def regras_cartoes() -> list[RegraQualidade]:
    return [
        RegraQualidade("id_cartao_nulo", F.col("id_cartao").isNotNull()),
        RegraQualidade("id_conta_nulo", F.col("id_conta").isNotNull()),
        RegraQualidade("limite_negativo", F.col("limite") >= 0),
        RegraQualidade(
            "status_invalido",
            F.col("status_cartao").isin("ativo", "cancelado", "bloqueado"),
        ),
        RegraQualidade("operacao_invalida", F.col("operacao").isin("I", "U", "D")),
    ]


def regras_transacoes() -> list[RegraQualidade]:
    return [
        RegraQualidade("id_transacao_nulo", F.col("id_transacao").isNotNull()),
        RegraQualidade("id_cartao_nulo", F.col("id_cartao").isNotNull()),
        # valor deve ser estritamente positivo (zero/negativo sao inconsistentes).
        RegraQualidade("valor_nao_positivo", F.col("valor") > 0),
        RegraQualidade("moeda_nula", F.col("moeda").isNotNull()),
    ]


def regras_eventos_risco() -> list[RegraQualidade]:
    return [
        RegraQualidade("id_evento_nulo", F.col("id_evento").isNotNull()),
        RegraQualidade("id_transacao_nulo", F.col("id_transacao").isNotNull()),
        RegraQualidade(
            "severidade_invalida",
            F.col("severidade").isin("baixa", "media", "alta"),
        ),
    ]


def regras_estornos() -> list[RegraQualidade]:
    return [
        RegraQualidade("id_estorno_nulo", F.col("id_estorno").isNotNull()),
        RegraQualidade("id_transacao_nulo", F.col("id_transacao").isNotNull()),
    ]
