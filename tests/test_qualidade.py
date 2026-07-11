"""Testes das regras de qualidade e segregacao em quarentena."""

from __future__ import annotations

from novarota.qualidade import regras
from novarota.qualidade.regras import aplicar_regras


def test_clientes_segrega_cpf_e_renda_invalidos(spark):
    dados = [
        (1, "11111111111", 4500.0, "I"),   # valido
        (2, "ABC123", 1000.0, "I"),        # cpf invalido
        (3, "33333333333", -10.0, "I"),    # renda negativa
        (4, "44444444444", 2000.0, "X"),   # operacao invalida
    ]
    df = spark.createDataFrame(dados, ["id_cliente", "cpf", "renda", "operacao"])

    resultado = aplicar_regras(df, regras.regras_clientes())

    assert resultado.validos.count() == 1
    assert resultado.quarentena.count() == 3
    motivos = {r["motivo_quarentena"] for r in resultado.quarentena.collect()}
    assert any("cpf_invalido" in m for m in motivos)
    assert any("renda_negativa" in m for m in motivos)
    assert any("operacao_invalida" in m for m in motivos)


def test_transacoes_valor_nao_positivo_vai_para_quarentena(spark):
    dados = [
        ("T1", 900, 150.0, "BRL"),   # valido
        ("T2", 900, 0.0, "BRL"),     # valor zero
        ("T3", 900, -5.0, "BRL"),    # valor negativo
        ("T4", 900, 10.0, None),     # moeda nula
    ]
    df = spark.createDataFrame(dados, ["id_transacao", "id_cartao", "valor", "moeda"])

    resultado = aplicar_regras(df, regras.regras_transacoes())

    validos = [r["id_transacao"] for r in resultado.validos.collect()]
    assert validos == ["T1"]
    assert resultado.quarentena.count() == 3


def test_multiplos_motivos_sao_concatenados(spark):
    df = spark.createDataFrame(
        [(None, "ABC", -1.0, "I")],
        "id_cliente int, cpf string, renda double, operacao string",
    )
    resultado = aplicar_regras(df, regras.regras_clientes())
    motivo = resultado.quarentena.collect()[0]["motivo_quarentena"]
    # Deve listar as tres violacoes separadas por ';'.
    assert motivo.count(";") == 2
