"""Testes das agregacoes da camada Ouro (regras de negocio)."""

from __future__ import annotations

from src.transformacao.ouro import (
    construir_cliente_mes,
    construir_indicadores_risco,
)

COLUNAS_FATO = [
    "id_transacao", "id_cliente", "ano_mes", "canal", "estabelecimento",
    "valor", "valor_liquido", "valor_estornado", "qtd_estornos",
    "qtd_eventos_risco", "flag_estornada", "flag_cartao_cancelado",
    "flag_fraude", "flag_chargeback",
]


def _fato(spark):
    linhas = [
        # cliente 1: 2 transacoes validas, uma estornada parcialmente.
        ("T1", 1, "2024-01", "pos", "Mercado", 100.0, 100.0, 0.0, 0, 0,
         False, False, False, False),
        ("T2", 1, "2024-01", "online", "Loja", 200.0, 150.0, 50.0, 1, 0,
         True, False, False, False),
        # cliente 1: transacao em cartao CANCELADO -> nao entra nas metricas.
        ("T3", 1, "2024-02", "online", "Loja", 500.0, 500.0, 0.0, 0, 0,
         False, True, False, False),
        # cliente 2: fraude + chargeback.
        ("T4", 2, "2024-01", "online", "Joalheria", 3000.0, 3000.0, 0.0, 0, 1,
         False, False, True, False),
        ("T5", 2, "2024-02", "online", "Eletronicos", 800.0, 0.0, 800.0, 1, 1,
         True, False, False, True),
    ]
    return spark.createDataFrame(linhas, COLUNAS_FATO)


def test_cliente_mes_exclui_cartao_cancelado(spark):
    resultado = {(r["id_cliente"], r["ano_mes"]): r
                 for r in construir_cliente_mes(_fato(spark)).collect()}
    # cliente 1 em 2024-02 so tinha a transacao de cartao cancelado -> ausente.
    assert (1, "2024-02") not in resultado
    # cliente 1 em 2024-01: 2 transacoes, valor liquido = 100 + 150.
    jan = resultado[(1, "2024-01")]
    assert jan["qtd_transacoes"] == 2
    assert jan["valor_liquido_total"] == 250.0
    assert jan["valor_bruto_total"] == 300.0


def test_indicadores_risco_conta_fraude_e_chargeback(spark):
    ind = {(r["id_cliente"], r["ano_mes"]): r
           for r in construir_indicadores_risco(_fato(spark)).collect()}
    assert ind[(2, "2024-01")]["qtd_fraudes"] == 1
    assert ind[(2, "2024-02")]["qtd_chargebacks"] == 1
    assert ind[(2, "2024-02")]["valor_estornado"] == 800.0
