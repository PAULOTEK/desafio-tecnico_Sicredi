"""Testes da construcao do historico SCD Tipo 2."""

from __future__ import annotations

from novarota.transformacao.scd2 import DATA_FIM_ABERTA, DefinicaoSCD2, construir_historico

DEF = DefinicaoSCD2(
    chave_negocio="id_cliente",
    colunas_atributos=["cidade", "segmento"],
)


def _df(spark, linhas):
    return spark.createDataFrame(
        linhas, ["id_cliente", "cidade", "segmento", "data_atualizacao", "operacao"]
    )


def test_duas_versoes_fecham_vigencia_corretamente(spark):
    df = _df(spark, [
        (1, "Porto Alegre", "varejo", "2024-01-05 10:00:00", "I"),
        (1, "Gramado", "alta_renda", "2024-02-10 14:00:00", "U"),
    ])
    hist = {r["cidade"]: r for r in construir_historico(df, DEF).collect()}

    antiga = hist["Porto Alegre"]
    nova = hist["Gramado"]
    assert antiga["flag_vigente"] is False
    assert nova["flag_vigente"] is True
    # A versao antiga fecha exatamente no inicio da nova.
    assert antiga["data_fim_vigencia"] == nova["data_inicio_vigencia"]
    assert str(nova["data_fim_vigencia"]) == DATA_FIM_ABERTA


def test_duplicata_exata_e_removida(spark):
    df = _df(spark, [
        (2, "Curitiba", "varejo", "2024-01-06 09:00:00", "I"),
        (2, "Curitiba", "varejo", "2024-01-06 09:00:00", "I"),
    ])
    hist = construir_historico(df, DEF)
    assert hist.count() == 1


def test_delete_encerra_vigencia_e_nao_fica_vigente(spark):
    df = _df(spark, [
        (3, "Sao Paulo", "varejo", "2024-01-01 10:00:00", "I"),
        (3, "Sao Paulo", "varejo", "2024-03-01 10:00:00", "D"),
    ])
    hist = {str(r["operacao"]): r for r in construir_historico(df, DEF).collect()}
    deletado = hist["D"]
    assert deletado["flag_vigente"] is False
    # Ao deletar, o fim de vigencia iguala o inicio (registro encerrado).
    assert deletado["data_fim_vigencia"] == deletado["data_inicio_vigencia"]


def test_surrogate_key_e_deterministica(spark):
    linhas = [(4, "Londrina", "varejo", "2024-01-01 10:00:00", "I")]
    sk1 = construir_historico(_df(spark, linhas), DEF).collect()[0]["sk"]
    sk2 = construir_historico(_df(spark, linhas), DEF).collect()[0]["sk"]
    assert sk1 == sk2
