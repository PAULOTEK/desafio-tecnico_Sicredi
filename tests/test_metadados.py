"""Testes dos metadados tecnicos da camada Bronze."""

from __future__ import annotations

from src.novarota.common.metadados import COLUNAS_METADADOS, adicionar_metadados_bronze


def test_adiciona_todas_as_colunas_de_metadados(spark):
    df = spark.createDataFrame([(1, "Ana")], ["id_cliente", "nome"])
    resultado = adicionar_metadados_bronze(df, batch_id="B1", schema_version="v1")
    for coluna in COLUNAS_METADADOS:
        assert coluna in resultado.columns


def test_hash_linha_deterministico_para_linhas_iguais(spark):
    df = spark.createDataFrame(
        [(1, "Ana"), (1, "Ana"), (2, "Bruno")], ["id_cliente", "nome"]
    )
    resultado = adicionar_metadados_bronze(df, batch_id="B1")
    hashes = [r["hash_linha"] for r in resultado.select("hash_linha", "id_cliente").collect()]
    # As duas linhas identicas devem gerar o mesmo hash; a terceira, diferente.
    assert len(set(hashes)) == 2


def test_batch_id_e_schema_version_preenchidos(spark):
    df = spark.createDataFrame([(1,)], ["id_cliente"])
    linha = adicionar_metadados_bronze(df, batch_id="B42", schema_version="v2").collect()[0]
    assert linha["batch_id"] == "B42"
    assert linha["schema_version"] == "v2"
