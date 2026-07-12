"""Testes da configuracao parametrizavel."""

from __future__ import annotations

from datetime import date

import pytest
from src.novarota.config import Config


def test_valores_padrao():
    cfg = Config()
    assert cfg.modo_execucao == "full"
    assert cfg.schema_bronze == "bronze"
    assert cfg.dir_landing.is_absolute()


def test_modo_invalido_gera_erro():
    with pytest.raises(ValueError):
        Config(modo_execucao="parcial")


def test_data_referencia_aceita_string():
    cfg = Config(data_referencia="2024-03-01")
    assert cfg.data_referencia == date(2024, 3, 1)


def test_variaveis_de_ambiente_sobrescrevem(monkeypatch):
    monkeypatch.setenv("NOVAROTA_MODO_EXECUCAO", "incremental")
    monkeypatch.setenv("NOVAROTA_SCHEMA_OURO", "gold")
    cfg = Config.carregar(caminho_yaml="/caminho/inexistente.yaml")
    assert cfg.modo_execucao == "incremental"
    assert cfg.schema_ouro == "gold"


def test_nome_tabela_qualificado():
    cfg = Config()
    assert cfg.tabela("prata", "clientes") == "prata.clientes"


def test_nome_tabela_com_catalogo():
    cfg = Config(usar_catalogo=True)
    assert cfg.tabela("prata", "clientes") == "novarota.prata.clientes"
    assert cfg.schema_qualificado("bronze") == "novarota.bronze"


def test_usar_catalogo_aceita_string():
    cfg = Config(usar_catalogo="true")
    assert cfg.usar_catalogo is True
    cfg2 = Config(usar_catalogo="false")
    assert cfg2.usar_catalogo is False
