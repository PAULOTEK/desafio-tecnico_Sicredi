"""Gerador de massa sintetica da Cooperativa NovaRota.

Cria arquivos de entrada
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from src.common.logging_config import obter_logger

logger = obter_logger("novarota.gerador")


def _escrever_json_lines(caminho: Path, registros: list[dict]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as f:
        for reg in registros:
            f.write(json.dumps(reg, ensure_ascii=False) + "\n")
    logger.info("arquivo_gerado=%s registros=%d", caminho, len(registros))


def _escrever_csv(caminho: Path, registros: list[dict], colunas: list[str]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        for reg in registros:
            writer.writerow(reg)
    logger.info("arquivo_gerado=%s registros=%d", caminho, len(registros))


# --------------------------------------------------------------------------- #
# CLIENTES (CDC)
# --------------------------------------------------------------------------- #
def _clientes_carga1() -> list[dict]:
    return [
        {"id_cliente": 1, "cpf": "11111111111", "nome": "Ana Souza", "cidade": "Porto Alegre",
         "estado": "RS", "renda": 4500.0, "segmento": "varejo",
         "data_atualizacao": "2024-01-05 10:00:00", "operacao": "I"},
        {"id_cliente": 2, "cpf": "22222222222", "nome": "Bruno Lima", "cidade": "Caxias do Sul",
         "estado": "RS", "renda": 9800.0, "segmento": "alta_renda",
         "data_atualizacao": "2024-01-05 10:05:00", "operacao": "I"},
        {"id_cliente": 3, "cpf": "33333333333", "nome": "Carla Dias", "cidade": "Curitiba",
         "estado": "PR", "renda": 3000.0, "segmento": "varejo",
         "data_atualizacao": "2024-01-06 09:00:00", "operacao": "I"},
        # Duplicata exata (mesmo conteudo) para exercitar deduplicacao por hash.
        {"id_cliente": 3, "cpf": "33333333333", "nome": "Carla Dias", "cidade": "Curitiba",
         "estado": "PR", "renda": 3000.0, "segmento": "varejo",
         "data_atualizacao": "2024-01-06 09:00:00", "operacao": "I"},
        # Registro invalido: cpf malformado e renda negativa -> quarentena.
        {"id_cliente": 4, "cpf": "ABC123", "nome": "Diego Alves", "cidade": "Florianopolis",
         "estado": "SC", "renda": -100.0, "segmento": "varejo",
         "data_atualizacao": "2024-01-07 11:00:00", "operacao": "I"},
        {"id_cliente": 5, "cpf": "55555555555", "nome": "Elisa Rocha", "cidade": "Sao Paulo",
         "estado": "SP", "renda": 15200.0, "segmento": "alta_renda",
         "data_atualizacao": "2024-01-08 08:30:00", "operacao": "I"},
    ]


def _clientes_carga2() -> list[dict]:
    return [
        # Atualizacao cadastral: cliente 1 muda de cidade/renda/segmento (SCD2).
        {"id_cliente": 1, "cpf": "11111111111", "nome": "Ana Souza", "cidade": "Gramado",
         "estado": "RS", "renda": 5200.0, "segmento": "alta_renda",
         "data_atualizacao": "2024-02-10 14:00:00", "operacao": "U"},
        # Correcao do registro invalido do cliente 4 (agora valido).
        {"id_cliente": 4, "cpf": "44444444444", "nome": "Diego Alves", "cidade": "Florianopolis",
         "estado": "SC", "renda": 2800.0, "segmento": "varejo",
         "data_atualizacao": "2024-02-11 09:00:00", "operacao": "U"},
        # Novo cliente.
        {"id_cliente": 6, "cpf": "66666666666", "nome": "Fabio Nunes", "cidade": "Londrina",
         "estado": "PR", "renda": 7100.0, "segmento": "alta_renda",
         "data_atualizacao": "2024-02-12 16:20:00", "operacao": "I"},
    ]


# --------------------------------------------------------------------------- #
# CONTAS (CDC)
# --------------------------------------------------------------------------- #
def _contas_carga1() -> list[dict]:
    return [
        {"id_conta": 100, "id_cliente": 1, "tipo_conta": "corrente", "status_conta": "ativa",
         "data_abertura": "2020-03-01", "data_atualizacao": "2024-01-05 10:00:00", "operacao": "I"},
        {"id_conta": 101, "id_cliente": 1, "tipo_conta": "poupanca", "status_conta": "ativa",
         "data_abertura": "2021-06-15", "data_atualizacao": "2024-01-05 10:00:00", "operacao": "I"},
        {"id_conta": 102, "id_cliente": 2, "tipo_conta": "corrente", "status_conta": "ativa",
         "data_abertura": "2019-11-20", "data_atualizacao": "2024-01-05 10:05:00", "operacao": "I"},
        {"id_conta": 103, "id_cliente": 3, "tipo_conta": "corrente", "status_conta": "ativa",
         "data_abertura": "2022-02-10", "data_atualizacao": "2024-01-06 09:00:00", "operacao": "I"},
        {"id_conta": 104, "id_cliente": 5, "tipo_conta": "corrente", "status_conta": "ativa",
         "data_abertura": "2023-01-05", "data_atualizacao": "2024-01-08 08:30:00", "operacao": "I"},
        # Conta com id_cliente inexistente -> integridade referencial quebrada.
        {"id_conta": 105, "id_cliente": 999, "tipo_conta": "corrente", "status_conta": "ativa",
         "data_abertura": "2023-05-05", "data_atualizacao": "2024-01-09 08:30:00", "operacao": "I"},
    ]


def _contas_carga2() -> list[dict]:
    return [
        # Conta 103 e encerrada (mudanca de status).
        {"id_conta": 103, "id_cliente": 3, "tipo_conta": "corrente", "status_conta": "encerrada",
         "data_abertura": "2022-02-10", "data_atualizacao": "2024-02-15 10:00:00", "operacao": "U"},
        # Nova conta do cliente 6.
        {"id_conta": 106, "id_cliente": 6, "tipo_conta": "corrente", "status_conta": "ativa",
         "data_abertura": "2024-02-12", "data_atualizacao": "2024-02-12 16:20:00", "operacao": "I"},
    ]


# --------------------------------------------------------------------------- #
# CARTOES (CDC)
# --------------------------------------------------------------------------- #
def _cartoes_carga1() -> list[dict]:
    return [
        {"id_cartao": 900, "id_conta": 100, "tipo_cartao": "credito", "limite": 5000.0,
         "status_cartao": "ativo", "data_atualizacao": "2024-01-05 10:00:00", "operacao": "I"},
        {"id_cartao": 901, "id_conta": 102, "tipo_cartao": "credito", "limite": 12000.0,
         "status_cartao": "ativo", "data_atualizacao": "2024-01-05 10:05:00", "operacao": "I"},
        {"id_cartao": 902, "id_conta": 103, "tipo_cartao": "debito", "limite": 0.0,
         "status_cartao": "ativo", "data_atualizacao": "2024-01-06 09:00:00", "operacao": "I"},
        {"id_cartao": 903, "id_conta": 104, "tipo_cartao": "credito", "limite": 8000.0,
         "status_cartao": "ativo", "data_atualizacao": "2024-01-08 08:30:00", "operacao": "I"},
        # Cartao sem conta valida (id_conta inexistente) -> quarentena referencial.
        {"id_cartao": 904, "id_conta": 777, "tipo_cartao": "credito", "limite": 3000.0,
         "status_cartao": "ativo", "data_atualizacao": "2024-01-09 08:30:00", "operacao": "I"},
    ]


def _cartoes_carga2() -> list[dict]:
    return [
        # Aumento de limite do cartao 900 (nova versao historica - SCD2).
        {"id_cartao": 900, "id_conta": 100, "tipo_cartao": "credito", "limite": 7500.0,
         "status_cartao": "ativo", "data_atualizacao": "2024-02-10 14:00:00", "operacao": "U"},
        # Cartao 901 e cancelado (nao deve compor metricas futuras).
        {"id_cartao": 901, "id_conta": 102, "tipo_cartao": "credito", "limite": 12000.0,
         "status_cartao": "cancelado", "data_atualizacao": "2024-02-20 09:00:00", "operacao": "U"},
        # Novo cartao do cliente 6 (conta 106).
        {"id_cartao": 905, "id_conta": 106, "tipo_cartao": "credito", "limite": 6000.0,
         "status_cartao": "ativo", "data_atualizacao": "2024-02-12 16:20:00", "operacao": "I"},
    ]


# --------------------------------------------------------------------------- #
# TRANSACOES (particionadas por data)
# --------------------------------------------------------------------------- #
_COLS_TX_V1 = ["id_transacao", "id_cartao", "data_transacao", "valor", "mcc",
               "estabelecimento", "canal", "pais", "moeda"]
_COLS_TX_V2 = _COLS_TX_V1 + ["dispositivo"]  # evolucao de schema na carga 2


def _tx(id_transacao, id_cartao, data, valor, mcc, estab, canal="pos",
        pais="BR", moeda="BRL", dispositivo=None) -> dict:
    reg = {
        "id_transacao": id_transacao, "id_cartao": id_cartao, "data_transacao": data,
        "valor": valor, "mcc": mcc, "estabelecimento": estab, "canal": canal,
        "pais": pais, "moeda": moeda,
    }
    if dispositivo is not None:
        reg["dispositivo"] = dispositivo
    return reg


def _transacoes_carga1() -> dict[str, list[dict]]:
    """Retorna transacoes agrupadas por data (particao)."""

    return {
        "2024-01-10": [
            _tx("T0001", 900, "2024-01-10", 150.00, 5411, "Mercado Central", "pos"),
            _tx("T0002", 900, "2024-01-10", 89.90, 5812, "Restaurante Sul", "pos"),
            _tx("T0003", 901, "2024-01-10", 1200.00, 5732, "Eletronicos SA", "online"),
            # Duplicata exata do T0001 dentro da mesma carga.
            _tx("T0001", 900, "2024-01-10", 150.00, 5411, "Mercado Central", "pos"),
        ],
        "2024-01-11": [
            _tx("T0004", 903, "2024-01-11", 45.00, 5912, "Farmacia Vida", "pos"),
            _tx("T0005", 901, "2024-01-11", 3200.00, 5944, "Joalheria Luz", "online",
                pais="US", moeda="USD"),
            # Valor invalido (zero) -> quarentena.
            _tx("T0006", 903, "2024-01-11", 0.0, 5411, "Mercado Central", "pos"),
            # Valor invalido (negativo) -> quarentena.
            _tx("T0007", 900, "2024-01-11", -50.0, 5411, "Mercado Central", "pos"),
        ],
    }


def _transacoes_carga2() -> dict[str, list[dict]]:
    return {
        # Dado atrasado: transacao de 2024-01-10 so chega na carga 2.
        "2024-01-10": [
            _tx("T0008", 903, "2024-01-10", 220.00, 5411, "Mercado Norte", "pos",
                dispositivo="android"),
        ],
        "2024-02-14": [
            _tx("T0020", 900, "2024-02-14", 300.00, 5999, "Loja Presentes", "pos",
                dispositivo="ios"),
            _tx("T0021", 900, "2024-02-14", 4800.00, 5999, "Loja Presentes", "online",
                dispositivo="web"),
            # id_transacao repetido entre cargas (T0004 ja veio na carga 1).
            _tx("T0004", 903, "2024-01-11", 45.00, 5912, "Farmacia Vida", "pos",
                dispositivo="android"),
            # Transacao de cartao cancelado (901) apos cancelamento.
            _tx("T0022", 901, "2024-02-25", 500.00, 5732, "Eletronicos SA", "online",
                dispositivo="web"),
            _tx("T0023", 905, "2024-02-14", 130.00, 5411, "Mercado Central", "pos",
                dispositivo="ios"),
        ],
    }


# --------------------------------------------------------------------------- #
# EVENTOS DE RISCO e ESTORNOS
# --------------------------------------------------------------------------- #
def _eventos_risco() -> list[dict]:
    return [
        {"id_evento": "E1", "id_transacao": "T0005", "tipo_evento": "fraude",
         "severidade": "alta", "data_evento": "2024-01-12"},
        {"id_evento": "E2", "id_transacao": "T0003", "tipo_evento": "suspeita",
         "severidade": "media", "data_evento": "2024-01-11"},
        {"id_evento": "E3", "id_transacao": "T0021", "tipo_evento": "chargeback",
         "severidade": "alta", "data_evento": "2024-02-16"},
        # Evento com vinculo invalido (transacao inexistente).
        {"id_evento": "E4", "id_transacao": "T9999", "tipo_evento": "fraude",
         "severidade": "alta", "data_evento": "2024-02-18"},
    ]


def _estornos() -> list[dict]:
    return [
        # Estorno total do T0003 (valor original 1200).
        {"id_estorno": "R1", "id_transacao": "T0003", "valor_estorno": 1200.00,
         "data_estorno": "2024-01-13", "motivo": "cancelamento_compra"},
        # Estorno parcial do T0021 (valor original 4800, estorna 2000).
        {"id_estorno": "R2", "id_transacao": "T0021", "valor_estorno": 2000.00,
         "data_estorno": "2024-02-17", "motivo": "chargeback"},
        # Estorno com referencia invalida.
        {"id_estorno": "R3", "id_transacao": "T8888", "valor_estorno": 100.00,
         "data_estorno": "2024-02-19", "motivo": "duplicidade"},
    ]


# --------------------------------------------------------------------------- #
# Orquestracao da geracao
# --------------------------------------------------------------------------- #
def gerar_massa(dir_landing: Path) -> None:
    """Gera todos os arquivos de entrada nas duas cargas."""

    dir_landing = Path(dir_landing)
    logger.info("iniciando_geracao dir_landing=%s", dir_landing)

    # ---- Clientes / Contas / Cartoes (JSON, estilo CDC) ----
    _escrever_json_lines(dir_landing / "clientes" / "carga=1" / "clientes_cdc.json",
                         _clientes_carga1())
    _escrever_json_lines(dir_landing / "clientes" / "carga=2" / "clientes_cdc.json",
                         _clientes_carga2())

    _escrever_json_lines(dir_landing / "contas" / "carga=1" / "contas_cdc.json",
                         _contas_carga1())
    _escrever_json_lines(dir_landing / "contas" / "carga=2" / "contas_cdc.json",
                         _contas_carga2())

    _escrever_json_lines(dir_landing / "cartoes" / "carga=1" / "cartoes_cdc.json",
                         _cartoes_carga1())
    _escrever_json_lines(dir_landing / "cartoes" / "carga=2" / "cartoes_cdc.json",
                         _cartoes_carga2())

    # ---- Transacoes (CSV particionado por data) ----
    for data, registros in _transacoes_carga1().items():
        caminho = dir_landing / "transacoes" / "carga=1" / f"data_transacao={data}" / "parte.csv"
        _escrever_csv(caminho, registros, _COLS_TX_V1)
    for data, registros in _transacoes_carga2().items():
        caminho = dir_landing / "transacoes" / "carga=2" / f"data_transacao={data}" / "parte.csv"
        _escrever_csv(caminho, registros, _COLS_TX_V2)

    # ---- Eventos de risco e Estornos (CSV) ----
    _escrever_csv(dir_landing / "eventos_risco" / "carga=1" / "eventos_risco.csv",
                 _eventos_risco(),
                 ["id_evento", "id_transacao", "tipo_evento", "severidade", "data_evento"])
    _escrever_csv(dir_landing / "estornos" / "carga=1" / "estornos.csv",
                 _estornos(),
                 ["id_estorno", "id_transacao", "valor_estorno", "data_estorno", "motivo"])

    logger.info("geracao_concluida dir_landing=%s", dir_landing)
