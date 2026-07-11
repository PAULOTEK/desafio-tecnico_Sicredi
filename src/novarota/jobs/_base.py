"""Utilitarios comuns aos jobs: parsing de argumentos e bootstrap.

Todos os jobs compartilham os mesmos parametros de linha de comando, permitindo
sobrescrever a configuracao (modo de execucao, data de referencia, batch_id e
caminho do YAML) sem alterar codigo.
"""

from __future__ import annotations

import argparse

from novarota.common.logging_config import configurar_logs
from novarota.config import Config


def parsear_args(descricao: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=descricao)
    parser.add_argument("--config", help="Caminho do arquivo YAML de configuracao")
    parser.add_argument("--modo", choices=["full", "incremental"], help="Modo de execucao")
    parser.add_argument("--data-referencia", help="Data de referencia (YYYY-MM-DD)")
    parser.add_argument("--batch-id", help="Identificador do batch")
    return parser.parse_args()


def montar_config(args: argparse.Namespace) -> Config:
    configurar_logs()
    config = Config.carregar(args.config)
    if args.modo:
        config.modo_execucao = args.modo
    if args.data_referencia:
        from datetime import date

        config.data_referencia = date.fromisoformat(args.data_referencia)
    if args.batch_id:
        config.batch_id = args.batch_id
    return config
