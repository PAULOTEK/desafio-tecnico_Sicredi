"""Configuracao de logs estruturados por etapa do pipeline..
"""

from __future__ import annotations

import logging
import sys

_FORMATO = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
_configurado = False


def configurar_logs(nivel: int = logging.INFO) -> None:
    """Configura o logger raiz uma unica vez."""

    global _configurado
    if _configurado:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMATO))

    raiz = logging.getLogger()
    raiz.setLevel(nivel)
    raiz.addHandler(handler)

    # Reduz o ruido do log verboso do Spark/py4j.
    for ruidoso in ("py4j", "pyspark"):
        logging.getLogger(ruidoso).setLevel(logging.WARNING)

    _configurado = True


def obter_logger(nome: str) -> logging.Logger:
    """Retorna um logger nomeado, garantindo a configuracao previa."""

    configurar_logs()
    return logging.getLogger(nome)
