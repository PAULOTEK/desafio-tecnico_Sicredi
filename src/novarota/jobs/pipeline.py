"""Job orquestrador: executa o pipeline ponta a ponta (Bronze -> Prata -> Ouro).

Reutiliza uma unica SparkSession entre as camadas. Trata excecoes com
*fail-fast*: qualquer falha critica interrompe o pipeline e propaga o erro
(registrado no log), evitando materializar camadas em estado inconsistente.
"""

from __future__ import annotations

import argparse

from novarota.common.logging_config import obter_logger
from novarota.common.spark import criar_spark
from novarota.ingestao.bronze import executar_bronze
from novarota.ingestao.gerador_dados import gerar_massa
from novarota.jobs._base import montar_config
from novarota.transformacao.ouro import executar_ouro
from novarota.transformacao.prata import executar_prata

logger = obter_logger("novarota.pipeline")


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline NovaRota ponta a ponta")
    parser.add_argument("--config")
    parser.add_argument("--modo", choices=["full", "incremental"])
    parser.add_argument("--data-referencia")
    parser.add_argument("--batch-id")
    parser.add_argument(
        "--gerar-dados",
        action="store_true",
        help="Gera a massa sintetica na landing antes de ingerir",
    )
    return parser.parse_args()


def main() -> None:
    args = _args()
    config = montar_config(args)

    if getattr(args, "gerar_dados", False):
        gerar_massa(config.dir_landing)

    spark = criar_spark("novarota-pipeline", config)
    try:
        logger.info("### PIPELINE inicio batch_id=%s ###", config.batch_id)
        executar_bronze(spark, config)
        executar_prata(spark, config)
        executar_ouro(spark, config)
        logger.info("### PIPELINE concluido com sucesso ###")
    except Exception:
        logger.exception("### PIPELINE falhou (fail-fast) ###")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
