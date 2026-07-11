"""Job: modelagem Ouro."""

from __future__ import annotations

from src.common.spark import criar_spark
from src.jobs._base import montar_config, parsear_args
from src.transformacao.ouro import executar_ouro


def main() -> None:
    args = parsear_args("Modelagem Ouro (fato, dimensoes, features)")
    config = montar_config(args)
    spark = criar_spark("novarota-ouro", config)
    try:
        executar_ouro(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
