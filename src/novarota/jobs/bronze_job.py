"""Job: ingestao Bronze."""

from __future__ import annotations

from novarota.common.spark import criar_spark
from novarota.ingestao.bronze import executar_bronze
from novarota.jobs._base import montar_config, parsear_args


def main() -> None:
    args = parsear_args("Ingestao Bronze (Delta Lake)")
    config = montar_config(args)
    spark = criar_spark("novarota-bronze", config)
    try:
        executar_bronze(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
