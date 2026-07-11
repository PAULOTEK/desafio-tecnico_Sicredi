"""Job: transformacao Prata."""

from __future__ import annotations

from novarota.common.spark import criar_spark
from novarota.jobs._base import montar_config, parsear_args
from novarota.transformacao.prata import executar_prata


def main() -> None:
    args = parsear_args("Transformacao Prata (limpeza, qualidade, SCD2)")
    config = montar_config(args)
    spark = criar_spark("novarota-prata", config)
    try:
        executar_prata(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
