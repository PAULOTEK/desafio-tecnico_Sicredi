"""Job: gera a massa sintetica na landing zone."""

from __future__ import annotations

from src.ingestao.gerador_dados import gerar_massa
from src.jobs._base import montar_config, parsear_args


def main() -> None:
    args = parsear_args("Gera massa sintetica da Cooperativa NovaRota")
    config = montar_config(args)
    gerar_massa(config.dir_landing)


if __name__ == "__main__":
    main()
