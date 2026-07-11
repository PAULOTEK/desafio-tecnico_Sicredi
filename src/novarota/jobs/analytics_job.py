"""Job: executa as consultas SQL avancadas em ``sql/analytics``.

Le cada arquivo ``.sql``, executa suas instrucoes (multiplas por arquivo sao
separadas por ``;``) e exibe o resultado das consultas ``SELECT``. Serve como
camada de execucao/demonstracao do SQL avancado exigido no desafio.
"""

from __future__ import annotations

from pathlib import Path

from novarota.common.logging_config import obter_logger
from novarota.common.spark import criar_spark
from novarota.config import RAIZ_PROJETO
from novarota.jobs._base import montar_config, parsear_args

logger = obter_logger("novarota.analytics")

DIR_SQL = RAIZ_PROJETO / "sql" / "analytics"


def _instrucoes(texto: str) -> list[str]:
    # Remove comentarios de linha e separa por ';'.
    linhas = [ln for ln in texto.splitlines() if not ln.strip().startswith("--")]
    conteudo = "\n".join(linhas)
    return [i.strip() for i in conteudo.split(";") if i.strip()]


def main() -> None:
    args = parsear_args("Executa consultas SQL avancadas (camada Ouro/Prata)")
    config = montar_config(args)
    spark = criar_spark("novarota-analytics", config)
    try:
        for arquivo in sorted(DIR_SQL.glob("*.sql")):
            logger.info("=== SQL %s ===", arquivo.name)
            for instrucao in _instrucoes(Path(arquivo).read_text(encoding="utf-8")):
                df = spark.sql(instrucao)
                if df.columns:  # consultas com retorno
                    df.show(50, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
