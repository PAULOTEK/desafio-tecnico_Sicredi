"""Configuracao parametrizavel do pipeline.

Toda a parametrizacao (catalogo, schemas, caminhos, datas, modo de execucao e
batch_id) fica concentrada aqui, evitando valores hardcoded espalhados pelo
codigo. Os valores podem vir de tres fontes, em ordem crescente de prioridade:

1. valores padrao definidos neste modulo;
2. arquivo YAML (``config/config.yaml`` por padrao);
3. variaveis de ambiente prefixadas com ``NOVAROTA_``.

Exemplo::

    export NOVAROTA_MODO_EXECUCAO=incremental
    export NOVAROTA_DATA_REFERENCIA=2024-03-01
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field, fields
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

# Raiz do projeto (dois niveis acima deste arquivo: src/novarota/config.py).
RAIZ_PROJETO = Path(__file__).resolve().parents[2]

MODOS_EXECUCAO_VALIDOS = {"full", "incremental"}


@dataclass
class Config:
    """Parametros de execucao do pipeline NovaRota."""

    # Catalogo/schema no estilo Unity Catalog (catalog.schema.table).
    catalogo: str = "novarota"
    schema_bronze: str = "bronze"
    schema_prata: str = "prata"
    schema_ouro: str = "ouro"

    # Quando True, os nomes das tabelas sao qualificados com o catalogo
    # (``catalogo.schema.tabela``) — modo Unity Catalog no Databricks. Quando
    # False (padrao), usamos ``schema.tabela``, pois o metastore local do Spark
    # (execucao fora do Databricks) nao suporta catalogos de tres niveis.
    usar_catalogo: bool = False

    # Caminhos do lakehouse (padrao local). Em Databricks o notebook aponta o
    # landing para um Volume (ex.: /Volumes/novarota/bronze/landing) e as tabelas
    # sao gerenciadas pelo Unity Catalog. Caminhos absolutos (inclusive /Volumes)
    # passados via YAML/env sao respeitados como estao (ver _normalizar).
    dir_dados: Path = RAIZ_PROJETO / "data"
    dir_landing: Path = RAIZ_PROJETO / "data" / "landing"
    dir_lakehouse: Path = RAIZ_PROJETO / "data" / "lakehouse"
    dir_warehouse: Path = RAIZ_PROJETO / "data" / "spark-warehouse"

    # Controle de carga.
    modo_execucao: str = "full"  # full | incremental
    data_referencia: date = field(default_factory=date.today)
    batch_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d%H%M%S"))

    def __post_init__(self) -> None:
        self._normalizar()
        self._validar()

    # ------------------------------------------------------------------ #
    # Construcao
    # ------------------------------------------------------------------ #
    @classmethod
    def carregar(cls, caminho_yaml: str | os.PathLike[str] | None = None) -> Config:
        """Monta a configuracao combinando YAML e variaveis de ambiente."""

        parametros: dict[str, Any] = {}

        caminho = Path(caminho_yaml) if caminho_yaml else RAIZ_PROJETO / "config" / "config.yaml"
        if caminho.exists():
            conteudo = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
            parametros.update(conteudo)

        parametros.update(cls._ler_variaveis_ambiente())
        return cls(**parametros)

    @staticmethod
    def _ler_variaveis_ambiente() -> dict[str, Any]:
        nomes = {f.name for f in fields(Config)}
        coletados: dict[str, Any] = {}
        for nome in nomes:
            chave = f"NOVAROTA_{nome.upper()}"
            if chave in os.environ:
                coletados[nome] = os.environ[chave]
        return coletados

    # ------------------------------------------------------------------ #
    # Normalizacao e validacao
    # ------------------------------------------------------------------ #
    def _normalizar(self) -> None:
        # Caminhos relativos sao resolvidos a partir da raiz do projeto, para
        # que os jobs funcionem independentemente do diretorio de execucao.
        def _resolver(valor: Path | str) -> Path:
            caminho = Path(valor)

            if str(caminho).startswith("/Volumes"):
                return caminho

            if caminho.is_absolute():
                return caminho

            return RAIZ_PROJETO / caminho

        self.dir_dados = _resolver(self.dir_dados)
        self.dir_landing = _resolver(self.dir_landing)
        self.dir_lakehouse = _resolver(self.dir_lakehouse)
        self.dir_warehouse = _resolver(self.dir_warehouse)

        if isinstance(self.data_referencia, str):
            self.data_referencia = date.fromisoformat(self.data_referencia)
        if isinstance(self.data_referencia, datetime):
            self.data_referencia = self.data_referencia.date()

        self.modo_execucao = str(self.modo_execucao).lower().strip()
        self.batch_id = str(self.batch_id)

        if isinstance(self.usar_catalogo, str):
            self.usar_catalogo = self.usar_catalogo.strip().lower() in {
                "1", "true", "yes", "sim", "y", "s"
            }
        else:
            self.usar_catalogo = bool(self.usar_catalogo)

    def _validar(self) -> None:
        if self.modo_execucao not in MODOS_EXECUCAO_VALIDOS:
            raise ValueError(
                f"modo_execucao invalido: {self.modo_execucao!r}. "
                f"Valores aceitos: {sorted(MODOS_EXECUCAO_VALIDOS)}"
            )

    # ------------------------------------------------------------------ #
    # Utilitarios
    # ------------------------------------------------------------------ #
    def schema_qualificado(self, schema: str) -> str:
        """Retorna o schema qualificado conforme o ambiente.

        No Databricks/Unity Catalog (``usar_catalogo=True``) inclui o catalogo
        (``catalogo.schema``); localmente retorna apenas ``schema``.
        """

        return f"{self.catalogo}.{schema}" if self.usar_catalogo else schema

    def tabela(self, schema: str, nome: str) -> str:
        """Retorna o nome totalmente qualificado da tabela.

        No Databricks/Unity Catalog (``usar_catalogo=True``) usamos tres niveis
        (``catalogo.schema.tabela``). Localmente usamos ``schema.tabela``,
        porque o metastore local do Spark nao suporta catalogos de tres niveis.
        """

        return f"{self.schema_qualificado(schema)}.{nome}"

    def novo_batch_id(self) -> str:
        """Gera um batch_id unico caso seja necessario isolar reprocessos."""

        return f"{self.batch_id}-{uuid.uuid4().hex[:8]}"
