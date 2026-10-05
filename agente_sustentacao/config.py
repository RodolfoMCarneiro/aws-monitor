"""Configuração via variáveis de ambiente."""
import os
from dataclasses import dataclass, field
from typing import NamedTuple


def _lista(var: str, padrao: str) -> tuple[str, ...]:
    return tuple(x.strip().upper() if var.startswith("STATUS") else x.strip()
                 for x in os.getenv(var, padrao).split(",") if x.strip())


class Etapa(NamedTuple):
    """Etapa da tabela de última execução. As colunas são explícitas porque
    os nomes não seguem um padrão único (ex.: cryptography_a_status_end)."""
    nome: str
    inicio: str
    fim: str
    status: str
    depende_de: str | None = None   # etapa que precisa ter rodado antes, na mesma execução
    step_name: str | None = None    # trecho do step_name na tabela de métricas


# Ordem = ordem de execução. heavy/light_transformation são legado e ficam de fora.
# step_name casa por trecho com as state machines sdlf-meutime-<trecho>.
ETAPAS = (
    Etapa("input", "input_start", "input_end", "input_status", step_name="input-data-sm-a"),
    Etapa("transformation", "transformation_start", "transformation_end", "transformation_status",
          depende_de="input", step_name="transformation-sm-a"),
    Etapa("cryptography_sm_a", "cryptography_sm_a_start", "cryptography_a_status_end", "cryptography_sm_a_status",
          step_name="cryptography-sm-a"),
    Etapa("cryptography_sm_b", "cryptography_sm_b_start", "cryptography_sm_b_end", "cryptography_sm_b_status",
          step_name="cryptography-sm-b"),
)


@dataclass(frozen=True)
class Config:
    aws_profile: str | None = os.getenv("AWS_PROFILE")
    aws_region: str = os.getenv("AWS_REGION", "us-east-2")

    # Tabela de controle dos pipelines (DynamoDB)
    tabela_ultima_execucao: str = os.getenv("TABELA_ULTIMA_EXECUCAO", "job-pipelines-last-executions")
    etapas: tuple[Etapa, ...] = field(default_factory=lambda: tuple(
        e for e in ETAPAS if e.nome in _lista("ETAPAS", ",".join(x.nome for x in ETAPAS))))
    status_ok: tuple[str, ...] = field(default_factory=lambda: _lista("STATUS_OK", "FINISHED"))
    status_executando: tuple[str, ...] = field(default_factory=lambda: _lista("STATUS_EXECUTANDO", "RUNNING,STARTED"))
    max_horas_execucao: float = float(os.getenv("MAX_HORAS_EXECUCAO", "3"))

    # Tabela de métricas (uma linha por execução de Step Function, com cluster e step do EMR)
    tabela_metricas: str = os.getenv("TABELA_METRICAS", "job-metrics")
    indice_metricas: str = os.getenv("INDICE_METRICAS", "last_updated_date-metric-index")
    max_dossies: int = int(os.getenv("MAX_DOSSIES", "30"))
    max_mb_log: float = float(os.getenv("MAX_MB_LOG", "20"))

    # LiteLLM: ex. "litellm_proxy/glm-4.6" (proxy) ou "openai/glm-4.6" + api_base
    modelo: str = os.getenv("LLM_MODEL", "litellm_proxy/glm-4.6")
    api_base: str | None = os.getenv("LLM_API_BASE")
    api_key: str | None = os.getenv("LLM_API_KEY")

    max_iteracoes: int = int(os.getenv("MAX_ITERACOES", "12"))
    max_chars_tool: int = int(os.getenv("MAX_CHARS_TOOL", "12000"))
    max_chars_coleta: int = int(os.getenv("MAX_CHARS_COLETA", "60000"))


CFG = Config()
