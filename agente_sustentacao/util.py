"""Utilitários compartilhados."""
import re
from datetime import datetime, timezone
from functools import lru_cache

import boto3
from botocore.config import Config as BotoConfig

from config import CFG

# Formatos de ID que podem aparecer na coleta (base para validar tools de detalhe)
RE_SFN_EXEC = r"arn:aws[a-z-]*:states:[a-z0-9-]+:\d{12}:execution:[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+"
RE_EMR_CLUSTER = r"j-[A-Z0-9]{6,20}"
RE_EMR_STEP = r"s-[A-Z0-9]{6,30}"


@lru_cache
def _sessao() -> boto3.Session:
    return boto3.Session(profile_name=CFG.aws_profile, region_name=CFG.aws_region)


_BOTO_CFG = BotoConfig(retries={"mode": "adaptive", "max_attempts": 10})


@lru_cache
def client(servico: str):
    """Clients só são criados aqui e só usados dentro das tools."""
    return _sessao().client(servico, config=_BOTO_CFG)


def agora_utc() -> datetime:
    """As tabelas de controle gravam em UTC sem fuso; comparamos no mesmo formato."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def trunc(texto, n: int = 500) -> str | None:
    if texto is None:
        return None
    texto = str(texto)
    return texto if len(texto) <= n else texto[:n] + "…[truncado]"


def extrair_ids(texto: str) -> set[str]:
    """IDs presentes na coleta: o LLM só pode detalhar o que apareceu nela."""
    ids: set[str] = set()
    for padrao in (RE_SFN_EXEC, RE_EMR_CLUSTER, RE_EMR_STEP):
        ids.update(re.findall(padrao, texto))
    return ids
