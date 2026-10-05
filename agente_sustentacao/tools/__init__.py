"""Registro: coleta determinística + tools de detalhe expostas ao LLM.

A coleta segue o caminho do analista: triagem da tabela de controle e dossiê de cada
falha (ver pipelines.py e dossie.py).

Regras para tools de detalhe (camada 2):
- O LLM nunca recebe clients boto3 nem escolhe operação da AWS.
- Cada tool recebe UM id, validado por regex E pela presença na coleta.
- Toda tool nova passa por revisão de código.
"""
import json
import re
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError

from config import CFG

from .dossie import gerar_dossies
from .pipelines import pipelines_triagem

# nome -> (função, parâmetro, regex, descrição). O dossiê já traz o detalhe de cada
# falha; aqui entram tools sob demanda (ver checklist no README).
DETALHE: dict[str, tuple] = {}

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": nome,
            "description": desc + " Só aceita IDs que aparecem na coleta.",
            "parameters": {
                "type": "object",
                "properties": {param: {"type": "string"}},
                "required": [param],
            },
        },
    }
    for nome, (_, param, _, desc) in DETALHE.items()
]


def _json(obj) -> str:
    return json.dumps(obj, default=str, ensure_ascii=False)


def coletar() -> dict:
    resultado = {
        "regiao": CFG.aws_region,
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    try:
        resultado["pipelines"] = pipelines_triagem()
    except (ClientError, BotoCoreError) as e:
        resultado["pipelines"] = {"erro_coleta": f"{type(e).__name__}: {e}"}
    if "com_problema" in resultado["pipelines"]:
        try:
            resultado["dossies"] = gerar_dossies(resultado["pipelines"]["com_problema"])
        except (ClientError, BotoCoreError, RuntimeError) as e:
            resultado["dossies"] = {"erro_coleta": f"{type(e).__name__}: {e}"}
    return resultado


def executar(nome: str, argumentos: str, ids_permitidos: set[str]) -> str:
    """Executa uma tool de detalhe. Erros voltam como texto para o LLM."""
    if nome not in DETALHE:
        return _json({"erro": f"tool desconhecida: {nome}"})
    fn, param, padrao, _ = DETALHE[nome]

    try:
        args = json.loads(argumentos or "{}")
    except json.JSONDecodeError:
        return _json({"erro": "argumentos não são JSON válido"})
    valor = args.get(param) if isinstance(args, dict) else None

    if not isinstance(valor, str) or not re.fullmatch(padrao, valor):
        return _json({"erro": f"'{param}' ausente ou em formato inválido"})
    if valor not in ids_permitidos:
        return _json({"erro": f"{param} não aparece na coleta; só é possível detalhar IDs coletados"})

    try:
        texto = _json(fn(valor))
    except (ClientError, BotoCoreError) as e:
        texto = _json({"erro": f"{type(e).__name__}: {e}"})
    if len(texto) > CFG.max_chars_tool:
        texto = texto[: CFG.max_chars_tool] + "…[truncado]"
    return texto
