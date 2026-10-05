"""Triagem pela tabela de última execução dos pipelines (DynamoDB, somente leitura).

Cada item é um pipeline (PK table_name) com colunas de início/fim/status por etapa
(ver config.ETAPAS). Nem todo pipeline tem todas as etapas, e colunas antigas de
etapas que não rodam mais continuam no item. Por isso:
- etapa sem colunas no item é ignorada;
- só são avaliadas as etapas da última execução (mesmo dia da atividade mais recente);
- etapa antiga só gera alerta se outra etapa depende dela (depende_de).
"""
from datetime import date, datetime

from boto3.dynamodb.types import TypeDeserializer

from config import CFG
from util import agora_utc, client

FORMATO_DATA = "%Y-%m-%d %H:%M:%S"
CAMPOS_FIXOS = ("table_name", "dataset", "team", "dh_criacao_emr", "pipeline_status")
GRAVIDADE = {"FALHA": 0, "TRAVADO": 1, "NAO_EXECUTOU": 2, "SEM_DADOS": 3, "EXECUTANDO": 4, "OK": 5}

_des = TypeDeserializer()


def _data(valor) -> datetime | None:
    try:
        return datetime.strptime(str(valor), FORMATO_DATA)
    except (TypeError, ValueError):
        return None


def _scan() -> list[dict]:
    """Scan só com as colunas usadas. FilterExpression não reduz custo de Scan
    e esconderia os pipelines FINISHED que não rodaram hoje, então filtramos aqui."""
    campos = list(CAMPOS_FIXOS) + [c for e in CFG.etapas for c in (e.inicio, e.fim, e.status)]
    nomes = {f"#c{i}": c for i, c in enumerate(campos)}
    itens = []
    pag = client("dynamodb").get_paginator("scan").paginate(
        TableName=CFG.tabela_ultima_execucao,
        ProjectionExpression=", ".join(nomes),
        ExpressionAttributeNames=nomes,
    )
    for p in pag:
        itens.extend({k: _des.deserialize(v) for k, v in i.items()} for i in p["Items"])
    return itens


def classificar(item: dict, agora: datetime, hoje: date) -> dict:
    etapas = {
        e.nome: {"status": item.get(e.status), "inicio": _data(item.get(e.inicio)), "fim": _data(item.get(e.fim))}
        for e in CFG.etapas
        if item.get(e.status) is not None or item.get(e.inicio) is not None
    }
    inicios = [e["inicio"] for e in etapas.values() if e["inicio"]]
    ultima = max(inicios) if inicios else None

    def r(situacao: str, etapa: str | None, detalhe: str) -> dict:
        return {**{c: item.get(c) for c in CAMPOS_FIXOS},
                "situacao": situacao, "etapa": etapa, "detalhe": detalhe,
                "ultima_execucao": ultima, "dias_desde_execucao": (hoje - ultima.date()).days if ultima else None,
                "etapas": etapas}

    if not etapas or ultima is None:
        return r("SEM_DADOS", None, "nenhuma etapa conhecida com data de início")

    # etapas da última execução; sem data de início entra também (não dá para descartar)
    atuais = [n for n, e in etapas.items() if e["inicio"] is None or e["inicio"].date() == ultima.date()]
    for nome in atuais:
        e = etapas[nome]
        st = (e["status"] or "").upper()
        if st in CFG.status_executando:
            if e["inicio"] and (agora - e["inicio"]).total_seconds() > CFG.max_horas_execucao * 3600:
                return r("TRAVADO", nome, f"{st} desde {e['inicio']}")
            return r("EXECUTANDO", nome, f"{st} desde {e['inicio']}")
        if st not in CFG.status_ok:
            return r("FALHA", nome, f"status={e['status'] or 'ausente'}")

    for et in CFG.etapas:
        dep = etapas.get(et.depende_de) if et.depende_de else None
        if et.nome in etapas and dep and dep["inicio"]:
            ini = etapas[et.nome]["inicio"]
            if ini is None or ini < dep["inicio"]:
                return r("NAO_EXECUTOU", et.nome, f"{et.nome} não rodou após o {et.depende_de} de {dep['inicio']}")

    if ultima.date() < hoje:
        etapa = next(n for n, e in etapas.items() if e["inicio"] == ultima)
        return r("NAO_EXECUTOU", etapa, f"última atividade em {ultima.date()}")
    return r("OK", None, "")


def pipelines_triagem(agora: datetime | None = None) -> dict:
    agora = agora or agora_utc()
    hoje = agora.date()
    avaliados = sorted(
        (classificar(i, agora, hoje) for i in _scan()),
        key=lambda x: (GRAVIDADE[x["situacao"]], x.get("dataset") or "", x.get("table_name") or ""),
    )
    contagem = {s: 0 for s in GRAVIDADE}
    for a in avaliados:
        contagem[a["situacao"]] += 1
    return {
        "tabela": CFG.tabela_ultima_execucao,
        "data_referencia_utc": hoje,
        "total_pipelines": len(avaliados),
        "por_situacao": contagem,
        # OK fica só na contagem para não inflar a coleta enviada ao LLM
        "com_problema": [a for a in avaliados if a["situacao"] not in ("OK", "EXECUTANDO")],
        "em_execucao": [a for a in avaliados if a["situacao"] == "EXECUTANDO"],
    }
