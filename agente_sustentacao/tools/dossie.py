"""Dossiê de cada pipeline com problema, seguindo o caminho do analista (somente leitura):

tabela de métricas (DynamoDB) -> execução da Step Function -> step do EMR -> stderr no S3
(-> log do driver, quando o Spark roda em cluster mode e o stderr do step não explica).
"""
import gzip
import re
from functools import lru_cache

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import BotoCoreError, ClientError

from config import CFG
from util import client, trunc

from .padroes import classificar_erro

CAMPOS_METRICAS = ("table_name", "dataset", "id_cluster", "id_step_cluster", "pipeline_status",
                   "dt_update", "hr_update", "preupdate_timestamp", "postupdate_timestamp")
RX_LINHA_ERRO = re.compile(r"Exception|Error|Caused by|Traceback|FAILED|Killed|exit ?code", re.I)
RX_APP = re.compile(r"application_\d+_\d+")
RX_S3 = re.compile(r"s3[an]?://([^/]+)/?(.*)")
ERROS_AWS = (ClientError, BotoCoreError)

_des = TypeDeserializer()


# ---------- tabela de métricas ----------

def _checar_indice() -> None:
    """O GSI precisa projetar as colunas usadas; senão a busca voltaria vazia sem aviso."""
    tabela = client("dynamodb").describe_table(TableName=CFG.tabela_metricas)["Table"]
    idx = next((i for i in tabela.get("GlobalSecondaryIndexes", []) if i["IndexName"] == CFG.indice_metricas), None)
    if idx is None:
        raise RuntimeError(f"índice {CFG.indice_metricas} não existe em {CFG.tabela_metricas}")
    proj = idx["Projection"]
    faltando = [] if proj["ProjectionType"] == "ALL" else [
        c for c in CAMPOS_METRICAS if c not in proj.get("NonKeyAttributes", []) and c not in ("dt_update", "hr_update")]
    if faltando:
        raise RuntimeError(f"índice {CFG.indice_metricas} não projeta {faltando}")


@lru_cache
def _metricas_do_dia(dia: str) -> dict[str, list[dict]]:
    """Todas as execuções do dia, agrupadas por table_name (uma Query por dia, não por pipeline)."""
    grupos: dict[str, list[dict]] = {}
    pag = client("dynamodb").get_paginator("query").paginate(
        TableName=CFG.tabela_metricas,
        IndexName=CFG.indice_metricas,
        KeyConditionExpression="#d = :d",
        ExpressionAttributeNames={"#d": "dt_update"},
        ExpressionAttributeValues={":d": {"S": dia}},
    )
    for p in pag:
        for raw in p["Items"]:
            m = {k: _des.deserialize(v) for k, v in raw.items()}
            grupos.setdefault(m.get("table_name"), []).append(m)
    return grupos


def _escolher_execucao(execucoes: list[dict], etapa: str | None) -> tuple[dict | None, list[dict]]:
    """Prioriza a execução da etapa com problema, depois status != OK, depois a mais recente."""
    dica = next((e.step_name for e in CFG.etapas if e.nome == etapa and e.step_name), None)
    candidatas = [m for m in execucoes if dica and dica in (m.get("step_name") or "")] or execucoes
    ordenadas = sorted(candidatas, key=lambda m: m.get("hr_update") or "", reverse=True)
    ordenadas.sort(key=lambda m: (m.get("pipeline_status") or "").upper() in CFG.status_ok)
    return (ordenadas[0] if ordenadas else None), ordenadas[1:6]


# ---------- Step Functions ----------

@lru_cache
def _arns_state_machines() -> dict[str, str]:
    arns = {}
    for p in client("stepfunctions").get_paginator("list_state_machines").paginate():
        arns.update({sm["name"]: sm["stateMachineArn"] for sm in p["stateMachines"]})
    return arns


def _step_function(step_name: str, id_execucao: str) -> dict:
    sm_arn = _arns_state_machines().get(step_name)
    if not sm_arn:
        return {"erro": f"state machine {step_name} não encontrada"}
    arn = sm_arn.replace(":stateMachine:", ":execution:") + ":" + id_execucao
    try:
        d = client("stepfunctions").describe_execution(executionArn=arn)
    except ERROS_AWS as e:
        return {"execution_arn": arn, "erro": f"{type(e).__name__}: {e}"}
    return {"execution_arn": arn, "status": d["status"], "inicio": d["startDate"], "fim": d.get("stopDate"),
            "erro": d.get("error"), "causa": trunc(d.get("cause"), 1500)}


# ---------- EMR + logs ----------

@lru_cache
def _log_uri(cluster_id: str) -> str | None:
    uri = client("emr").describe_cluster(ClusterId=cluster_id)["Cluster"].get("LogUri")
    return uri.rstrip("/") + "/" if uri else None


def _emr_step(cluster_id: str, step_id: str) -> dict:
    s = client("emr").describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]
    st, fd = s["Status"], s["Status"].get("FailureDetails", {})
    return {
        "cluster_id": cluster_id, "step_id": step_id, "nome": s.get("Name"), "estado": st["State"],
        "motivo": trunc(fd.get("Reason"), 500), "mensagem": trunc(fd.get("Message"), 1500),
        "log_file": fd.get("LogFile"),
        "args": trunc(" ".join(s.get("Config", {}).get("Args", [])), 800),
        "inicio": st.get("Timeline", {}).get("StartDateTime"), "fim": st.get("Timeline", {}).get("EndDateTime"),
    }


def _ler_s3(uri: str) -> str:
    m = RX_S3.match(uri)
    if not m:
        raise ValueError(f"URI inválida: {uri}")
    obj = client("s3").get_object(Bucket=m.group(1), Key=m.group(2))
    if obj["ContentLength"] > CFG.max_mb_log * 1024 * 1024:
        return f"[log com {obj['ContentLength'] // 1024 // 1024} MB ignorado; limite MAX_MB_LOG={CFG.max_mb_log}]"
    dados = obj["Body"].read()
    if uri.endswith(".gz"):
        dados = gzip.decompress(dados)
    return dados.decode("utf-8", errors="replace")


def trecho_erro(texto: str, max_chars: int = 4000) -> str:
    """Primeiras ocorrências (causa raiz) + últimas (desfecho) + cauda do log."""
    linhas = texto.splitlines()
    idx = [i for i, l in enumerate(linhas) if RX_LINHA_ERRO.search(l)]
    sel = {j for i in idx[:10] + idx[-10:] for j in range(i, min(i + 4, len(linhas)))}
    sel |= set(range(max(0, len(linhas) - 15), len(linhas)))
    out, anterior = [], None
    for i in sorted(sel):
        if anterior is not None and i != anterior + 1:
            out.append("...")
        out.append(linhas[i][:400])
        anterior = i
    return trunc("\n".join(out), max_chars)


def _log_driver(base: str, cluster_id: str, app_id: str) -> str | None:
    """Em cluster mode, a exceção real fica no container do driver (sufixo _000001)."""
    m = RX_S3.match(f"{base}{cluster_id}/containers/{app_id}/")
    r = client("s3").list_objects_v2(Bucket=m.group(1), Prefix=m.group(2), MaxKeys=1000)
    chaves = [o["Key"] for o in r.get("Contents", []) if re.search(r"_000001/stderr(\.gz)?$", o["Key"])]
    return f"s3://{m.group(1)}/{chaves[0]}" if chaves else None


def _logs(emr: dict) -> list[dict]:
    base = _log_uri(emr["cluster_id"])
    uris = []
    if emr.get("log_file"):
        uris.append(emr["log_file"])
    if base:
        uris.append(f"{base}{emr['cluster_id']}/steps/{emr['step_id']}/stderr.gz")
    logs = []
    for uri in dict.fromkeys(uris):
        try:
            texto = _ler_s3(uri)
            logs.append({"uri": uri, "trecho": trecho_erro(texto), "_completo": texto})
        except (*ERROS_AWS, ValueError, OSError) as e:
            logs.append({"uri": uri, "erro": f"{type(e).__name__}: {e}"})

    texto_total = " ".join(l.get("_completo", "") for l in logs)
    app = RX_APP.search(texto_total)
    if base and app and classificar_erro(texto_total)["categoria"] == "NAO_CLASSIFICADO":
        try:
            uri = _log_driver(base, emr["cluster_id"], app.group(0))
            if uri:
                texto = _ler_s3(uri)
                logs.append({"uri": uri, "trecho": trecho_erro(texto), "_completo": texto})
        except (*ERROS_AWS, ValueError, OSError) as e:
            logs.append({"uri": f"driver {app.group(0)}", "erro": f"{type(e).__name__}: {e}"})
    return logs


# ---------- montagem ----------

def dossie(p: dict) -> dict:
    etapa_info = (p.get("etapas") or {}).get(p.get("etapa")) or {}
    ref = etapa_info.get("inicio") or p.get("ultima_execucao")
    dia = ref.strftime("%Y-%m-%d") if ref else None
    d = {"table_name": p["table_name"], "dataset": p.get("dataset"), "situacao": p["situacao"],
         "etapa": p.get("etapa"), "detalhe": p.get("detalhe"), "data_execucao": dia}
    if not dia:
        return {**d, "aviso": "sem data de execução para buscar na tabela de métricas"}

    execucao, outras = _escolher_execucao(_metricas_do_dia(dia).get(p["table_name"], []), p.get("etapa"))
    d["outras_execucoes_do_dia"] = [
        {k: m.get(k) for k in ("step_name", "id_state_machine", "hr_update", "pipeline_status")} for m in outras]
    if not execucao:
        return {**d, "aviso": f"nenhuma execução de {p['table_name']} em {dia} na tabela de métricas"}
    d["execucao"] = {k: execucao.get(k) for k in ("step_name", "id_state_machine", "pipeline_status",
                                                  "preupdate_timestamp", "postupdate_timestamp")}

    d["step_function"] = _step_function(execucao["step_name"], execucao["id_state_machine"])

    texto_classificacao = " ".join(str(d["step_function"].get(k) or "") for k in ("erro", "causa"))
    if execucao.get("id_cluster") and execucao.get("id_step_cluster"):
        try:
            d["emr"] = _emr_step(execucao["id_cluster"], execucao["id_step_cluster"])
            d["logs"] = _logs(d["emr"])
            texto_classificacao += " " + " ".join(
                [d["emr"].get("motivo") or "", d["emr"].get("mensagem") or ""]
                + [l.pop("_completo", "") for l in d["logs"]])
        except ERROS_AWS as e:
            d["emr"] = {"cluster_id": execucao["id_cluster"], "step_id": execucao["id_step_cluster"],
                        "erro": f"{type(e).__name__}: {e}"}
    else:
        d["emr"] = {"aviso": "execução sem id_cluster/id_step_cluster na tabela de métricas"}

    d["classificacao"] = classificar_erro(texto_classificacao)
    return d


def gerar_dossies(com_problema: list[dict]) -> dict:
    alvos = [p for p in com_problema if p["situacao"] in ("FALHA", "TRAVADO")]
    _checar_indice()
    dossies, erros = [], []
    for p in alvos[: CFG.max_dossies]:
        try:
            dossies.append(dossie(p))
        except ERROS_AWS as e:
            erros.append({"table_name": p["table_name"], "erro": f"{type(e).__name__}: {e}"})
    return {"dossies": dossies, "nao_detalhados": max(0, len(alvos) - CFG.max_dossies), "erros_coleta": erros}
