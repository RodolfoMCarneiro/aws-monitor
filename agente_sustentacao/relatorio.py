"""Relatório determinístico (sem LLM): base de comparação e fallback."""


def _tabela(cabecalho: list[str], linhas: list[list]) -> str:
    if not linhas:
        return "_Nenhuma falha no período._\n"
    out = ["| " + " | ".join(cabecalho) + " |", "|" + "---|" * len(cabecalho)]
    for l in linhas:
        out.append("| " + " | ".join(str(c).replace("|", "/").replace("\n", " ") for c in l) + " |")
    return "\n".join(out) + "\n"


def _erros(secao: dict) -> str:
    if "erro_coleta" in secao:
        return f"> ⚠️ Coleta falhou: {secao['erro_coleta']}\n"
    if secao.get("erros_coleta"):
        return f"> ⚠️ {len(secao['erros_coleta'])} recurso(s) não puderam ser lidos.\n"
    return ""


def _pipelines(p: dict) -> list[str]:
    md = ["## Pipelines (tabela de controle)", _erros(p)]
    if "erro_coleta" in p:
        return md
    cont = " · ".join(f"{k}: {v}" for k, v in p["por_situacao"].items())
    md.append(f"`{p['tabela']}` · referência {p['data_referencia_utc']} (UTC) · {p['total_pipelines']} pipelines · {cont}\n")
    md.append(_tabela(
        ["Situação", "Pipeline", "Dataset", "Time", "Etapa", "Detalhe", "Dias desde execução"],
        [[x["situacao"], x["table_name"], x["dataset"], x["team"], x["etapa"], x["detalhe"], x["dias_desde_execucao"]]
         for x in p["com_problema"]],
    ))
    if p["em_execucao"]:
        md.append(f"_{len(p['em_execucao'])} pipeline(s) em execução dentro do tempo esperado._\n")
    return md


def _dossies(d: dict) -> list[str]:
    md = ["## Dossiês das falhas", _erros(d)]
    if "erro_coleta" in d:
        return md
    if not d.get("dossies"):
        md.append("_Nenhuma falha ou execução travada para detalhar._\n")
    for x in d.get("dossies", []):
        cl = x.get("classificacao") or {}
        ex, sf, emr = x.get("execucao") or {}, x.get("step_function") or {}, x.get("emr") or {}
        md.append(f"### {x['table_name']} — {x['situacao']} em {x.get('etapa')} ({x.get('data_execucao')})")
        if cl:
            md.append(f"- **Classificação:** {cl['categoria']}" + (f" — {cl['acao_sugerida']}" if cl.get("acao_sugerida") else ""))
        if x.get("aviso"):
            md.append(f"- ⚠️ {x['aviso']}")
        if ex:
            md.append(f"- **Execução:** `{ex.get('step_name')}` / `{ex.get('id_state_machine')}` ({ex.get('pipeline_status')})")
        if sf.get("erro") or sf.get("causa"):
            md.append(f"- **Step Function:** {sf.get('erro')}: {(sf.get('causa') or '')[:300]}")
        if emr.get("cluster_id"):
            md.append(f"- **EMR:** `{emr['cluster_id']}` / `{emr.get('step_id')}` {emr.get('estado', '')} {emr.get('motivo') or emr.get('erro') or ''}")
        for log in x.get("logs", []):
            if log.get("trecho"):
                md += [f"- **Log** `{log['uri']}`", "```", "\n".join(log["trecho"].splitlines()[-25:]), "```"]
            else:
                md.append(f"- **Log** `{log['uri']}`: {log.get('erro')}")
        md.append("")
    if d.get("nao_detalhados"):
        md.append(f"_{d['nao_detalhados']} falha(s) além do limite MAX_DOSSIES não detalhadas._\n")
    return md


def resumo_md(c: dict) -> str:
    md = ["# Sustentação do data lake",
          f"Região: {c['regiao']} · gerado em {c['gerado_em_utc']} · horários em UTC\n"]
    md += _pipelines(c.get("pipelines", {}))
    md += _dossies(c.get("dossies", {}))
    return "\n".join(md)
