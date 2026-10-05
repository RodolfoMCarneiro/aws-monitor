"""Ponto de entrada: python main.py [--sem-llm]"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from config import CFG
from relatorio import resumo_md
from tools import coletar


def main() -> None:
    ap = argparse.ArgumentParser(description="Avaliação de sustentação do data lake")
    ap.add_argument("--sem-llm", action="store_true", help="só coleta + relatório determinístico")
    ap.add_argument("--saida", default="saida", help="pasta de saída")
    args = ap.parse_args()

    pasta = Path(args.saida)
    pasta.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"Coletando em {CFG.aws_region}...", file=sys.stderr)
    coleta = coletar()
    (pasta / f"coleta_{ts}.json").write_text(json.dumps(coleta, default=str, ensure_ascii=False, indent=2), encoding="utf-8")

    base = resumo_md(coleta)
    (pasta / f"resumo_{ts}.md").write_text(base, encoding="utf-8")

    if args.sem_llm:
        print(pasta / f"resumo_{ts}.md")
        return

    from agente import analisar  # import tardio: --sem-llm não exige litellm

    relatorio, trace = analisar(coleta)
    (pasta / f"trace_{ts}.json").write_text(json.dumps(trace, default=str, ensure_ascii=False, indent=2), encoding="utf-8")
    (pasta / f"relatorio_{ts}.md").write_text(relatorio, encoding="utf-8")
    print(pasta / f"relatorio_{ts}.md")


if __name__ == "__main__":
    main()
