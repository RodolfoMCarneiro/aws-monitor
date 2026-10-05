"""Catálogo de erros conhecidos: regex sobre o log/mensagem -> categoria e ação padrão.

Ordem importa: o primeiro padrão que casar define a categoria.
Novos padrões entram aqui à medida que a sustentação identifica recorrências.
"""
import re

PADROES = [
    (r"OutOfMemoryError|Java heap space|Container killed by YARN for exceeding|exit code 137|GC overhead limit",
     "MEMORIA", "Aumentar memória de driver/executor ou revisar particionamento e skew; reprocessar."),
    (r"ExecutorLostFailure|Lost executor|Spot|instance.{0,40}terminat|Decommission",
     "PERDA_DE_NO", "Verificar perda de nós (Spot/decommission) no cluster; reprocessar."),
    (r"Path does not exist|NoSuchKey|FileNotFoundException|The specified key does not exist",
     "ORIGEM_AUSENTE", "Confirmar se a origem foi entregue no S3; reprocessar após a chegada."),
    (r"UNRESOLVED_COLUMN|cannot resolve|Column .{0,80} does not exist|schema mismatch|Parquet column cannot be converted|CANNOT_UP_CAST",
     "SCHEMA", "Mudança de schema na origem: validar com o time responsável e ajustar o mapeamento."),
    (r"AccessDenied|Access Denied|not authorized|KMS.{0,40}(denied|disabled)",
     "PERMISSAO", "Verificar role do EMR, bucket policy e chave KMS."),
    (r"SlowDown|Rate exceeded|Throttl|503 Service Unavailable",
     "THROTTLING", "Reprocessar; se recorrente, reduzir paralelismo ou escalonar horários."),
    (r"Connection refused|Connection timed out|UnknownHostException|Communications link failure|SocketTimeout",
     "CONEXAO", "Verificar disponibilidade e rede da origem/destino; reprocessar."),
]
_COMPILADOS = [(re.compile(p, re.I), cat, acao) for p, cat, acao in PADROES]


def classificar_erro(texto: str | None) -> dict:
    for rx, cat, acao in _COMPILADOS:
        m = rx.search(texto or "")
        if m:
            return {"categoria": cat, "acao_sugerida": acao, "evidencia": m.group(0)}
    return {"categoria": "NAO_CLASSIFICADO", "acao_sugerida": None, "evidencia": None}
