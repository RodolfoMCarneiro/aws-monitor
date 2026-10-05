# Agente de sustentação do data lake (AWS)

Automatiza a rotina matinal da sustentação: verifica a última execução de cada pipeline, investiga cada falha até o log do Spark e gera um diagnóstico com plano de ação usando um LLM (GLM via LiteLLM).

## Rotina que o agente reproduz

1. Abrir a tabela de última execução e ver quais pipelines falharam, travaram ou não rodaram.
2. Para cada falha, achar a execução na tabela de métricas, que traz a Step Function, o cluster e o step do EMR.
3. No EMR, ler o stderr do step (e, em cluster mode, o log do driver Spark) para entender o erro.
4. Classificar a causa e decidir a ação.

```
job-pipelines-last-executions   triagem: FALHA / TRAVADO / NAO_EXECUTOU / OK
  └─ job-metrics (GSI dt_update)  execução da etapa com problema
       ├─ Step Functions                  erro e causa da execução
       └─ EMR DescribeStep                motivo da falha do step
            └─ S3 (LogUri do cluster)     stderr do step e, se preciso, log do driver Spark
```

| Etapa | Módulo | Usa LLM? |
|---|---|---|
| Triagem da tabela de controle | `tools/pipelines.py` | Não |
| Dossiê de cada falha | `tools/dossie.py` | Não |
| Classificação por erros conhecidos | `tools/padroes.py` | Não |
| Relatório base | `relatorio.py` | Não |
| Diagnóstico, agrupamento de incidentes e plano de ação | `agente.py` | Sim |

O relatório base é sempre gerado e funciona como fallback se o LLM falhar.

## Fontes de dados

**Tabela de controle** `job-pipelines-last-executions` (PK `table_name`): uma linha por pipeline, com `dataset`, `team`, `pipeline_status` e, para cada etapa, colunas de início, fim e status. As etapas avaliadas estão em `ETAPAS` no `config.py`:

| Etapa | Colunas | State machine |
|---|---|---|
| input | `input_start`, `input_end`, `input_status` | `sdlf-meutime-input-data-sm-a` |
| transformation | `transformation_start`, `transformation_end`, `transformation_status` | `sdlf-meutime-transformation-sm-a` |
| cryptography_sm_a | `cryptography_sm_a_start`, `cryptography_a_status_end`, `cryptography_sm_a_status` | `sdlf-meutime-cryptography-sm-a` |
| cryptography_sm_b | `cryptography_sm_b_start`, `cryptography_sm_b_end`, `cryptography_sm_b_status` | `sdlf-meutime-cryptography-sm-b` |

As colunas `heavy_transformation_*` e `light_transformation_*` não são avaliadas.

**Tabela de métricas** `job-metrics` (PK `step_name`, SK `id_state_machine`): uma linha por execução de Step Function, com `table_name`, `id_cluster`, `id_step_cluster`, `pipeline_status` e `dt_update`/`hr_update` (última atualização). O GSI `last_updated_date-metric-index` (PK `dt_update`, SK `hr_update`, projeção `All`) permite buscar todas as execuções de um dia.

**Logs do EMR**: bucket `<datalake>-<regiao>-<conta>-emr-logs`, com um prefixo por etapa (`input_data/`, `transformation/`, `cryptography_stage_a/`...). Cada cluster grava em `<etapa>/logs/<cluster-id>/` as pastas `steps/`, `containers/` e `node/`. O agente lê o `LogUri` do próprio cluster, então não precisa mapear prefixos.

Todos os horários das tabelas são UTC, e o agente trabalha em UTC.

## Segurança

- Toda chamada à AWS é de leitura (`Scan`, `Query`, `Describe*`, `Get*`, `List*`).
- O LLM não acessa a AWS: recebe a coleta pronta. O log completo não é enviado, só o trecho relevante.
- Logs e mensagens de erro são tratados como dado (instrução explícita no prompt contra prompt injection).
- Tools de detalhe sob demanda podem ser registradas em `tools/__init__.py` (`DETALHE`). Cada uma aceita um ID por chamada, validado por regex e pela presença na coleta. **Toda tool nova exige revisão de código** (checklist no fim deste arquivo).

## Instalação

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
aws sso login --profile seu-perfil
```

## Configuração (variáveis de ambiente)

| Variável | Padrão | Uso |
|---|---|---|
| `AWS_PROFILE` | – | profile SSO |
| `AWS_REGION` | us-east-2 | região |
| `TABELA_ULTIMA_EXECUCAO` | job-pipelines-last-executions | tabela de controle |
| `ETAPAS` | todas de `config.ETAPAS` | subconjunto de etapas a avaliar |
| `STATUS_OK` | FINISHED | status de sucesso |
| `STATUS_EXECUTANDO` | RUNNING,STARTED | status em andamento; qualquer outro vira FALHA |
| `MAX_HORAS_EXECUCAO` | 3 | acima disso uma etapa em andamento vira TRAVADO |
| `TABELA_METRICAS` | job-metrics | tabela de execuções |
| `INDICE_METRICAS` | last_updated_date-metric-index | GSI por data |
| `MAX_DOSSIES` | 30 | máximo de falhas detalhadas por execução |
| `MAX_MB_LOG` | 20 | logs maiores que isso no S3 não são lidos |
| `LLM_MODEL` | litellm_proxy/glm-4.6 | modelo no formato LiteLLM |
| `LLM_API_BASE` | – | URL do proxy LiteLLM |
| `LLM_API_KEY` | – | chave do proxy |
| `MAX_ITERACOES` | 12 | limite de rodadas com o LLM |
| `MAX_CHARS_TOOL` | 12000 | tamanho máximo do retorno de cada tool |
| `MAX_CHARS_COLETA` | 60000 | tamanho máximo da coleta enviada ao LLM |

Etapas novas, ou colunas com outro nome, entram em `ETAPAS` no `config.py`.

## Uso

```bash
python main.py --sem-llm   # só coleta e relatório base (valida o acesso à AWS)
python main.py             # completo, com diagnóstico do LLM
```

Saída em `saida/`: `coleta_*.json`, `resumo_*.md`, `relatorio_*.md` e `trace_*.json` (conversa completa com o LLM, para auditoria).

Teste offline (sem AWS e sem LLM): `python tests/teste_offline.py`

## Permissões IAM usadas

```
dynamodb:Scan                           (tabela de controle)
dynamodb:Query, dynamodb:DescribeTable  (tabela de métricas e seu GSI)
states:ListStateMachines, states:DescribeExecution
elasticmapreduce:DescribeCluster, elasticmapreduce:DescribeStep
s3:GetObject, s3:ListBucket             (bucket de logs do EMR)
```

## Limitações conhecidas

- **NAO_EXECUTOU** não conhece a agenda dos pipelines: um pipeline semanal aparece como não executado nos outros dias.
- O EMR sincroniza logs com o S3 a cada ~5 min: falhas muito recentes ou clusters encerrados de forma abrupta podem não ter log.
- Execuções de Step Function com mais de 90 dias não podem mais ser consultadas.
- Uma região por execução.

## Adicionando uma tool de detalhe (checklist)

1. A função recebe só um ID, nunca operação, SQL ou caminho livre.
2. Usa só chamadas `List*`, `Describe*` ou `Get*`.
3. Retorna dados resumidos (contagens, top N, texto truncado).
4. Registra em `tools/__init__.py` (`DETALHE`) com regex de validação (`util.py`).
5. Adiciona um caso em `tests/teste_offline.py`.
6. Passa por revisão de código.
