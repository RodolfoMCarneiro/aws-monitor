# Plano de evolução: do relatório ao painel de sustentação

Este plano leva o agente de hoje (CLI, só leitura, relatório em Markdown) a um painel local com FastAPI, em que o analista vê a situação do dia, pede diagnóstico do LLM por grupo de falha, registra a avaliação e, numa fase posterior, reexecuta Step Functions com o mesmo input.

Referências visuais: `web/painel.html` (esboço v1) e `web/painel_v2.html` (esboço v2, que é o alvo deste plano).

## Princípios

1. **O que não precisa de LLM não usa LLM.** Triagem, dossiê, classificação, agrupamento e reaproveitamento de diagnóstico são determinísticos. O LLM entra só para diagnosticar um grupo de falhas.
2. **Uma chamada ao LLM por grupo, não por pipeline.** O grupo é formado por dataset + etapa + assinatura do erro. O LLM recebe um dossiê completo e um resumo curto dos demais membros.
3. **Leitura e escrita separadas.** A coleta continua só de leitura. A reexecução entra com uma role própria, desligada por padrão, e nunca é disparada pelo LLM.
4. **Dados ficam locais e fora do Git.** Banco SQLite e JSONs de coleta ficam em `saida/`, que não é versionada. O que vai para o Git é código e conhecimento revisado (por exemplo, padrões novos em `tools/padroes.py`).
5. **Só a última execução de cada tabela.** O painel mostra a situação atual: `job-pipelines-last-executions` diz o estado de cada pipeline e `job-metrics` detalha a execução. Tendência e histórico ficam no dashboard do Datadog, que recebe o mesmo conteúdo da Step Function de processamento. O painel não reconstrói séries históricas.
6. **Uma camada de acesso aos dados.** Todo acesso ao banco passa por um único módulo, para que a troca futura por um banco compartilhado fique restrita a ele.

## Visão das fases

| Fase | Entrega | Depende de | Escreve na AWS? |
|---|---|---|---|
| 0 | Higiene do repositório | – | Não |
| 1 | Coleta enriquecida e agrupamento | 0 | Não |
| 2 | Armazenamento local (SQLite) | 1 | Não |
| 3 | API e painel: visão geral | 2 | Não |
| 4 | Análise por grupo e avaliação | 3 | Não |
| 5 | Reexecução com o mesmo input | 4 + decisões D1–D3 | **Sim** |
| 6 | Retroalimentação e métricas | 4 | Não |
| 7 | Banco compartilhado (quando necessário) | critérios da fase 7 | Não |

As fases 0 a 4 entregam valor sem nenhuma permissão nova na AWS. A fase 5 só começa depois das decisões pendentes (ver seção "Decisões pendentes").

Pré-requisito: o PR #1 (troca de `litellm` pelo SDK `openai`) mergeado. A fase 4 usa `response.usage` do SDK para registrar tokens.

---

## Fase 0: Higiene do repositório

**Objetivo:** impedir que dados de coleta, banco e traces cheguem ao GitHub.

- Incluir no `.gitignore`: `saida/`, `*.db`, `*.db-wal`, `*.db-shm`.
- Conferir que nenhum `coleta_*.json`, `trace_*.json` ou `resumo_*.md` já foi commitado (`git log --all -- 'saida/*'`). Se houver, avaliar reescrita de histórico antes de o repositório ser compartilhado.
- Registrar no README que `saida/` contém trechos de log, ARNs e inputs de execução e não deve sair da máquina.

**Pronto quando:** `git status` não mostra nada de `saida/` depois de uma execução completa.

---

## Fase 1: Coleta enriquecida e agrupamento

**Objetivo:** a coleta passa a trazer o que o painel e a reexecução precisam, ainda sem banco e sem API.

### 1.1 Input da execução
- `tools/dossie.py::_step_function` já chama `DescribeExecution`. Passar a guardar o campo `input` (JSON) no dossiê, truncado por um limite configurável.
- O input **não** vai para o LLM: é usado só para exibição e reexecução. Remover o campo do payload em `agente.py`.

### 1.2 Assinatura do erro
- Novo módulo `tools/assinatura.py` com `assinatura(texto) -> str` e `hash_assinatura(texto) -> str`.
- Regra: pegar a primeira linha de exceção relevante (classe + mensagem) e normalizar removendo datas, horários, IDs de execução, cluster e step, ARNs, nomes de tabela do pipeline, valores de partição (`dt=...`) e números longos.
- Falha sem log (ex.: travada com stderr não sincronizado) recebe assinatura própria por pipeline, para nunca agrupar por ausência de evidência.

### 1.3 Agrupamento
- Novo módulo `tools/grupos.py` com `agrupar(dossies) -> list[grupo]`, chave `dataset | etapa | hash_assinatura`.
- Cada grupo: chave, dataset, etapa, categoria, assinatura, membros (table_name, ARN da execução, hora, cluster).
- `coletar()` passa a devolver também `grupos`.
- `relatorio.py` ganha uma seção "Grupos de falha" no relatório base, útil já no CLI.

### 1.4 Testes
- Em `tests/teste_offline.py`: normalização (duas mensagens que diferem só em data/ID geram a mesma assinatura; ORA-12170 e ORA-00942 geram assinaturas diferentes), agrupamento do caso `autorizacao` (5 + 1) e presença do input no dossiê.

**Pronto quando:** `python main.py --sem-llm` mostra os grupos no `resumo_*.md`, e o teste offline cobre os casos acima.

---

## Fase 2: Armazenamento local (SQLite)

**Objetivo:** persistir coletas, análises, avaliações e (no futuro) reexecuções, sem serviço externo.

### 2.1 Estrutura
- `armazenamento.py`: única porta de acesso ao banco (funções como `registrar_coleta`, `salvar_analise`, `salvar_avaliacao`, `diagnostico_validado`, `registrar_reexecucao`).
- `schema.sql` + tabela `versao_schema` para migrações simples, sem ORM.
- Banco em `saida/sustentacao.db`, modo WAL, `foreign_keys=ON`.

### 2.2 Tabelas

| Tabela | Campos principais | Observação |
|---|---|---|
| `coletas` | id, gerado_em_utc, regiao, caminho_json | Liga análises e avaliações à coleta em que foram feitas; não alimenta gráfico histórico |
| `grupos` | id, coleta_id, dataset, etapa, categoria, assinatura, hash_assinatura | Gerado na coleta |
| `grupo_membros` | grupo_id, table_name, execution_arn, situacao | Permite separar um pipeline do grupo |
| `analises` | id, grupo_id, origem (`llm` / `reaproveitada`), analise_origem_id, modelo, tokens_entrada, tokens_saida, diagnostico_json, caminho_trace, criado_em | Tokens reais, não estimados |
| `avaliacoes` | id, analise_id, analista, veredito, categoria_final, acao, comentario, criado_em | Só inserção: atualizar = nova linha |
| `reexecucoes` | id, table_name, execution_arn_original, execution_arn_nova, hash_input, parou_anterior, justificativa, solicitado_por, situacao, chave_idempotencia (UNIQUE), criado_em, atualizado_em | Criada já na fase 2, usada na fase 5 |

Blobs grandes (coleta e trace) continuam como arquivo em `saida/`; o banco guarda o caminho.

### 2.3 Identidade do analista
- Obter com `sts get-caller-identity` na mesma sessão SSO da coleta e gravar o ARN/usuário em `avaliacoes.analista` e `reexecucoes.solicitado_por`. Sem campo de nome digitado.

### 2.4 Backup
- Comando `python main.py --backup` usando `VACUUM INTO 'saida/backup_<data>.db'` (seguro com o app rodando). Guardar fora do repositório.
- Nunca copiar o `.db` com cópia de arquivo enquanto o app estiver aberto, e nunca versioná-lo (ver fase 0).

**Pronto quando:** uma execução `--sem-llm` registra coleta e grupos no banco, e o teste offline roda contra um banco temporário.

---

## Fase 3: API e painel, visão geral

**Objetivo:** servir a aba "Visão geral" do esboço v2 com dados reais, só leitura.

- `api.py` com FastAPI + Uvicorn, ouvindo só em `127.0.0.1`.
- Endpoints:
  - `GET /` serve `web/painel.html` (versão final derivada do v2).
  - `GET /coletas/ultima`: big numbers, gráficos por dataset e causa, grupos.
  - `GET /?pipeline=<table_name>`: abre a aba de investigação no grupo do pipeline. É o destino do link de contexto no dashboard do Datadog.
  - `POST /coletas`: roda o fluxo `--sem-llm` em background e devolve o id. Bloquear coletas simultâneas.
- O HTML troca os dados fixos (`DADOS`) por `fetch` nesses endpoints.
- Visão geral só com a última execução: big numbers por situação, barra de situação de todas as tabelas, problemas por etapa, falhas por dataset e por causa, e a tabela de grupos. Link "Histórico no Datadog" para tendências.
- No Datadog: configurar o link de contexto do dashboard apontando para `/?pipeline={{table_name}}`.
- Dependências novas: `fastapi`, `uvicorn`. Nenhuma permissão AWS nova.

**Pronto quando:** `uvicorn api:app` abre o painel com a coleta real mais recente, o botão "Atualizar coleta" funciona e o link a partir do Datadog abre o grupo certo.

---

## Fase 4: Análise por grupo e avaliação

**Objetivo:** aba "Investigação" funcionando para análise e avaliação (ainda sem reexecução).

### 4.1 Análise por grupo no agente
- Nova função `agente.analisar_grupo(grupo, dossies) -> (diagnostico, trace, usage)`.
- Payload: dossiê completo do primeiro membro + uma linha por membro restante (table_name, hora, cluster, linha de erro). Sem input de execução.
- Prompt de sistema próprio para grupo, pedindo saída em JSON (causa, confiança, evidências, membros que destoam, impacto, plano, lacunas), validada antes de salvar. Se a validação falhar, salvar o texto bruto e marcar a análise como "não estruturada".
- Registrar `usage.prompt_tokens` e `usage.completion_tokens` em `analises`.
- `analisar(coleta)` (relatório completo do CLI) continua existindo.

### 4.2 Reaproveitamento
- Antes de chamar o LLM, `armazenamento.diagnostico_validado(hash_assinatura, dias=N)` busca a última avaliação `correto` da mesma assinatura. Se houver, cria a análise com `origem = reaproveitada` e 0 tokens. O analista pode forçar nova análise.

### 4.3 Endpoints
- `POST /analises {grupo_id, table_names}`: cria um job por grupo. Fila em processo com no máximo 2 análises simultâneas.
- `GET /analises/{id}`: estado e resultado (polling simples; SSE é opcional).
- `POST /analises/{id}/separar {table_name}`: tira um pipeline do grupo e cria análise individual.
- `POST /analises/{id}/avaliacoes`: grava avaliação (nova linha a cada envio).

**Pronto quando:** um grupo real é analisado com uma chamada, os tokens aparecem no cartão, a avaliação persiste após reiniciar o servidor, e uma assinatura avaliada como correta é reaproveitada na coleta seguinte.

---

## Fase 5: Reexecução com o mesmo input

**Objetivo:** o analista reexecuta, a partir do painel, a Step Function que falhou, com o mesmo input. **Primeira escrita do projeto na AWS.**

### 5.1 Segurança e permissões
- Role ou profile SSO **separado** da coleta, usado só por `acoes.py`, com apenas:
  - `states:StartExecution` e `states:StopExecution` em `arn:aws:states:<regiao>:<conta>:stateMachine:sdlf-meutime-*` e nas execuções correspondentes;
  - `states:DescribeExecution` para acompanhar.
- Variável `REEXECUCAO_HABILITADA=false` por padrão. Com `false`, o botão aparece desabilitado e a API recusa.
- Lista de operadores (identidades SSO) em configuração; os demais só veem.
- O LLM não tem acesso a `acoes.py` em nenhuma hipótese (não entra em `tools/`).

### 5.2 Fluxo
1. A tela de confirmação mostra o input original, a state machine e o nome da nova execução (`<id_original>-r<n>`). Exige justificativa e confirmação.
2. `POST /reexecucoes {execution_arns, justificativa, parar_travadas, chave_idempotencia}`.
3. Para cada ARN: `DescribeExecution` (lê o input atual da AWS, não o do banco), confere o hash com o exibido na tela; se for travada e `parar_travadas`, `StopExecution`; então `StartExecution` com o mesmo input.
4. Grava em `reexecucoes` antes de chamar a AWS (situação `solicitada`) e atualiza depois. A `chave_idempotencia` UNIQUE impede duplo envio.
5. `GET /reexecucoes?dia=` acompanha via `DescribeExecution` até o término.

### 5.3 Limitações a tratar na tela
- Execuções com mais de 90 dias não podem ser consultadas: sem input, não há reexecução.
- Pipelines "não executados" não têm execução para repetir.

**Pronto quando:** em ambiente de homologação, uma reexecução real termina com sucesso, aparece na auditoria com identidade e justificativa, e um duplo clique não gera duas execuções.

---

## Fase 6: Retroalimentação e métricas

**Objetivo:** o sistema fica mais barato e mais preciso com o uso.

- Relatório periódico (consulta no banco) de assinaturas avaliadas como `correto` 3 ou mais vezes: candidatas a padrão novo em `tools/padroes.py`, via PR com revisão.
- Métricas sobre a própria ferramenta (não sobre os pipelines, cujo histórico fica no Datadog): taxa de acerto do LLM por categoria (vereditos), tokens consumidos, percentual de grupos reaproveitados, reexecuções por resultado (sucesso / falhou de novo). Podem ser uma consulta ou relatório, sem gráfico no painel.
- Reexecução que falhou de novo com a mesma assinatura sinaliza "causa não resolvida" no grupo.

**Pronto quando:** pelo menos um padrão novo entrou em `padroes.py` a partir dos dados de avaliação.

---

## Fase 7: Banco compartilhado (quando necessário)

**Gatilhos para migrar:** mais de uma máquina precisa ver as mesmas avaliações e reexecuções, ou o painel passa a rodar como serviço.

- Opções: DynamoDB (já usado no ecossistema) ou Postgres. O esquema da fase 2 se mantém.
- A troca fica restrita a `armazenamento.py`.
- Até lá, não versionar o banco nem tentar compartilhá-lo via Git: arquivo binário não se mescla e as avaliações de um lado se perdem.

---

## Decisões pendentes

| # | Pergunta | Afeta |
|---|---|---|
| D1 | Os pipelines sobrescrevem a partição do dia ao reprocessar, ou há risco de duplicar dados? | Fase 5: pode exigir passo de limpeza antes da reexecução |
| D2 | Reexecutar a etapa `input` dispara sozinho `transformation` e criptografia, ou cada etapa precisa ser reexecutada? | Fase 5: "reexecutar a etapa" ou "reexecutar a partir da etapa" |
| D3 | Quem pode reexecutar (lista de operadores) e em qual ambiente validar primeiro? | Fase 5 |
| D4 | Por quantos dias um diagnóstico validado pode ser reaproveitado? | Fase 4 |
| D5 | Algum dataset tem pipelines de origens diferentes? Se sim, precisa de mapeamento dataset → origem para o agrupamento | Fase 1 |

## Riscos

| Risco | Mitigação |
|---|---|
| Normalização ampla junta erros diferentes num grupo | Assinatura usa classe + mensagem, não só categoria; "Analisar à parte" na tela; LLM aponta membros que destoam |
| Normalização estreita não agrupa nada | Custo volta ao da análise individual, sem perda de qualidade; ajustar regras com casos reais nos testes |
| Reexecução duplica dados | Decisão D1 antes da fase 5; aviso na tela; flag desligada por padrão |
| Reexecução em cascata indesejada | Decisão D2; mostrar na confirmação o que será disparado |
| Dados sensíveis vazam pelo Git | Fase 0; banco e coletas em `saida/`, ignorada |
| Prompt injection via log | Já tratado no prompt; o LLM não tem acesso a ações de escrita |
