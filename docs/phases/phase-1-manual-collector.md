# Fase 1: coletor manual (rascunho)

- Status: em discussão; nada implementado
- Data: 2026-10-18 (revisada em 2026-10-19, com a separação do repositório)

Primeira versão do robô que monta o pacote de um incidente, rodada à mão no
terminal. A pergunta da fase é: **o que já dá para coletar hoje basta para
diagnosticar os 3 incidentes de referência?** A resposta mostra como os logs e
os dados precisam ser emitidos e organizados.

A fase segue o roteiro da proposta de troubleshooting com AI
(`docs/proposals/ai-troubleshooting-hands-on.md` do `lakehouse-ai-ops`). Os
incidentes de referência e os pontos de melhoria (IDs `MP-NN`) também estão
lá, em `evaluation/reference-incidents/` e `docs/improvements.md`.

Fora do escopo: como o coletor é disparado (poller), o PostgreSQL próprio, o
modelo, a linhagem do Marquez e o histórico (pacotes v2 e v3 da proposta).

## Onde fica

Neste repositório, que reúne tudo o que lê o ambiente: o coletor do incidente
(esta fase), o poller e os coletores de histórico (fases seguintes). O
`lakehouse-ai-ops` fica só com o diagnóstico e a avaliação e nunca tem
credenciais do ambiente. A ligação entre os dois é o pacote do incidente
([seção Contrato](#contrato-com-o-lakehouse-ai-ops)).

## O que coletar

Itens que a fase 0 mostrou necessários, com a fonte e o acesso conferidos na
stack local em 2026-10-18.

| Item | Fonte | Acesso | `ri-001` | `ri-002` | `ri-003` | Ponto |
| --- | --- | --- | --- | --- | --- | --- |
| run, task e tentativas (estado, horários, operator) | API do Airflow: `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}` e `.../taskInstances/{task_id}/tries` | token em `POST /auth/token` | ✓ | ✓ | ✓ | |
| estado das demais tasks da run (`upstream_failed`) | API do Airflow: `.../dagRuns/{run_id}/taskInstances` | idem | | | ✓ | MP-22 |
| eventos `lh_*` de cada tentativa | API do Airflow: `.../taskInstances/{task_id}/logs/{try_number}` com `Accept: application/json` | devolve o log como lista de objetos, já com os campos `lh_*` | ✓ | ✓ | ✓ | |
| exceção final (`Cause`, `Where`, `Next`) | mesmo log, evento `Task failed with exception` | idem | ✓ | ✓ | ✓ | |
| traceback do driver Spark | mesmo log, entre `::group::Spark driver output` e `::endgroup::` | idem; recortado a partir de `Traceback` | | | ✓ | MP-11 |
| checks reprovados com `expected_value` e `diagnostic` | `dq_execution` e `dq_check_result` no `platform_metadata`, pelo `dq_execution_id` do `lh_evidence` | PostgreSQL publicado em `127.0.0.1:5433` | ✓ | ✓ | | MP-10 |
| checks da mesma run na mesma coluna | `dq_check_result` pela `run_id` e `column_name` | idem | | ✓ | | MP-13 |
| definição da task | `pipeline.yaml` do domínio | arquivos locais em `~/projects/lakehouse-domains` | ✓ | ✓ | ✓ | |
| trecho do arquivo citado, com margem em volta da linha | arquivo do `pipeline_file` do `lh_evidence` (relativo à pasta do pipeline) | idem | ✓ | ✓ | ✓ | MP-12 |
| estado do repositório do domínio (commit, branch, alterações locais) | `git` no repositório do domínio | idem | ✓ | ✓ | ✓ | MP-14 |
| dono do domínio | `owner` do `domain_manifest.yaml` | idem | ✓ | ✓ | ✓ | |

Duas lacunas que o coletor contorna na primeira versão:

- **Qual `pipeline.yaml` gerou a DAG** (MP-25): nem a DAG nem os eventos
  dizem. O coletor procura nos `pipeline.yaml` do domínio o `dag.dag_id`
  igual ao do incidente.
- **A linha no arquivo** (MP-01): só o traceback do Spark informa (e com
  desvio, MP-12). Para YAML de checks, o coletor procura a linha do
  `id: <check_id>` e, se o check citar uma coluna, a linha dela dentro do
  check. Essa busca é provisória e mostra o que o evento deveria trazer.

## Arquitetura

Um pacote Python, com um subpacote por responsabilidade. Nesta fase, só
`collection` existe; os outros entram nas fases seguintes.

```text
lakehouse-ops-collector/
  src/lakehouse_ops_collector/
    package.py           # IncidentKey e IncidentPackage: o formato publicado (modelos Pydantic)
    collection/          # fase 1: coletor do incidente
      __main__.py        # CLI: collect e inspect
      collect.py         # caso de uso collect_incident(key) -> IncidentPackage
      ports.py           # AirflowSource, QualitySource, DomainSource, PackageStore
      limits.py          # orçamento de tamanho e o que nunca entra
      adapters/
        airflow_api.py       # API REST v2 do Airflow
        platform_metadata.py # dq_execution e dq_check_result
        domain_files.py      # pipeline.yaml, arquivo citado, git, manifest
        json_store.py        # incidents/<chave>/package.json
    detection/           # depois: poller
    history/             # depois: coletores de histórico
  tests/                 # adapters falsos e respostas gravadas da fase 0
```

Os subpacotes importam só `package.py`, nunca uns aos outros.

Os "robôs" são os três adapters de fonte: cada um sabe ler um sistema e
devolver só o que interessa. O caso de uso `collect_incident` chama os três,
aplica os limites e grava o pacote. Uma fonte com erro (ex.: banco fora do ar)
não impede o pacote: ela aparece em `sources` com o erro, o que também é um
sinal para o diagnóstico.

Configuração por variáveis de ambiente, em um `.env` não versionado (com
`.env.example` versionado): URL e usuário da API do Airflow, conexão do
`platform_metadata` e caminho dos domínios. Na fase 1, as credenciais são o
admin local do Airflow e o usuário `platform` do banco; os dois podem
escrever, e a troca por usuários somente leitura é o ponto MP-26.
Dependências: Python 3.12, Pydantic, `psycopg` para o PostgreSQL e o cliente
HTTP da biblioteca padrão, com versões fixadas.

## Contrato com o `lakehouse-ai-ops`

O pacote do incidente é a interface entre os dois repositórios:

- este repositório é o dono do formato: `package_version` no próprio pacote e,
  quando o formato estabilizar, um JSON Schema gerado dos modelos Pydantic
  (`model_json_schema()`) e publicado aqui;
- o `lakehouse-ai-ops` só lê os arquivos JSON (depois, a base de incidentes)
  e nunca importa código daqui;
- mudar ou remover um campo exige subir `package_version`; acrescentar campo
  não exige.

## Uso planejado

```bash
# monta o pacote de um incidente
.venv/bin/python -m lakehouse_ops_collector.collection collect \
  --dag-id commercial_daily_sales_by_region \
  --run-id 'backfill__2026-10-15T00:00:00+00:00' \
  --task-id validate_daily_sales_by_region

# mostra o que uma fonte devolve, sem gravar pacote (para aprender e depurar)
.venv/bin/python -m lakehouse_ops_collector.collection inspect airflow --dag-id ... --run-id ... --task-id ...
```

O pacote fica em `incidents/<dag_id>/<run_id>/<task_id>/package.json`, fora do
Git. O `collect` vem primeiro; o `inspect` entra depois que o `collect`
funcionar, reaproveitando os mesmos adapters. Ele mostra o que cada fonte
devolve antes dos filtros e limites, e responde se um dado faltou na fonte ou
foi descartado pelo coletor.

## Pacote v1 (rascunho)

```json
{
  "package_version": 1,
  "collected_at": "2026-10-18T23:00:00Z",
  "incident": {"dag_id": "…", "run_id": "…", "task_id": "…", "map_index": -1},
  "sources": {"airflow": "ok", "platform_metadata": "ok", "domain": "ok"},
  "run": {"state": "failed", "logical_date": "…", "other_tasks": [{"task_id": "…", "state": "upstream_failed"}]},
  "attempts": [
    {
      "try_number": 1, "state": "failed", "start_date": "…", "end_date": "…",
      "lh_events": [{"lh_event": "operator.failed", "lh_error_kind": "…", "lh_reason": "…", "lh_evidence": []}],
      "exception": "<Cause, Where, Next>",
      "engine_output": "<traceback recortado, só Spark>"
    }
  ],
  "quality": {
    "failed_checks": [{"check_id": "…", "check_type": "…", "column": "…", "expected_value": {}, "observed_value": {}, "diagnostic": {}}],
    "related_checks": [{"task_id": "…", "check_id": "…", "status": "passed", "expected_value": {}}]
  },
  "domain": {
    "repository": {"commit": "…", "branch": "…", "dirty": true},
    "owner": "analytics",
    "pipeline_file": "commercial/daily_sales_by_region/pipeline.yaml",
    "task_definition": {},
    "cited_files": [{"path": "…", "line": 10, "line_source": "search", "excerpt": "<linhas 1 a 20>"}]
  }
}
```

Limites iniciais (a calibrar com os 3 casos): `engine_output` com até 40
linhas, até 20 checks reprovados, `diagnostic` com até 10 valores
inesperados, trecho de arquivo com 10 linhas antes e depois. Nunca entram
linhas de dados, credenciais nem variáveis de ambiente. Os valores
inesperados de colunas categóricas (como `Sul`) entram por serem valores de
domínio, não linhas.

## Como avaliar

Os 3 incidentes já aconteceram, mas o domínio volta para a `main` ao fim de
cada caso, e coletar depois leria os arquivos sem a falha (MP-14). Para cada
caso:

1. reaplicar a falha na branch do caso (etapa 5 da fase 0 do
   `lakehouse-ai-ops`, `docs/phases/phase-0-reference-incidents.md`);
2. rodar `collect` para a run do caso;
3. desfazer a falha (etapa 10 da fase 0).

Depois, comparar o `package.json` com o `notes.md` do caso no
`lakehouse-ai-ops`:

- todo passo da investigação marcado "Um coletor leria sozinho? sim" está no
  pacote;
- o pacote cabe no orçamento (meta de cerca de 4 mil tokens, estimados por
  caracteres nesta fase; a contagem real vem na fase do modelo);
- nada do que nunca entra está no pacote.

O que faltar ou sobrar vira ponto novo, ou muda o status de um ponto, no
`docs/improvements.md` do `lakehouse-ai-ops`.

## Decisões

Tomadas em 2026-10-19:

| Assunto | Decisão | Motivo |
| --- | --- | --- |
| formato do pacote | modelos Pydantic | valida na criação, serializa em JSON e gera o JSON Schema do contrato; o `lakehouse-ai-ops` também usa Pydantic no `Diagnosis` |
| credenciais na fase 1 | admin local do Airflow e usuário `platform` do banco | leitura manual, no terminal do dono do ambiente; usuários somente leitura antes do poller (MP-26) |
| CLI | `collect` e `inspect` | o `inspect` custa pouco, porque reaproveita os adapters, e separa "a fonte não tem" de "o coletor cortou" |

## Plano de execução

O coletor é construído em 10 etapas. Cada etapa acrescenta uma parte do
pacote e termina com um teste manual nos 3 incidentes de referência, rodado
no terminal. Cada etapa também ganha testes automáticos (`pytest`) com
adapters falsos e respostas gravadas, mas o critério de pronto é o teste
manual.

Ciclo de cada etapa:

1. o Claude implementa a etapa e roda os testes automáticos; o status passa a
   **implementada, aguardando teste manual**;
2. você roda os comandos de "Testar" da etapa e compara com o "Esperado";
3. se passar, o status passa a **concluída**, com a data e o resultado; se
   não passar, a saída volta para correção.

Uma etapa só começa quando a anterior está concluída. Os comandos de uma etapa
**não implementada** ainda não funcionam: o `collect` roda, mas as seções novas
não aparecem no pacote (`show` imprime `null`).

### Andamento

| Etapa | O que acrescenta ao pacote | Status |
| --- | --- | --- |
| 1 | esqueleto e chave do incidente | implementada, aguardando teste manual |
| 2 | Airflow: run e tentativas | não implementada |
| 3 | Airflow: eventos `lh_*` e exceção | não implementada |
| 4 | Airflow: saída do driver Spark | não implementada |
| 5 | Data Quality: checks reprovados | não implementada |
| 6 | Data Quality: checks relacionados | não implementada |
| 7 | domínio: pipeline, task e repositório | não implementada |
| 8 | domínio: arquivo citado, linha e trecho | não implementada |
| 9 | limites e regras do pacote | não implementada |
| 10 | `inspect` e avaliação final | não implementada |

### Instalar (uma vez)

```bash
cd ~/projects/lakehouse-ops-collector
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

### Preparar o terminal (antes de cada sessão)

```bash
cd ~/projects/lakehouse-ops-collector
DAG=commercial_daily_sales_by_region
RUN_001='backfill__2026-10-15T00:00:00+00:00'; TASK_001=validate_daily_sales_by_region
RUN_002='backfill__2026-10-16T00:00:00+00:00'; TASK_002=validate_daily_sales_by_region
RUN_003='backfill__2026-10-17T00:00:00+00:00'; TASK_003=build_daily_sales_by_region

collect() { .venv/bin/python -m lakehouse_ops_collector.collection collect --dag-id "$DAG" --run-id "$1" --task-id "$2"; }
pkg()     { echo "incidents/$DAG/$1/$2/package.json"; }
show()    { python3 -c 'import json, sys; print(json.dumps(json.load(open(sys.argv[1])).get(sys.argv[2]), indent=2, ensure_ascii=False))' "$(pkg "$1" "$2")" "$3"; }
```

- `collect "$RUN_001" "$TASK_001"` monta o pacote do `ri-001`;
- `show "$RUN_001" "$TASK_001" attempts` mostra uma seção do pacote.

As stacks precisam estar no ar e as runs dos 3 casos, no histórico do
Airflow (estão desde 2026-10-18).

### Etapa 1. Esqueleto e chave do incidente

Status: implementada em 2026-10-19, aguardando teste manual. Na
implementação, os comandos abaixo foram rodados com sucesso: os pacotes dos 3
casos foram gravados no caminho de `pkg`, e a chave vazia terminou com
`--run-id: String should have at least 1 character` e código 2, sem gravar
arquivo.

Construir: `pyproject.toml`, `src/lakehouse_ops_collector/`, `.env.example`,
o modelo Pydantic `IncidentKey` e um `collect` que só valida a chave e grava o
pacote com `package_version`, `collected_at` e `incident`.

Testar:

```bash
collect "$RUN_001" "$TASK_001" && show "$RUN_001" "$TASK_001" incident
collect "" "$TASK_001"   # deve falhar com erro de validação claro, sem gravar arquivo
```

Pronto quando: os 3 casos geram `package.json` no caminho de `pkg`, e a chave
vazia é rejeitada pelo Pydantic.

### Etapa 2. Airflow: run e tentativas

Status: não implementada.

Construir: adapter `airflow_api.py` com token em `POST /auth/token`; seções
`run` (estado, `logical_date`, demais tasks) e `attempts` (tentativa, estado,
horários); `sources.airflow`.

Testar:

```bash
for c in 001 002 003; do r=RUN_$c; t=TASK_$c; collect "${!r}" "${!t}"; show "${!r}" "${!t}" attempts; done
show "$RUN_003" "$TASK_003" run
AIRFLOW_API_URL=http://127.0.0.1:1 collect "$RUN_001" "$TASK_001"; show "$RUN_001" "$TASK_001" sources
```

Esperado:

| Caso | `attempts` | `run` |
| --- | --- | --- |
| `ri-001` | tentativas 1 e 2, `failed` | sem tasks `upstream_failed` |
| `ri-002` | tentativas 1 e 2, `failed` | sem tasks `upstream_failed` |
| `ri-003` | tentativas 1 e 2, `failed` | `validate_daily_sales_by_region` em `upstream_failed` |

Com a URL errada, o pacote é gravado mesmo assim, com o erro em
`sources.airflow`.

### Etapa 3. Airflow: eventos `lh_*` e exceção

Status: não implementada.

Construir: leitura do log de cada tentativa pela API (`Accept:
application/json`); `attempts[].lh_events` só com os campos `lh_*` e
`attempts[].exception` (`Cause`, `Where`, `Next`).

Testar: `show` de `attempts` nos 3 casos.

| Caso | `lh_error_kind` | `lh_evidence` da tentativa 2 |
| --- | --- | --- |
| `ri-001` | `data_quality` | `dq_execution:425fbd6e-…` e `pipeline_file:quality/daily_sales_by_region.yaml` |
| `ri-002` | `data_quality` | `dq_execution:6e612a71-…` e `pipeline_file:quality/daily_sales_by_region.yaml` |
| `ri-003` | `engine` | `spark_application:app-20261018222628-0020` e `pipeline_file:jobs/build_daily_sales_by_region.py` |

Pronto quando: cada tentativa tem `operator.started`, `operator.failed` e
`operator.finished`, e nenhum campo fora de `lh_*` entrou nos eventos.

### Etapa 4. Airflow: saída do driver Spark

Status: não implementada.

Construir: recorte do bloco `Spark driver output` a partir de `Traceback`, em
`attempts[].engine_output`, com limite de linhas (MP-11).

Testar: `show "$RUN_003" "$TASK_003" attempts`.

Esperado: no `ri-003`, o `engine_output` tem `build_daily_sales_by_region.py",
line 40, in aggregate` e a lista completa de sugestões, terminando em
`customer_id` (o que o `lh_reason` corta, MP-04). Nos `ri-001` e `ri-002`,
`engine_output` não existe.

### Etapa 5. Data Quality: checks reprovados

Status: não implementada.

Construir: adapter `platform_metadata.py`; `quality.failed_checks` a partir
dos `dq_execution` do `lh_evidence`, com `expected_value`, `observed_value` e
`diagnostic` (MP-10); `sources.platform_metadata`.

Testar: `show` de `quality` nos 3 casos.

| Caso | Esperado em `failed_checks` |
| --- | --- |
| `ri-001` | `expected_schema` nas 2 tentativas; o `expected_value` mostra `"2)": null` e `"type": "decimal(18"` em `total_amount` |
| `ri-002` | `region_standardized` nas 2 tentativas; o `diagnostic` mostra `partial_unexpected_list: ["Sul"]` |
| `ri-003` | vazio: o caso não tem `dq_execution` |

### Etapa 6. Data Quality: checks relacionados

Status: não implementada.

Construir: `quality.related_checks`, com os checks da mesma run sobre a mesma
coluna dos checks reprovados (MP-13).

Testar: `show "$RUN_002" "$TASK_002" quality`.

Esperado: no `ri-002`, `related_checks` tem o `region_standardized` de
`sources.validate_silver_customers`, `passed`, com `Sul` na lista. No
`ri-001`, vazio: os outros checks de `total_amount` não rodaram, porque o
`schema` reprovou antes.

### Etapa 7. Domínio: pipeline, task e repositório

Status: não implementada.

Construir: adapter `domain_files.py`; busca do `pipeline.yaml` pelo
`dag.dag_id` (MP-25); `domain.pipeline_file`, `task_definition`, `owner` e
`repository` (commit, branch, `dirty`) (MP-14); `sources.domain`.

Testar: `show` de `domain` nos 3 casos.

Esperado: `pipeline_file` `commercial/daily_sales_by_region/pipeline.yaml`,
`owner` `analytics`, a definição da task do caso e `repository` com
`branch: main` e `dirty: false`.

### Etapa 8. Domínio: arquivo citado, linha e trecho

Status: não implementada.

Construir: `domain.cited_files` a partir do `pipeline_file` do
`lh_evidence`, com a linha (`line_source`: `traceback` ou `search`) e um
trecho com margem (MP-12).

Testar com a falha reaplicada, porque o coletor lê o arquivo como está agora
(MP-14). Para cada caso, crie a branch e rode o `sed` do caso (etapa 5 da
fase 0 do `lakehouse-ai-ops`), colete e desfaça:

```bash
DOMAIN=~/projects/lakehouse-domains/lakehouse-pipelines-domain-b3
git -C "$DOMAIN" switch -c phase-1/ri-001
# sed do ri-001 (etapa 5 da fase 0)
collect "$RUN_001" "$TASK_001"; show "$RUN_001" "$TASK_001" domain
git -C "$DOMAIN" restore . && git -C "$DOMAIN" switch main && git -C "$DOMAIN" branch -D phase-1/ri-001
```

| Caso | `line` e `line_source` | O trecho deve mostrar |
| --- | --- | --- |
| `ri-001` | 10, `search` (coluna `total_amount` dentro do check `expected_schema`) | `total_amount: {type: decimal(18, 2), …}` sem aspas |
| `ri-002` | 13, `search` (linha do `id: region_standardized`) | a lista sem `Sul` na linha 16 |
| `ri-003` | 40, `traceback` | `F.sum("sale_amount")` na linha 42, dentro da margem |

Nos três, `repository.dirty` é `true` e `branch` é `phase-1/<caso>`. Rodar
também uma vez sem reaplicar a falha: o trecho mostra o arquivo corrigido e
`dirty: false`, que é exatamente o problema do MP-14.

### Etapa 9. Limites e regras do pacote

Status: não implementada.

Construir: `limits.py` com os limites da seção
[Pacote v1](#pacote-v1-rascunho) e a remoção do que nunca entra; o `collect`
passa a imprimir o tamanho do pacote em caracteres e uma estimativa de
tokens (caracteres / 4).

Testar:

```bash
for c in 001 002 003; do r=RUN_$c; t=TASK_$c; collect "${!r}" "${!t}"; done
grep -l -i -E "password|token|secret" incidents/"$DAG"/*/*/package.json   # não deve listar nada
```

Pronto quando: os 3 pacotes ficam abaixo de cerca de 16 mil caracteres (4 mil
tokens) e o `grep` não encontra nada.

### Etapa 10. `inspect` e avaliação final

Status: não implementada.

Construir: `inspect airflow|quality|domain`, que chama um adapter e imprime o
que a fonte devolveu, antes de filtros e limites.

Testar:

```bash
.venv/bin/python -m lakehouse_ops_collector.collection inspect quality --dag-id "$DAG" --run-id "$RUN_001" --task-id "$TASK_001"
```

Esperado: as linhas completas da `dq_check_result`, inclusive campos que o
pacote corta.

Avaliação final: para cada caso, compare o `package.json` com o `notes.md` no
`lakehouse-ai-ops`. Todo passo marcado "Um coletor leria sozinho? sim" precisa
estar no pacote. O que faltar ou sobrar vira ponto novo, ou muda o status de
um ponto, no `docs/improvements.md` do `lakehouse-ai-ops`.
