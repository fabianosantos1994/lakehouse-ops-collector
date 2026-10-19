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
