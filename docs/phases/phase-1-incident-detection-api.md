# Fase 1: API de detecção de incidentes

- Status: concluída em 2026-10-19
- Data: 2026-10-19

Primeira peça do repositório: uma API HTTP que encontra no Airflow as tasks
com falha e devolve, para cada uma, a **chave do incidente**, que é
exatamente o que o coletor vai receber. Somente leitura: a API só consulta o
Airflow.

O coletor do incidente fica para depois. O que já foi construído dele está
guardado na branch `standby/incident-collector`, fora da `main`.

## O que a API devolve

Cada incidente detectado traz a chave, o status e todas as tentativas da task.
Exemplo do `ri-001`:

```json
{
  "key": {
    "dag_id": "commercial_daily_sales_by_region",
    "run_id": "backfill__2026-10-15T00:00:00+00:00",
    "task_id": "validate_daily_sales_by_region",
    "map_index": -1
  },
  "status": "failed",
  "attempts": [
    {"number": 1, "cycle": 1, "state": "failed", "operator": "LakehouseDataQualityOperator",
     "start_date": "2026-10-18T20:53:54Z", "end_date": "2026-10-18T20:53:58Z"},
    {"number": 2, "cycle": 1, "state": "failed", "operator": "LakehouseDataQualityOperator",
     "start_date": "2026-10-18T20:58:58Z", "end_date": "2026-10-18T20:59:07Z"}
  ]
}
```

| Campo | Significado |
| --- | --- |
| `key` | a chave do incidente: o que o coletor recebe (`dag_id`, `run_id`, `task_id`, `map_index`) |
| `status` | `failed` (falha definitiva) ou `retrying` (falhou, mas ainda vai tentar de novo) |
| `attempts` | uma entrada por tentativa, em ordem (campos abaixo) |

Campos de cada tentativa:

| Campo | Significado |
| --- | --- |
| `number` | número da tentativa: 1 é a primeira; continua subindo depois de um `clear` |
| `cycle` | ciclo da tentativa: começa em 1 e sobe a cada `clear` |
| `state` | estado da tentativa no Airflow (`failed`, `up_for_retry`...) |
| `operator` | classe do operator que rodou esta tentativa |
| `start_date`, `end_date` | horários de início e fim |

**Ciclo.** Um ciclo é a primeira tentativa mais as repetições automáticas
(`retries`). O `clear` reinicia a cota de repetições e abre um ciclo novo.
Com `retries: 1`, duas falhas seguidas de um `clear` e mais duas falhas ficam
assim:

```text
number 1, cycle 1   failed
number 2, cycle 1   failed    ← fim do ciclo 1: falha definitiva
          clear
number 3, cycle 2   failed
number 4, cycle 2   failed    ← fim do ciclo 2
```

A API identifica o `clear` pelo `max_tries` que o Airflow guarda em cada
tentativa: numa repetição normal ele não muda, e o `clear` sempre o aumenta.

**Operator por tentativa.** Normalmente é o mesmo em todas. Ele pode mudar
quando um `clear` roda a tentativa nova na versão mais recente da DAG, depois
de alguém alterar o `pipeline.yaml`.

Só abrem incidente as tasks em `failed` e, se pedido, em `up_for_retry`. As
tasks em `upstream_failed` não abrem: elas não rodaram, e o incidente é o da
task que falhou antes delas (seção 5 da proposta de troubleshooting do
`lakehouse-ai-ops`).

## Endpoints

| Método e caminho | Para quê | Respostas |
| --- | --- | --- |
| `GET /health` | estado da API e do Airflow (scheduler, dag-processor) | sempre `200`; o Airflow fora do ar aparece em `airflow.status: error` |
| `GET /incidents` | lista os incidentes, do mais recente para o mais antigo | `200`; `502` se o Airflow não responder; `422` se um parâmetro for inválido |
| `GET /incidents/{dag_id}/{run_id}/{task_id}` | consulta um incidente pela chave | `200`; `404` se a task não existe ou não está com falha; `502` se o Airflow não responder |
| `GET /docs` | documentação interativa, para chamar a API pelo navegador | `200` |

Parâmetros de `GET /incidents`:

| Parâmetro | Padrão | Efeito |
| --- | --- | --- |
| `since` | nenhum | só incidentes cuja **última tentativa** terminou a partir deste horário (ver abaixo); ISO 8601 **com fuso** (ex.: `2026-10-18T21:00:00Z`) |
| `dag_id` | todas as DAGs | só desta DAG |
| `include_retrying` | `false` | inclui as tasks que ainda vão tentar de novo |
| `limit` | `100` | máximo de incidentes (1 a 1000) |

**Onde o `since` é aplicado.** Ele compara com o `end_date` da última
tentativa da task, a que deixou a task no estado atual. Não usa o
`start_date` nem as tentativas anteriores. No `ri-001`:

```text
number 1   end_date 20:53:58   ← ignorada pelo filtro
number 2   end_date 20:59:07   ← é esta que o since compara
```

Com `since=20:55`, o `ri-001` aparece (20:59:07 ≥ 20:55), embora a primeira
tentativa tenha terminado antes. Com `since=21:00`, não aparece. Num incidente
`failed`, esse horário é o momento em que a falha ficou definitiva. Quando a
task ganha uma tentativa nova (um `clear`, por exemplo), o horário passa a ser
o dela.

Em `GET /incidents/{dag_id}/{run_id}/{task_id}`, o `run_id` vai codificado na
URL: `:` vira `%3A` e `+` vira `%2B`. O parâmetro opcional `map_index` serve
para tasks mapeadas.

## Arquitetura

```text
src/lakehouse_ops_collector/
  incident.py            # IncidentKey: a chave que o coletor recebe (Pydantic)
  detection/
    __main__.py          # sobe a API (uvicorn)
    api.py               # endpoints (FastAPI)
    service.py           # casos de uso find_incidents e get_incident
    ports.py             # TaskInstanceSource: interface da fonte
    models.py            # modelos da resposta e da fonte
    settings.py          # configuração por .env e variáveis de ambiente
    adapters/
      airflow_api.py     # API REST v2 do Airflow: token, paginação, filtros
```

A API conversa com o Airflow só pelo adapter. Os testes trocam o adapter por
um Airflow simulado (`httpx.MockTransport`) e não acessam o ambiente.

## Decisões

| Assunto | Decisão | Motivo |
| --- | --- | --- |
| framework | FastAPI e uvicorn | usa os mesmos modelos Pydantic da chave e gera a página `/docs` |
| consulta ao Airflow | `GET /api/v2/dags/~/dagRuns/~/taskInstances` com `state`, `end_date_gte` e `order_by=-end_date`, paginado | uma chamada cobre todas as DAGs; `~` é o curinga do Airflow |
| credenciais | admin local do Airflow, por `.env` | decisão da fase anterior; trocar por um usuário `Viewer` (MP-26 do `lakehouse-ai-ops`) |
| exposição | `127.0.0.1:8090`, sem autenticação na API | só a própria máquina chega à API, como os serviços do `lakehouse-infra` |
| tentativas | lista `attempts`, com `number` e `cycle` em cada uma, no lugar de `try_number` e `max_tries` do Airflow | identifica cada tentativa e cada `clear` sem comparar horários; no Airflow, `max_tries` conta só as repetições ("tentativa 2 de 1") e muda a cada `clear` |
| operator | em cada tentativa, não no incidente | a tentativa depois de um `clear` pode rodar com outra definição da task |
| filtro `since` | `end_date_gte` sobre a última tentativa | é o único horário que o Airflow filtra na listagem; marca quando a falha ficou definitiva |
| histórico das tentativas | uma chamada a `.../taskInstances/{task_id}/tries` por incidente | o Airflow não devolve as tentativas anteriores na listagem; com dezenas de incidentes por dia, o custo é pequeno |
| token do Airflow | pedido em `POST /auth/token`, guardado em memória e renovado uma vez quando o Airflow responde `401` | o token vale 24 h |

## Executar

### Instalar (uma vez)

```bash
cd ~/projects/lakehouse-ops-collector
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env
```

No `.env`, ajuste `AIRFLOW_USERNAME` e `AIRFLOW_PASSWORD` para os valores de
`AIRFLOW_ADMIN_USERNAME` e `AIRFLOW_ADMIN_PASSWORD` do `docker/.env` do
`lakehouse-infra`.

### Subir a API (terminal 1)

```bash
cd ~/projects/lakehouse-ops-collector
.venv/bin/python -m lakehouse_ops_collector.detection
```

Fica rodando e mostra cada requisição. `Ctrl+C` encerra.

### Teste manual (terminal 2)

Rode cada comando e compare com o esperado. Pelo navegador, o mesmo teste pode
ser feito em `http://127.0.0.1:8090/docs`.

**1. Saúde**

```bash
curl -s http://127.0.0.1:8090/health | python3 -m json.tool
```

Esperado: `"status": "ok"` e, em `airflow`, `"status": "ok"` com o scheduler
`healthy`.

**2. Lista de incidentes**

```bash
curl -s http://127.0.0.1:8090/incidents | python3 -m json.tool
```

Esperado: `count: 4`, nesta ordem:

| `run_id` | `task_id` | Caso |
| --- | --- | --- |
| `backfill__2026-10-17T00:00:00+00:00` | `build_daily_sales_by_region` | `ri-003` |
| `backfill__2026-10-16T00:00:00+00:00` | `validate_daily_sales_by_region` | `ri-002` |
| `backfill__2026-10-15T00:00:00+00:00` | `validate_daily_sales_by_region` | `ri-001` |
| `scheduled__2026-10-18T00:00:00+00:00` | `validate_daily_sales_by_region` | run a mais, criada ao despausar a DAG (MP-23) |

Todos com `status: failed` e duas entradas em `attempts`: `number` 1 e 2,
as duas com `cycle: 1` (nenhum `clear`) e `state: failed`, a segunda cerca de
5 minutos depois da primeira. Os horários aparecem com frações de segundo.

**3. Um incidente por caso**

```bash
B=http://127.0.0.1:8090/incidents/commercial_daily_sales_by_region
curl -s "$B/backfill__2026-10-15T00%3A00%3A00%2B00%3A00/validate_daily_sales_by_region" | python3 -m json.tool   # ri-001
curl -s "$B/backfill__2026-10-16T00%3A00%3A00%2B00%3A00/validate_daily_sales_by_region" | python3 -m json.tool   # ri-002
curl -s "$B/backfill__2026-10-17T00%3A00%3A00%2B00%3A00/build_daily_sales_by_region" | python3 -m json.tool      # ri-003
```

Esperado: o incidente de cada caso, com `key` igual à da tabela acima. Em
cada tentativa, o `operator` é `LakehouseDataQualityOperator` no `ri-001` e no
`ri-002`, e `LakehouseSparkSubmitOperator` no `ri-003`.

**4. Filtro por horário**

```bash
curl -s "http://127.0.0.1:8090/incidents?since=2026-10-18T21:00:00Z" | python3 -m json.tool
```

Esperado: `count: 2`, só `ri-003` e `ri-002`. O `ri-001` fica de fora porque
a sua última tentativa terminou às 20:59 UTC.

**5. Respostas de erro**

```bash
curl -s -w ' HTTP %{http_code}\n' "$B/backfill__2026-10-17T00%3A00%3A00%2B00%3A00/sources.generate_sales"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' "http://127.0.0.1:8090/incidents?since=2026-10-18T21:00:00"
```

Esperado: `404` com `task not found or not failed` (a task teve sucesso) e
`422` (horário sem fuso).

**6. Airflow fora do ar (opcional)**

Encerre a API, suba de novo com `AIRFLOW_API_URL=http://127.0.0.1:1
.venv/bin/python -m lakehouse_ops_collector.detection` e repita os passos 1 e
2. Esperado: `/health` com `airflow.status: error` e `/incidents` com `502` e
`airflow unavailable`.

## Andamento

| Data | Evento |
| --- | --- |
| 2026-10-19 | implementada; testes automáticos passando; os passos 1 a 6 rodaram com o resultado esperado na implementação |
| 2026-10-19 | `operator` passou para cada tentativa; tentativas ganharam `cycle`; `max_attempts` saiu; filtro `since` explicado |
| 2026-10-19 | teste manual aprovado pelo Postman; fase concluída |

## Próximos passos

- O coletor do incidente volta da branch `standby/incident-collector` e passa
  a receber a `key` desta API.
- O poller da proposta é esta mesma consulta repetida a cada 30 s, com a
  posição da última leitura guardada (`since`).
- Ainda não implementado, da proposta de formato: `dag_version` em cada
  tentativa; status `recovered` (falhou e depois passou) e `running` (reabriu
  e está rodando), na consulta por chave, no lugar do `404` de hoje.
- O cálculo do `cycle` só foi testado com dados simulados. Um `clear`
  controlado na run `scheduled__2026-10-18` confirmaria no Airflow real.
