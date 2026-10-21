# Fase 3: poller e API em container

- Status: implementada, sem o seu teste manual; **suspensa em 2026-10-21**
  com a integração de AI (containers removidos; os serviços estão na branch
  `standby/ai-integration` do `lakehouse-infra`)
- Data: 2026-10-21

Na fase 2, o poller roda à mão num terminal e para com `Ctrl+C`. Esta fase o
coloca num container da stack `ops` do `lakehouse-infra`, ao lado do
`postgres-ops`, rodando sozinho e voltando depois de reiniciar o host. A API de
detecção vai num segundo serviço, da mesma imagem. O código do poller e da API
não muda: só o lugar onde roda e de onde vem a configuração.

```text
stack ops (lakehouse-infra)
  postgres-ops    banco do registro                    127.0.0.1:5435
  poller          rodada a cada 30 s                   sem porta
  detection-api   API da fase 1                        127.0.0.1:8090
        │
        └── rede lakehouse-network ──▶ airflow-apiserver:8080
```

A mesma imagem será reaproveitada pelo coletor da
[fase 4](phase-4-incident-collector.md).

## O que será construído

| Peça | Onde | O que faz |
| --- | --- | --- |
| `Dockerfile` e `.dockerignore` | este repositório | imagem `lakehouse-ops-collector`, com Python 3.12 e o pacote instalado; fora da imagem: `.venv`, `.env`, `incidents/` e `tests/` |
| serviço `poller` | `docker/ops/docker-compose.yml` do `lakehouse-infra` | roda o `registry init` (não altera nada se a tabela existe) e depois o laço do poller |
| serviço `detection-api` | idem | a API da fase 1, publicada só no loopback do host |
| variável `LAKEHOUSE_OPS_COLLECTOR_HOST_PATH` | `docker/.env` do `lakehouse-infra` | caminho deste repositório, para o build da imagem; o mesmo padrão do `LAKEHOUSE_PLATFORM_CORE_HOST_PATH` |

## Decisões

Confirmadas por você em 2026-10-21, todas como propostas.

| Assunto | Proposta | Motivo |
| --- | --- | --- |
| onde ficam os arquivos | `Dockerfile` aqui; serviços no `lakehouse-infra` | o código é deste repositório; o ambiente (rede, banco, portas, credenciais) é da infra, como o Airflow, que é construído lá com arquivos do `lakehouse-platform-core-mvp` |
| imagem base | `python:3.12.11-slim-bullseye`, a mesma do executor do Spark no `lakehouse-infra` | versão fixada e já usada no ambiente |
| configuração | por variáveis de ambiente do Compose, sem `.env` dentro da imagem: `AIRFLOW_API_URL=http://airflow-apiserver:8080`, `OPS_DB_HOST=postgres-ops`, `OPS_DB_PORT=5432` | o código já lê essas variáveis; dentro da rede, os nomes dos serviços substituem `127.0.0.1` |
| credenciais | `AIRFLOW_ADMIN_USERNAME`/`AIRFLOW_ADMIN_PASSWORD` e `OPS_DB_PASSWORD`, todas do `docker/.env` do `lakehouse-infra` | um lugar só para as credenciais do ambiente; os usuários restritos ficam para o fim do desenvolvimento (MP-26 e MP-27 do `lakehouse-ai-ops`) |
| parada | `stop_signal: SIGINT` no Compose | o `docker stop` vira o mesmo `Ctrl+C` da fase 2, que encerra limpo e desfaz uma rodada pela metade, sem mudar o código |
| reinício | `restart: unless-stopped` nos dois serviços | voltam sozinhos depois de reiniciar o host, como o `postgres-ops` |
| ordem no `start-all.sh` | a stack `ops` passa a subir depois do `airflow` (e a descer antes) | o poller encontra o Airflow já no ar; sem isso, as primeiras rodadas falham e se recuperam sozinhas |
| porta da API | dentro do container, `DETECTION_API_HOST=0.0.0.0`; no host, `${LAKEHOUSE_BIND_ADDRESS:-127.0.0.1}:8090` | o container precisa aceitar conexões da rede Docker; no host, segue o ADR 0011 do `lakehouse-infra`. A regra "a API escuta só em `127.0.0.1`" do `AGENTS.md` passa a valer para a porta publicada |
| health check | na API, `GET /health`; no poller, nenhum | nenhum serviço depende do poller; as linhas das rodadas em `docker logs` mostram se ele está vivo |
| poller à mão | não rodar o poller do terminal com o container no ar | os dois gravariam o mesmo registro; não duplica nada, mas as linhas ficam confusas. O `--once` à mão continua valendo para testes |

## Execução

Cada etapa é implementada por mim e testada por você; só fica "concluída"
depois do seu teste. Construir a imagem, subir ou derrubar containers e mexer
no `docker/.env` do `lakehouse-infra` são feitos com o seu ok, em cada etapa.

| Etapa | Entrega | Status |
| --- | --- | --- |
| 1 | imagem construída; uma rodada no container, na rede `lakehouse-network` | implementada, aguardando teste manual |
| 2 | serviço `poller` na stack `ops`: sobe com `start-all.sh`, rodadas em `docker logs`, `docker stop` limpo | implementada, aguardando teste manual |
| 3 | operação: reiniciar o container retoma sem duplicar; reiniciar o `postgres-ops` faz as rodadas falharem e voltarem sozinhas | implementada, aguardando teste manual |
| 4 | serviço `detection-api`: o teste manual da fase 1 pela porta 8090, inclusive pelo Postman do Windows | implementada, aguardando teste manual |

Esperado, em resumo:

- **Etapa 1.** `docker build` sem erro e, no container temporário, a mesma
  linha da fase 2: `since=2026-10-21T02:17:37Z  lidos=0  novos=0  atualizados=0  abertos=3`
  (o `since` andou com o fim da tentativa que passou na etapa 5 da fase 2).
- **Etapa 2.** `docker ps` com `lakehouse-ops-poller-1` rodando e
  `docker logs -f` com uma linha a cada 30 s; `docker stop` termina com
  `poller encerrado`.
- **Etapa 3.** Depois de cada reinício, o registro continua com 4 linhas, e
  as rodadas voltam a `rodada falhou: ...` só enquanto o banco está fora.
- **Etapa 4.** As respostas da fase 1 em `http://127.0.0.1:8090`, e
  `lakehouse-ops-detection-api-1` como `healthy`.

### Implementado

- Neste repositório: `Dockerfile` (base `python:3.12.11-slim-bullseye`,
  usuário `ops` sem root, comando padrão do poller) e `.dockerignore` (fora:
  `.env`, `.venv`, `incidents/`, `tests/`, `docs/`, `.git`).
- No `lakehouse-infra` (ADR 0014 de lá, hoje só na branch
  `standby/ai-integration`): os serviços `poller` e
  `detection-api` em `docker/ops/docker-compose.yml`, a variável
  `LAKEHOUSE_OPS_COLLECTOR_HOST_PATH` no `docker/.env` e no `.env.example`, a
  stack `ops` depois do `airflow` no `start-all.sh`, testes e documentação.
- O código do poller e da API não mudou.

Não implementado: health check do poller e usuários restritos (MP-26 e
MP-27).

### Teste manual

No `lakehouse-infra`:

```bash
docker ps --filter name=lakehouse-ops --format '{{.Names}}  {{.Status}}  {{.Ports}}'
docker logs -f lakehouse-ops-poller-1
```

Esperado: `lakehouse-ops-poller-1` `Up`, `lakehouse-ops-detection-api-1`
`(healthy)` em `127.0.0.1:8090` e, nos logs, uma linha a cada 30 s com
`abertos=3`.

Pelo Postman do Windows, com o port forward `-L 8090:127.0.0.1:8090`: os
passos da fase 1 em `http://localhost:8090`.

**Falha forçada (o seu teste).** Provoque a falha numa task da DAG
`commercial_daily_sales_by_region`. Com `retries: 1`, a task só fica `failed`
depois da 2ª tentativa, cerca de 5 minutos depois da 1ª; enquanto ela está em
`up_for_retry`, o poller não registra nada, porque só a falha definitiva abre
incidente. Esperado, depois disso:

- nos logs, uma rodada com `novos=1` e `abertos=4`;
- no registro, a linha da task com `status` `failed`, 2 tentativas no
  `cycle` 1 e `created_at_ds` e `updated_at_ds` `poller`:

```bash
docker exec lakehouse-ops-postgres-ops-1 psql -U ops -d ops -c "select run_id, task_id, status, (select jsonb_agg(a->'cycle') from jsonb_array_elements(attempts) a) as cycles, created_at_dt, created_at_ds from detection.incident order by created_at_dt desc limit 3"
```

Lembrete do MP-23: despausar a DAG cria uma run agendada; pause de novo
depois do teste, se não quiser a execução diária.

## Andamento

| Data | Evento |
| --- | --- |
| 2026-10-21 | documento criado; nada implementado |
| 2026-10-21 | implementada e no ar: imagem construída, `poller` e `detection-api` subiram com `./scripts/start-all.sh up ops`; testes automáticos dos dois repositórios passando. Conferido na implementação: rodada `lidos=0  novos=0  atualizados=0  abertos=3`; teste da fase 1 pela porta 8090 (`/health` ok, 3 `failed`, dia 18 `recovered` com ciclos 1, 1, 2, `404` e `422`); `docker stop` encerrou na hora com `poller encerrado` e código 0; `docker start` retomou sem duplicar; com o `postgres-ops` parado, uma rodada falhou (`registro indisponível`) e a seguinte voltou sozinha, sem reiniciar o container |
| 2026-10-21 | suspensa antes do teste manual, junto com a integração de AI: containers do `poller` e da `detection-api` removidos; volume do registro e imagem mantidos; a stack `ops` saiu da `main` do `lakehouse-infra` (ADR 0015 de lá). Mapa e retomada: `docs/suspension.md` do `lakehouse-ai-ops` |

## Fora desta fase

- O coletor do incidente ([fase 4](phase-4-incident-collector.md)).
- Usuários restritos do Airflow e do banco (MP-26 e MP-27), no fim do
  desenvolvimento.
- Health check do poller (por exemplo, um arquivo com a hora da última
  rodada), se for preciso.
