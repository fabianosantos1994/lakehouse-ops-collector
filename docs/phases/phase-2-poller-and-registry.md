# Fase 2: poller e registro de incidentes

- Status: concluída em 2026-10-21
- Data: 2026-10-19

A fase 1 entregou uma API que responde quando alguém pergunta. Esta fase cria
o **poller**, que pergunta sozinho a cada 30 s, e o **registro**, uma tabela
no PostgreSQL que guarda cada incidente uma vez só. O registro é de onde o
coletor vai receber as chaves quando voltar.

```text
Airflow ──(fase 1: detecção)──▶ poller ──▶ PostgreSQL (registro) ──▶ coletor (depois)
```

O poller chama os casos de uso da detecção diretamente, sem passar pela API
HTTP. A API da fase 1 continua igual e pode rodar ao mesmo tempo.

## Como funciona

A cada 30 s, o poller faz duas coisas:

1. **Busca incidentes novos.** Chama `find_incidents` com `since` e grava no
   registro. Um incidente com a mesma `key` não é duplicado: é atualizado.
2. **Acompanha os incidentes abertos.** Consulta cada incidente do registro
   que ainda está aberto e atualiza o status: continua `failed`, voltou a
   rodar (`running`) ou passou depois de um `clear` (`recovered`).

**De onde vem o `since`.** É o maior `end_date` da última tentativa entre os
incidentes já registrados, menos uma margem de 5 minutos. A margem cobre duas
tasks paralelas que terminam quase juntas, quando uma delas só aparece como
`failed` no Airflow depois da leitura. Como o registro não duplica, ler de
novo o mesmo incidente não causa problema. Com o registro vazio, o poller lê
todo o histórico.

**Status no registro:**

| Status | Quando |
| --- | --- |
| `failed` | a task falhou em todas as tentativas do ciclo atual |
| `running` | depois de um `clear`, a task está rodando ou esperando uma nova tentativa |
| `recovered` | depois de falhar, a task terminou com sucesso |

Um incidente `recovered` não é mais acompanhado. Se a mesma `key` falhar de
novo depois de um `clear`, ela volta a `failed` com o novo ciclo nas
tentativas.

## O que será construído

| Peça | Onde | O que faz |
| --- | --- | --- |
| serviço `postgres-ops` | `lakehouse-infra`, stack nova `ops` | PostgreSQL do registro, com volume próprio, em `127.0.0.1:5435` |
| schema `detection` | este repositório (`schema.sql`) | tabela `incident`; aplicado por um comando deste repositório, não pela infra |
| port `IncidentRegistry` e adapter PostgreSQL | `detection/` | gravar e ler o registro |
| `get_incident` com `running` e `recovered` | `detection/service.py` | a consulta por chave responde também depois da falha, em vez de `404` |
| poller | `detection/poller.py` | o laço de 30 s, rodado na mão no terminal |

Tabela `detection.incident` (criada na etapa 2; o DDL está em
[`schema.sql`](../../src/lakehouse_ops_collector/detection/schema.sql)):

| Coluna | Conteúdo |
| --- | --- |
| `dag_id`, `run_id`, `task_id`, `map_index` | a `key`; chave primária da tabela |
| `status` | `failed`, `running` ou `recovered`; outro valor é recusado pelo banco |
| `attempts` | as tentativas, no mesmo formato da API (`jsonb`) |
| `last_end_date` | `end_date` da última tentativa; base do `since`; vazio enquanto a tentativa atual não terminou |
| `created_at_dt`, `created_at_ds` | quando e por qual modo o registro foi criado; não mudam depois |
| `updated_at_dt`, `updated_at_ds` | quando e por qual modo o registro foi alterado por último |

O sufixo `_ds` indica quem gravou. Hoje só existe o modo `poller`, a consulta
ao Airflow agendada pelo poller; outros modos poderão alimentar a mesma tabela
(por exemplo, um registro manual pela API) e terão o próprio valor.


## Decisões

| Assunto | Decisão | Motivo |
| --- | --- | --- |
| armazenamento | PostgreSQL desde o início, sem a etapa em arquivo | decisão sua; o poller já precisa consultar incidentes abertos |
| onde fica o banco | serviço próprio `postgres-ops`, stack `ops` do `lakehouse-infra`, banco `ops` em `127.0.0.1:5435` | ADR 0004 do `lakehouse-infra`: um PostgreSQL por consumidor; o `platform_metadata` é da auditoria de Data Quality. Registrado no ADR 0013 do `lakehouse-infra` |
| quem cria as tabelas | este repositório, com `CREATE TABLE IF NOT EXISTS` | a infra entrega o banco vazio; o formato do registro muda junto com o código |
| escrita | este repositório passa a gravar, **só no banco `ops`** (ADR 0001) | Airflow, domínios e os outros bancos continuam somente leitura |
| usuário do banco | durante o desenvolvimento, o `ops`, dono do banco; ao fim, um usuário restrito ao schema `detection` | decisão sua; liberdade para recriar o schema enquanto ele muda, menor privilégio depois |
| driver | `psycopg[binary]==3.3.6` | driver PostgreSQL atual para Python; o `binary` dispensa a `libpq` no host |
| execução do poller | processo rodado na mão no terminal, com `--once` para uma rodada só | igual à API; agendar como serviço fica para depois |
| rodada com falha | no laço, mostra `rodada falhou: ...` e segue; com `--once`, encerra com erro | o Airflow ou o banco fora do ar por um tempo não derruba o poller; a rodada seguinte tenta de novo do mesmo `since` |
| intervalo | 30 s, configurável em `POLLER_INTERVAL_SECONDS`; contado do fim de uma rodada ao início da seguinte | uma rodada lenta não se sobrepõe à seguinte |
| incidentes `retrying` | não são registrados | o coletor só age na falha definitiva |
| testes | registro simulado nos testes automáticos; o adapter PostgreSQL é verificado no teste manual | os testes não acessam o ambiente |

## Execução

Cada etapa é implementada por mim e testada por você. Ela só fica
"concluída" depois do seu teste. Os comandos abaixo são os previstos e podem
mudar na implementação.

| Etapa | Entrega | Status |
| --- | --- | --- |
| 1 | banco `postgres-ops` no ar | concluída em 2026-10-20 |
| 2 | schema criado pelo comando deste repositório | concluída em 2026-10-20 |
| 3 | uma rodada do poller registra os incidentes | concluída em 2026-10-21 |
| 4 | poller contínuo, sem duplicar e retomando de onde parou | concluída em 2026-10-21 |
| 5 | acompanhamento de um incidente até `recovered` | concluída em 2026-10-21 |

### Etapa 1: subir o banco

```bash
cd ~/projects/lakehouse-infra
docker compose --project-name lakehouse-ops -f docker/ops/docker-compose.yml --env-file docker/.env up -d
```

Sobe o `postgres-ops`. O `--project-name lakehouse-ops` dá ao container o
mesmo padrão de nome das outras stacks. O mesmo resultado sai de
`./scripts/start-all.sh up ops`.

Implementado no `lakehouse-infra` (ainda sem commit):

- stack nova `docker/ops/docker-compose.yml`, com o serviço `postgres-ops`
  (`postgres:16`), volume `lakehouse_ops_postgres_data` e
  `restart: unless-stopped`;
- banco `ops`, usuário `ops` e senha `ops`, vindos de `OPS_DB_*` no
  `docker/.env`; sem essas variáveis, o Compose usa os mesmos valores;
- a stack `ops` no `scripts/start-all.sh`, nos testes, no README, no
  `docs/architecture.md` e no ADR 0013 de lá.

Não implementado nesta etapa: nenhuma tabela. O banco sobe vazio; o schema
vem na etapa 2.

**Teste manual**

```bash
docker ps --filter name=lakehouse-ops --format '{{.Names}}  {{.Status}}  {{.Ports}}'
```

Esperado: `lakehouse-ops-postgres-ops-1  Up ... (healthy)  127.0.0.1:5435->5432/tcp`.

```bash
docker exec lakehouse-ops-postgres-ops-1 psql -U ops -d ops -c '\dn' -c '\dt'
```

Esperado: só o schema `public` e `Did not find any relations.` Pelo
Windows, o mesmo banco abre num cliente como o DBeaver com o port forward
`-L 5435:127.0.0.1:5435` (host `localhost`, porta `5435`, banco `ops`).

### Etapa 2: criar o schema

```bash
cd ~/projects/lakehouse-ops-collector
.venv/bin/python -m lakehouse_ops_collector.detection.registry init
```

Cria o schema `detection` e a tabela `incident` no banco `ops`. Só cria o
que falta: rodar de novo não altera nada.

Antes da primeira vez, acrescente ao `.env` deste repositório as linhas
`OPS_DB_*` do `.env.example`. A senha (`OPS_DB_PASSWORD`) não tem valor padrão
no código, como as credenciais do Airflow; as outras têm padrão igual ao do
`postgres-ops`. Sem a senha, o comando encerra com `configuração incompleta:
OPS_DB_PASSWORD`.

Implementado:

- `detection/schema.sql`: o DDL da tabela, com `IF NOT EXISTS`;
- `detection/adapters/postgres.py`: conexão por `psycopg` e `init_schema`, o
  único código que grava (ADR 0001);
- `detection/registry.py`: o comando `init`, que diz se criou a tabela ou se
  ela já existia;
- `RegistrySettings` em `detection/settings.py`: lê só `OPS_DB_*`, então o
  `init` não precisa das credenciais do Airflow;
- testes em `tests/test_registry.py`, com a conexão simulada.

Não implementado nesta etapa: gravar e ler incidentes (port `IncidentRegistry`)
e o poller, que são da etapa 3; o usuário restrito do banco, que fica para o
fim do desenvolvimento.

**Teste manual**

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.registry init
.venv/bin/python -m lakehouse_ops_collector.detection.registry init
```

Esperado: a primeira vez imprime `detection.incident criada em
127.0.0.1:5435/ops`; a segunda, `detection.incident já existia em
127.0.0.1:5435/ops; nada alterado`.

```bash
docker exec lakehouse-ops-postgres-ops-1 psql -U ops -d ops -c '\d detection.incident' -c 'select count(*) from detection.incident'
```

Esperado: as 11 colunas da tabela acima, a chave primária
`incident_pkey` e `count` igual a 0. No DBeaver, o schema `detection` aparece
no banco `ops` depois de atualizar a árvore (F5).

### Etapa 3: uma rodada do poller

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.poller --once
```

Faz uma rodada só e encerra. Lê o Airflow (as credenciais `AIRFLOW_*` do
`.env`, como a API) e grava no registro (`OPS_DB_*`). A rodada mostra uma
linha:

```text
<agora>  since=<início da leitura>  lidos=N  novos=N  atualizados=N  abertos=N
```

- `since`: o maior `last_end_date` do registro menos 5 minutos; `-` com o
  registro vazio, quando a rodada lê todo o histórico;
- `lidos`: incidentes `failed` que o Airflow devolveu desde o `since`;
- `novos` e `atualizados`: chaves gravadas pela primeira vez e chaves que já
  existiam mas mudaram de status ou de tentativas; uma chave igual não conta;
- `abertos`: incidentes do registro que não estão `recovered`.

Implementado:

- `detection/poller.py`: a rodada (`run_once`) e o comando `--once`;
- port `IncidentRegistry` em `detection/ports.py` e, no adapter
  `adapters/postgres.py`, `save_all` (grava a rodada numa transação só, com
  `INSERT ... ON CONFLICT` pela `key`), `latest_end_date` e `open_count`;
- teto de 10.000 incidentes por rodada; se for atingido, a linha avisa que os
  mais antigos ficaram de fora;
- erros do Airflow ou do banco encerram a rodada com `rodada falhou: ...`, sem
  gravar nada; sem a tabela, a mensagem pede para rodar o `init`;
- testes em `tests/test_poller.py` (Airflow e registro simulados) e em
  `tests/test_registry.py`.

Não implementado nesta etapa: o laço de 30 s (sem `--once`, o comando avisa
que é da etapa 4) e o acompanhamento dos abertos até `running` e `recovered`
(etapa 5). `created_at_dt` e `created_at_ds` são gravados só na inserção;
`updated_at_dt` e `updated_at_ds` mudam só quando o status ou as tentativas
mudam. O poller grava `poller` nos dois `_ds`.

**Teste manual**

A tabela foi recriada com as colunas `_dt` e `_ds` e já está populada pela
rodada que rodei depois da recriação (4 novos). Rode mais uma vez:

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.poller --once
```

Esperado: `since=2026-10-18T22:21:30Z  lidos=1  novos=0  atualizados=0  abertos=4`.
O `since` é o fim do `ri-003` (22:26:30) menos 5 minutos, e só o `ri-003`
terminou depois disso; ele é relido, mas não muda nada.

Numa tabela vazia, a primeira rodada mostra
`since=-  lidos=4  novos=4  atualizados=0  abertos=4`.

```bash
docker exec lakehouse-ops-postgres-ops-1 psql -U ops -d ops -c "select run_id, task_id, status, jsonb_array_length(attempts) as attempts, created_at_dt, created_at_ds, updated_at_dt, updated_at_ds from detection.incident order by last_end_date desc"
```

Esperado: 4 linhas, os mesmos incidentes da fase 1 (`ri-003`, `ri-002`,
`ri-001` e a run `scheduled__2026-10-18`), todos `failed`, com 2 tentativas,
`poller` em `created_at_ds` e `updated_at_ds`, e `updated_at_dt` igual a
`created_at_dt`, porque a sua rodada não alterou nada. No DBeaver, a coluna
`attempts` mostra o JSON no mesmo formato da API.

### Etapa 4: poller contínuo

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.poller
```

Roda uma rodada a cada 30 s e mostra uma linha por rodada, no mesmo formato
do `--once`. `Ctrl+C` encerra. A pausa vem de `POLLER_INTERVAL_SECONDS` no
`.env` (padrão 30).

Implementado:

- `run_loop` em `detection/poller.py`: a rodada da etapa 3 repetida, com a
  pausa entre elas;
- uma rodada com erro do Airflow ou do banco mostra `rodada falhou: ...` numa
  linha, não grava nada e não encerra o laço;
- `Ctrl+C` encerra com `poller encerrado`; uma rodada interrompida no meio é
  desfeita pela transação, sem gravação pela metade;
- a retomada não precisa de nada novo: o `since` vem do registro, então
  subir de novo continua de onde parou;
- `PollerSettings` em `detection/settings.py`; intervalo zero ou negativo
  encerra com `configuração incompleta: POLLER_INTERVAL_SECONDS`.

Não implementado nesta etapa: o acompanhamento dos abertos até `running` e
`recovered` (etapa 5) e rodar o poller como serviço.

**Teste manual**

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.poller
```

Esperado: `poller iniciado: uma rodada a cada 30 s; Ctrl+C encerra` e, a cada
30 s, `since=2026-10-18T22:21:30Z  lidos=1  novos=0  atualizados=0  abertos=4`.
Depois de duas ou três rodadas, `Ctrl+C` mostra `poller encerrado`. Suba de
novo: a primeira rodada já sai com 0 novos, sem duplicar. No DBeaver, a
tabela continua com 4 linhas e `updated_at_dt` igual a `created_at_dt`.

Opcional, a falha sem derrubar nada no ambiente: suba com uma porta errada do
banco.

```bash
OPS_DB_PORT=1 POLLER_INTERVAL_SECONDS=5 .venv/bin/python -m lakehouse_ops_collector.detection.poller
```

Esperado: a cada 5 s, `rodada falhou: registro indisponível em
127.0.0.1:1/ops: ...`, e o poller continua até o `Ctrl+C`.

### Etapa 5: incidente recuperado

Com o poller rodando, dê `clear` pela UI do Airflow na task
`validate_daily_sales_by_region` da run `scheduled__2026-10-18`, a run a
mais, que é descartável. O domínio está correto na `main`, então a nova
tentativa deve passar.

Implementado:

- a cada rodada, depois de buscar as falhas novas, o poller consulta no
  Airflow, pela chave, cada incidente aberto do registro e grava o estado
  atual; as duas listas vão na mesma transação, uma entrada por chave;
- `get_incident` em `detection/service.py` responde também depois da falha.
  Com uma tentativa anterior `failed` e o estado atual:

  | Estado atual no Airflow | Status |
  | --- | --- |
  | `failed` | `failed` |
  | `up_for_retry` | `retrying` na API; `running` no registro, que não tem `retrying` |
  | sem estado (logo depois do `clear`), `scheduled`, `queued`, `running`, `deferred`, `up_for_reschedule`, `restarting` | `running` |
  | `success` | `recovered` |
  | `skipped`, `upstream_failed`, `removed` e outros | não muda o incidente |

- logo depois do `clear`, antes de o scheduler agendar a task, o Airflow a
  deixa sem estado e com o número da tentativa anterior; nesse intervalo, as
  tentativas continuam mostrando a 2 como `failed`, e não uma tentativa vazia;
- um incidente `recovered` sai do acompanhamento. Se a mesma `key` falhar de
  novo, a busca de falhas novas a traz de volta a `failed`;
- a API passa a responder `running` ou `recovered` em
  `GET /incidents/{dag_id}/{run_id}/{task_id}`; o `404` fica para a task que
  não existe ou nunca falhou.

Não implementado nesta etapa: `dag_version` nas tentativas e rodar o poller
como serviço.

**Teste manual**

1. Suba o poller e deixe rodando:

   ```bash
   .venv/bin/python -m lakehouse_ops_collector.detection.poller
   ```

   Esperado: `lidos=1  novos=0  atualizados=0  abertos=4` a cada rodada.

2. Despause a DAG `commercial_daily_sales_by_region`. Ela tem
   `catchup: False`, então cria só a run `scheduled__2026-10-20T00:00:00+00:00`,
   que roda o pipeline inteiro. Espere essa run terminar.

   Esperado: as rodadas continuam com `novos=0`. Se a run do dia 20 falhar,
   aparece `novos=1` e `abertos=5`: é um incidente novo, registrado como
   qualquer outro.

3. Dê `clear` em `validate_daily_sales_by_region` da run
   `scheduled__2026-10-18T00:00:00+00:00`. É a última task da DAG, então não
   há tasks depois dela para incluir.

   Esperado, nas rodadas seguintes:
   - `atualizados=1` com a task de volta a rodar: o registro passa a
     `running`; pode aparecer mais um `atualizados=1` quando a 3ª tentativa
     começa;
   - `atualizados=1` e `abertos=3` quando ela termina com sucesso: o registro
     passa a `recovered`;
   - depois disso, `atualizados=0` e `abertos=3`.

4. Confira o registro:

   ```bash
   docker exec lakehouse-ops-postgres-ops-1 psql -U ops -d ops -c "select status, (select jsonb_agg(a->'cycle') from jsonb_array_elements(attempts) a) as cycles, (select jsonb_agg(a->'state') from jsonb_array_elements(attempts) a) as states, updated_at_dt, updated_at_ds from detection.incident where run_id = 'scheduled__2026-10-18T00:00:00+00:00'"
   ```

   Esperado: `recovered`, ciclos `[1, 1, 2]`, estados
   `["failed", "failed", "success"]` e `updated_at_ds` `poller`. A 3ª
   tentativa com `cycle: 2` confirma o `cycle` no Airflow real.

5. Opcional, a API: com ela no ar, a consulta por chave dessa run devolve
   `"status": "recovered"` em vez de `404`.

Se a nova tentativa falhar, com `retries: 1` vêm a 3ª e a 4ª tentativas no
`cycle: 2`, cerca de 5 minutos uma da outra; o registro passa por `running` e
volta a `failed` com 4 tentativas. O `cycle` fica confirmado do mesmo jeito.

Depois do teste, a DAG continua despausada e roda todo dia às 00:00 UTC, a
menos que você a pause de novo.

## Andamento

| Data | Evento |
| --- | --- |
| 2026-10-19 | documento criado; nada implementado |
| 2026-10-19 | banco decidido: `postgres-ops` na stack `ops` do `lakehouse-infra`; etapa 1 implementada, testes de lá passando, container no ar |
| 2026-10-20 | etapa 1: acesso ao `postgres-ops` pelo DBeaver aprovado no teste manual; etapa concluída |
| 2026-10-20 | etapa 2 implementada; testes automáticos passando; DDL conferido no `postgres-ops` dentro de uma transação desfeita (`ROLLBACK`), sem deixar a tabela criada |
| 2026-10-20 | etapa 2: `init` rodado duas vezes (criada e depois "nada alterado") e tabela vazia conferida no DBeaver no teste manual; etapa concluída |
| 2026-10-20 | etapa 3 implementada; testes automáticos passando; duas rodadas contra o Airflow e o `postgres-ops` reais numa transação desfeita (`ROLLBACK`): 4 novos e depois 0, registro vazio ao final |
| 2026-10-20 | sua rodada do poller registrou os 4 incidentes `failed` |
| 2026-10-21 | colunas `first_seen_at` e `updated_at` trocadas por `created_at_dt`, `created_at_ds`, `updated_at_dt` e `updated_at_ds`, a seu pedido; tabela apagada (`DROP TABLE`, autorizado) e recriada pelo `init`; uma rodada repopulou os 4 incidentes e a seguinte não alterou nada; desenho do `collection.job` registrado como planejado |
| 2026-10-21 | etapa 3: tabela com o schema novo e os 4 incidentes conferida no teste manual; etapa concluída |
| 2026-10-21 | etapa 4 implementada; testes automáticos passando; no ambiente real, 3 rodadas de 2 s com 0 novos e nada alterado, e com a porta do banco errada as rodadas falharam sem encerrar o laço |
| 2026-10-21 | etapa 4: laço, `Ctrl+C`, retomada sem duplicar e falha com porta errada conferidos no teste manual; etapa concluída |
| 2026-10-21 | etapa 5 implementada; testes automáticos passando; no ambiente real, uma rodada com o acompanhamento dos 4 abertos não alterou nada, e uma task que nunca falhou continua sem incidente (`404`) |
| 2026-10-21 | etapa 5: DAG despausada, run do dia 20 sem incidente novo; `clear` na run `scheduled__2026-10-18` levou o registro de `failed` a `running` e a `recovered`, com ciclos `[1, 1, 2]` e estados `failed`, `failed`, `success`; o `cycle` ficou confirmado no Airflow real; etapa e fase concluídas |

## Fora desta fase

- Endpoint na API para consultar o registro.
- Agrupar incidentes da mesma run.
- Rodar o poller como serviço (container ou systemd).
- Volta do coletor, que passa a ler as chaves do registro (desenho abaixo).

## Processamento pelo coletor (planejado)

Desenho para a volta do coletor; **nada disto está implementado**. O `status`
do registro diz o que aconteceu com a task no Airflow. O processamento diz o
que já foi feito com o incidente, e fica numa tabela do coletor,
`collection.job`, no mesmo banco `ops`. Um incidente pode estar `failed` no
Airflow e `done` na coleta.

| Estado | Significado | Quando |
| --- | --- | --- |
| `pending` | ainda não processado | incidente `failed` no registro sem job para a sua `key` e o seu `cycle` |
| `processing` | o coletor está montando o pacote | o coletor pega o job e grava `started_at` |
| `done` | pacote gravado | grava `finished_at` e onde ficou o pacote |
| `error` | a coleta falhou | grava a mensagem; volta a `pending` até um limite de tentativas |

- **Um job por `key` e `cycle`.** Depois de um `clear` e de uma falha nova
  (ciclo 2), surge um job novo: a falha nova pede um pacote novo.
- **Sem dois coletores no mesmo job.** O coletor pega o job com
  `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Job travado volta.** Um job em `processing` há mais tempo que um limite
  volta a ficar disponível, para o caso de o coletor cair no meio.
- **Cada parte grava o que é dela.** O coletor só lê `detection.incident` e
  grava só em `collection.job`; o poller não conhece a coleta.
- **A decidir na volta do coletor:** o limite de tentativas e o tempo de
  `processing`; e o que fazer com um incidente que ficou `recovered` antes da
  coleta (coletar mesmo assim ou pular).
