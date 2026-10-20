# Fase 2: poller e registro de incidentes

- Status: em andamento; etapas 1 e 2 concluídas
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
| `first_seen_at`, `updated_at` | quando o poller viu o incidente pela primeira vez e a última mudança |

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
| 3 | uma rodada do poller registra os incidentes | não implementada |
| 4 | poller contínuo, sem duplicar e retomando de onde parou | não implementada |
| 5 | acompanhamento de um incidente até `recovered` | não implementada |

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

Esperado: as 9 colunas da tabela acima, a chave primária
`incident_pkey` e `count` igual a 0. No DBeaver, o schema `detection` aparece
no banco `ops` depois de atualizar a árvore (F5).

### Etapa 3: uma rodada do poller

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.poller --once
```

Faz uma rodada só e encerra.

Esperado: 4 incidentes registrados, os mesmos da fase 1 (`ri-001`, `ri-002`,
`ri-003` e a run a mais), todos `failed`. Rodar de novo registra 0 novos.

### Etapa 4: poller contínuo

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.poller
```

Roda a cada 30 s e mostra uma linha por rodada (novos, atualizados,
abertos). `Ctrl+C` encerra.

Esperado: as rodadas mostram 0 novos. Parar e subir de novo continua de onde
parou, sem duplicar nada.

### Etapa 5: incidente recuperado

Com o poller rodando, dê `clear` pela UI do Airflow na task
`validate_daily_sales_by_region` da run `scheduled__2026-10-18`, a run a
mais, que é descartável. O domínio está correto na `main`, então a nova
tentativa deve passar.

Esperado, no registro, para essa `key`:
- `running` enquanto a task roda;
- `recovered` quando ela terminar;
- uma 3ª tentativa com `cycle: 2`, o que confirma o `cycle` no Airflow real.

## Andamento

| Data | Evento |
| --- | --- |
| 2026-10-19 | documento criado; nada implementado |
| 2026-10-19 | banco decidido: `postgres-ops` na stack `ops` do `lakehouse-infra`; etapa 1 implementada, testes de lá passando, container no ar |
| 2026-10-20 | etapa 1: acesso ao `postgres-ops` pelo DBeaver aprovado no teste manual; etapa concluída |
| 2026-10-20 | etapa 2 implementada; testes automáticos passando; DDL conferido no `postgres-ops` dentro de uma transação desfeita (`ROLLBACK`), sem deixar a tabela criada |
| 2026-10-20 | etapa 2: `init` rodado duas vezes (criada e depois "nada alterado") e tabela vazia conferida no DBeaver no teste manual; etapa concluída |

## Fora desta fase

- Endpoint na API para consultar o registro.
- Agrupar incidentes da mesma run.
- Rodar o poller como serviço (container ou systemd).
- Volta do coletor, que passa a ler as chaves do registro.
