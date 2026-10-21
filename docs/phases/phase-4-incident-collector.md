# Fase 4: coletor do incidente

- Status: planejada, nada implementado; suspensa em 2026-10-21 com a
  integração de AI
- Data: 2026-10-21 (era a fase 3; passou a 4 quando o poller em container virou a fase 3)

A fase 2 deixou no registro (`detection.incident`) cada incidente, uma vez só.
Esta fase traz de volta o **coletor**, que monta o **pacote do incidente**: o
arquivo JSON com o que o diagnóstico precisa (tentativas, eventos `lh_*`,
checks reprovados, trecho do arquivo do domínio). O pacote é o contrato com o
`lakehouse-ai-ops`, que só o lê.

```text
Airflow ──▶ poller ──▶ detection.incident ──▶ coletor ──▶ collection.job + incidents/<chave>/package.json ──▶ lakehouse-ai-ops (depois)
```

O ponto de partida é o que está parado na branch `standby/incident-collector`:
o plano do pacote v1 e a etapa 1 implementada. A branch saiu da `main` antes
da fase 1, então o código é **portado** para a `main`, e não mesclado: a chave
do incidente passa a ser a de `incident.py`, e o resto segue o padrão das
fases 1 e 2.

## Como funciona

Dois modos, que gravam o mesmo pacote:

1. **Pela fila.** O coletor lê no registro os incidentes `failed` sem pacote
   e processa um por um, marcando cada um em `collection.job`
   (`pending` → `processing` → `done` ou `error`). É o desenho da
   [fase 2](phase-2-poller-and-registry.md#processamento-pelo-coletor-planejado).
2. **À mão, por chave.** `collect --dag-id ... --run-id ... --task-id ...`
   monta o pacote de um incidente. Serve para os 3 incidentes de referência,
   que precisam da falha reaplicada no domínio para o pacote ler o arquivo com
   o defeito (MP-14).

O pacote fica em `incidents/<dag_id>/<run_id>/<task_id>/package.json`, fora do
Git, como na branch parada.

## O que será construído

| Peça | Onde | O que faz |
| --- | --- | --- |
| subpacote `collection` | `src/lakehouse_ops_collector/collection/` | CLI, caso de uso `collect_incident`, ports e adapters, portados da branch parada |
| `package.py` | `src/lakehouse_ops_collector/` | formato do pacote (`IncidentPackage`, `package_version`), usando a chave de `incident.py` |
| schema `collection` | `collection/schema.sql` | tabela `collection.job`, criada pelo `init`, como o `detection` |
| adapters de fonte | `collection/adapters/` | Airflow (run, logs, eventos `lh_*`), `platform_metadata` (checks) e arquivos do domínio |
| `JsonPackageStore` | `collection/adapters/json_store.py` | grava o pacote, já existe na branch parada |

## Decisões a confirmar

| Assunto | Proposta | Motivo |
| --- | --- | --- |
| cliente do Airflow | mover token, renovação e paginação do adapter da detecção para um módulo compartilhado (`lakehouse_ops_collector/airflow.py`), usado pelos dois subpacotes | hoje o `AGENTS.md` só deixa os subpacotes importarem `incident.py`; sem o módulo comum, o coletor copiaria o cliente |
| tentativas no pacote | lidas do registro (`detection.incident.attempts`), já com `number` e `cycle` | o cálculo do ciclo fica num lugar só; o coletor só lê o registro, não o código da detecção |
| incidente `recovered` antes da coleta | não gera job; só `failed` entra na fila | como o poller, a coleta age na falha definitiva; o `collect` à mão continua coletando qualquer chave |
| um job por `key` e `cycle` | sim | uma falha nova depois de um `clear` pede um pacote novo |
| limites do job | até 3 tentativas com `error`; `processing` por mais de 10 minutos volta a `pending` | o coletor que cair no meio não deixa o job preso |
| credenciais do `platform_metadata` | usuário `platform`, por `.env`, como na branch parada | o usuário somente leitura é o MP-26 |
| `.env` | as variáveis novas do coletor entram no mesmo `.env` do repositório | um arquivo só para a API, o poller e o coletor |
| onde o coletor roda | na mesma imagem do poller da [fase 3](phase-3-poller-container.md), como mais um serviço da stack `ops` ou chamado pelo poller | a imagem já tem o pacote inteiro e as variáveis de acesso |

## Execução

Cada etapa é implementada por mim e testada por você; só fica "concluída"
depois do seu teste. Os comandos de cada etapa entram no documento quando ela
for implementada. As etapas 4 a 11 são as etapas 2 a 10 do plano da branch
parada, com as mesmas entregas e o mesmo esperado nos 3 casos.

| Etapa | Entrega | Status |
| --- | --- | --- |
| 1 | `collect` à mão: valida a chave e grava o pacote com `package_version`, `collected_at` e `incident` | não implementada |
| 2 | schema `collection` e uma rodada da fila (`--once`): cria os jobs `pending`, processa e marca `done` ou `error` | não implementada |
| 3 | fila contínua, como o poller, com retomada e jobs presos devolvidos | não implementada |
| 4 | Airflow: run e tentativas | não implementada |
| 5 | Airflow: eventos `lh_*` e exceção de cada tentativa | não implementada |
| 6 | Airflow: saída do driver Spark (traceback recortado) | não implementada |
| 7 | Data Quality: checks reprovados, com `expected_value` e `diagnostic` | não implementada |
| 8 | Data Quality: checks relacionados (mesma run, mesma coluna) | não implementada |
| 9 | domínio: `pipeline.yaml`, definição da task, dono e estado do repositório | não implementada |
| 10 | domínio: arquivo citado, linha e trecho, com a falha reaplicada | não implementada |
| 11 | limites e regras do pacote, `inspect` e avaliação final contra os `notes.md` | não implementada |

Esperado, em resumo:

- **Etapas 1 a 3.** Os 3 incidentes `failed` do registro (`ri-001`,
  `ri-002`, `ri-003`) ganham um job `done` e um `package.json`. A run
  `scheduled__2026-10-18`, já `recovered`, não ganha job. Rodar de novo não
  cria jobs nem pacotes novos.
- **Etapas 4 a 11.** O conteúdo de cada seção do pacote nos 3 casos, como na
  tabela de esperado de cada etapa do plano parado.

## Andamento

| Data | Evento |
| --- | --- |
| 2026-10-21 | documento criado a partir do plano da branch `standby/incident-collector`; nada implementado |
| 2026-10-21 | renumerada de fase 3 para fase 4; o poller em container entrou antes; gravar o pacote no banco ficou para uma fase posterior |

## Fora desta fase

- Diagnóstico com modelo (`lakehouse-ai-ops`).
- Pacote v2 e v3 (sensores, histórico de mudanças).
- Gravar o pacote no banco para auditoria e validação, numa fase posterior,
  decisão sua em 2026-10-21. Desenho discutido: tabela `collection.package`,
  só com inserções (recoletar cria um pacote novo e mantém o anterior), com a
  chave, o `cycle`, `package_version`, o pacote em `jsonb`, `size_chars` e
  `created_at_dt`/`created_at_ds`; o `collection.job` aponta para o pacote
  gerado. O agent continua sem credenciais do ambiente: lê o `package.json`
  exportado e, depois, um endpoint da API.
- Rodar o poller como serviço: é a [fase 3](phase-3-poller-container.md).
- Apagar a branch `standby/incident-collector`, depois que a etapa 11 for
  concluída.
