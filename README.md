# lakehouse-ops-collector

> **Suspenso em 2026-10-21.** A integração de AI foi adiada até ser
> repriorizada. Nada deste repositório roda no ambiente: os containers foram
> removidos e a stack `ops` saiu da `main` do `lakehouse-infra` (está na
> branch `standby/ai-integration` de lá). Mapa do que foi feito e como
> retomar: `docs/suspension.md` do `lakehouse-ai-ops`.

Lê, em modo somente leitura, as informações de operação da plataforma
lakehouse. Hoje tem a API de detecção, que encontra no Airflow as tasks com
falha e devolve a chave de cada incidente, e o registro de incidentes, o único
lugar em que o repositório grava
([ADR 0001](docs/decisions/0001-write-only-to-the-incident-registry.md)). Depois, o coletor do incidente e o
histórico de mudanças. Não tem AI: o diagnóstico fica no `lakehouse-ai-ops`,
que só lê o que este repositório produz.

Status: fase 1 (API de detecção) concluída
([docs/phases/phase-1-incident-detection-api.md](docs/phases/phase-1-incident-detection-api.md)).
Fase 2 (poller e registro de incidentes) concluída
([docs/phases/phase-2-poller-and-registry.md](docs/phases/phase-2-poller-and-registry.md)).
Fase 3 (poller e API em container) implementada, sem teste manual, suspensa
([docs/phases/phase-3-poller-container.md](docs/phases/phase-3-poller-container.md)).
Depois, fase 4, o coletor do incidente
([docs/phases/phase-4-incident-collector.md](docs/phases/phase-4-incident-collector.md));
até lá, o coletor está parado na branch `standby/incident-collector`.

## Posição no workspace

```text
lakehouse-infra               Airflow, platform_metadata, Hive Metastore, Marquez
lakehouse-domains/            pipeline.yaml, checks e jobs
        │
        │  só leitura
        ▼
lakehouse-ops-collector       detecção (fase 1); depois, pacote do incidente e histórico
        │
        │  chave do incidente; depois, o pacote
        ▼
lakehouse-ai-ops              diagnóstico com modelo local e avaliação
```

## Estrutura

| Caminho | Conteúdo |
| --- | --- |
| `src/lakehouse_ops_collector/incident.py` | chave do incidente (Pydantic): o que a detecção devolve e o coletor recebe |
| `src/lakehouse_ops_collector/detection/` | API de detecção: endpoints, casos de uso, port e adapter do Airflow |
| `src/lakehouse_ops_collector/detection/registry.py`, `schema.sql`, `adapters/postgres.py` | registro de incidentes: comando `init`, schema e adapter PostgreSQL |
| `src/lakehouse_ops_collector/detection/poller.py` | poller: lê os incidentes do Airflow e grava no registro |
| `.env.example` | variáveis de ambiente da API e do registro |
| `docs/phases/` | documento de cada fase |
| `docs/decisions/` | ADRs do repositório |
| `tests/` | testes do pacote (sem acessar o ambiente) e dos links da documentação |

## Instalação

Python 3.12:

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env   # ajuste AIRFLOW_USERNAME, AIRFLOW_PASSWORD e OPS_DB_PASSWORD
```

A API e o comando do registro leem o `.env` da pasta onde são iniciados;
variáveis de ambiente têm prioridade sobre ele. O registro usa o banco `ops`
do `postgres-ops` (stack `ops` do `lakehouse-infra`, em `127.0.0.1:5435`);
`OPS_DB_PASSWORD` não tem padrão e vem do `OPS_DB_PASSWORD` do `docker/.env` de
lá (sem a variável, o padrão de lá é `ops`).

## Uso

```bash
.venv/bin/python -m lakehouse_ops_collector.detection
```

A API sobe em `http://127.0.0.1:8090`:

```bash
curl -s http://127.0.0.1:8090/health
curl -s http://127.0.0.1:8090/incidents
curl -s "http://127.0.0.1:8090/incidents?since=2026-10-18T21:00:00Z&dag_id=commercial_daily_sales_by_region"
```

A documentação interativa fica em `http://127.0.0.1:8090/docs`. Endpoints,
parâmetros e respostas estão no
[documento da fase 1](docs/phases/phase-1-incident-detection-api.md#endpoints).

Criar o schema do registro (rodar de novo não altera nada) e rodar o poller,
que grava no registro os incidentes do Airflow a cada 30 s
(`POLLER_INTERVAL_SECONDS`) até o `Ctrl+C`, ou uma vez só com `--once`:

```bash
.venv/bin/python -m lakehouse_ops_collector.detection.registry init
.venv/bin/python -m lakehouse_ops_collector.detection.poller
.venv/bin/python -m lakehouse_ops_collector.detection.poller --once
```

### Em container

O poller e a API rodam como serviços da stack `ops` do `lakehouse-infra`, a
partir do `Dockerfile` deste repositório. Com a suspensão, a stack só existe na
branch `standby/ai-integration` de lá. Nela:

```bash
./scripts/start-all.sh up ops             # constrói a imagem e sobe postgres-ops, poller e detection-api
docker logs -f lakehouse-ops-poller-1     # uma linha por rodada
```

A configuração vem do `docker/.env` do `lakehouse-infra`, não do `.env` deste
repositório. Com o container no ar, não rode o poller à mão em laço; o
`--once` continua valendo para testes.

## Validação

```bash
.venv/bin/pytest -q
```
