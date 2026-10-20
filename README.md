# lakehouse-ops-collector

Lê, em modo somente leitura, as informações de operação da plataforma
lakehouse. Hoje tem a API de detecção, que encontra no Airflow as tasks com
falha e devolve a chave de cada incidente. Depois, o coletor do incidente e o
histórico de mudanças. Não tem AI: o diagnóstico fica no `lakehouse-ai-ops`,
que só lê o que este repositório produz.

Status: fase 1 (API de detecção) concluída
([docs/phases/phase-1-incident-detection-api.md](docs/phases/phase-1-incident-detection-api.md)).
O coletor do incidente está parado na branch `standby/incident-collector`.

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
| `.env.example` | variáveis de ambiente da API |
| `docs/phases/` | documento de cada fase |
| `docs/decisions/` | ADRs do repositório |
| `tests/` | testes do pacote (sem acessar o ambiente) e dos links da documentação |

## Instalação

Python 3.12:

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env   # ajuste AIRFLOW_USERNAME e AIRFLOW_PASSWORD
```

A API lê o `.env` da pasta onde é iniciada; variáveis de ambiente têm
prioridade sobre ele.

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

## Validação

```bash
.venv/bin/pytest -q
```
