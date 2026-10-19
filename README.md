# lakehouse-ops-collector

Coleta, em modo somente leitura, as informações de operação da plataforma
lakehouse: o pacote de cada incidente de DAG e, depois, a detecção de
incidentes e o histórico de mudanças. Não tem AI: o diagnóstico fica no
`lakehouse-ai-ops`, que só lê o que este repositório produz.

Status: fase 1 em desenvolvimento. O plano e o andamento estão em
[docs/phases/phase-1-manual-collector.md](docs/phases/phase-1-manual-collector.md#plano-de-execução);
hoje o `collect` só valida a chave do incidente e grava o pacote com ela
(etapa 1).

## Posição no workspace

```text
lakehouse-infra               Airflow, platform_metadata, Hive Metastore, Marquez
lakehouse-domains/            pipeline.yaml, checks e jobs
        │
        │  só leitura
        ▼
lakehouse-ops-collector       detecção, pacote do incidente, histórico (este repositório)
        │
        │  pacote do incidente (JSON versionado por package_version)
        ▼
lakehouse-ai-ops              diagnóstico com modelo local e avaliação
```

## Estrutura

| Caminho | Conteúdo |
| --- | --- |
| `src/lakehouse_ops_collector/package.py` | formato do pacote do incidente (Pydantic), contrato com o `lakehouse-ai-ops` |
| `src/lakehouse_ops_collector/collection/` | coletor do incidente: CLI, caso de uso, ports e adapters |
| `.env.example` | variáveis de ambiente do coletor |
| `docs/phases/` | roteiro de cada fase |
| `docs/decisions/` | ADRs do repositório |
| `tests/` | testes do pacote e dos links da documentação |

Os subpacotes `detection` e `history` são planejados
([arquitetura](docs/phases/phase-1-manual-collector.md#arquitetura)) e ainda
não existem.

## Instalação

Python 3.12:

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

O coletor lê a configuração de variáveis de ambiente (hoje só
`INCIDENTS_DIR`, opcional). Para usar um `.env` a partir do `.env.example`:

```bash
cp .env.example .env
set -a; . ./.env; set +a
```

## Uso

```bash
.venv/bin/python -m lakehouse_ops_collector.collection collect \
  --dag-id commercial_daily_sales_by_region \
  --run-id 'backfill__2026-10-15T00:00:00+00:00' \
  --task-id validate_daily_sales_by_region
```

Imprime o caminho do pacote gravado, por padrão em
`incidents/<dag_id>/<run_id>/<task_id>/package.json` (pasta fora do Git;
`INCIDENTS_DIR` muda a raiz). Uma chave inválida termina com código 2, sem
gravar nada.

## Validação

```bash
.venv/bin/pytest -q
```
