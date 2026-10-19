# lakehouse-ops-collector

Coleta, em modo somente leitura, as informações de operação da plataforma
lakehouse: o pacote de cada incidente de DAG e, depois, a detecção de
incidentes e o histórico de mudanças. Não tem AI: o diagnóstico fica no
`lakehouse-ai-ops`, que só lê o que este repositório produz.

Status: fase de descoberta. Ainda não há código; a primeira versão do coletor
está em [docs/phases/phase-1-manual-collector.md](docs/phases/phase-1-manual-collector.md).

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
| `docs/phases/` | roteiro de cada fase |
| `docs/decisions/` | ADRs do repositório |
| `tests/` | testes do repositório (links da documentação) |

O pacote `lakehouse_ops_collector` é planejado
([arquitetura](docs/phases/phase-1-manual-collector.md#arquitetura)) e ainda
não existe.

## Validação

```bash
python3 -m venv .venv
.venv/bin/pip install pytest
.venv/bin/pytest -q
```
