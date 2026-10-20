# Registros de decisão arquitetural (ADRs)

Decisões estruturais do `lakehouse-ops-collector`. As demais decisões de cada
fase estão no documento da fase, em [docs/phases/](../phases/). ADRs de outros repositórios são
citados com o nome do repositório (ex.: "ADR 0012 do
`lakehouse-platform-core-mvp`").

Índice:

| ADR | Decisão | Status |
| --- | --- | --- |
| [0001](0001-write-only-to-the-incident-registry.md) | Escrita só no banco do registro de incidentes | Aceita |

## Como registrar uma decisão

- Crie `NNNN-short-english-slug.md` com o próximo número livre.
- Use as seções: Status, Data, Contexto, Decisão, Consequências.
- Não reescreva um ADR aceito para mudar a decisão. Crie um novo ADR e marque o
  anterior como `Substituída por NNNN`.
- Acrescente a linha no índice acima.
