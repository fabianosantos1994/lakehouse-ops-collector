# AGENTS.md

Lê, em modo somente leitura, as informações de operação da plataforma lakehouse.
A fase 1 é a API de detecção, que encontra no Airflow as tasks com falha
(`docs/phases/phase-1-incident-detection-api.md`). A fase 2, concluída,
criou o poller e o registro de incidentes
(`docs/phases/phase-2-poller-and-registry.md`). O coletor do incidente
está parado na branch `standby/incident-collector` e não deve ser trazido
para a `main` sem pedido.

## Mapa do repositório

| Caminho | Conteúdo |
| --- | --- |
| `src/lakehouse_ops_collector/incident.py` | chave do incidente (contrato com o coletor e o `lakehouse-ai-ops`) |
| `src/lakehouse_ops_collector/detection/` | API de detecção e registro de incidentes |
| `src/lakehouse_ops_collector/detection/schema.sql` | schema do registro (`detection.incident`) |
| `src/lakehouse_ops_collector/detection/poller.py` | poller: rodada que lê o Airflow e grava no registro |
| `docs/phases/` | documento de cada fase |
| `docs/decisions/` | ADRs do repositório e índice |
| `tests/` | testes do repositório |

## Leitura sob demanda

| Necessidade | Fonte |
| --- | --- |
| endpoints, decisões e teste manual da detecção | `docs/phases/phase-1-incident-detection-api.md` (seção relevante) |
| plano do coletor do incidente (parado) | `docs/phases/phase-1-manual-collector.md` na branch `standby/incident-collector` |
| roteiro geral, incidentes de referência e pontos de melhoria (`MP-NN`) | `docs/proposals/ai-troubleshooting-hands-on.md`, `evaluation/reference-incidents/` e `docs/improvements.md` do `lakehouse-ai-ops` |
| eventos `lh_*`, categorias de erro e evidências | ADR 0012 do `lakehouse-platform-core-mvp` |
| serviços, portas e Connections do ambiente | `docs/architecture.md` do `lakehouse-infra` |

## Restrições

- Somente leitura, com uma exceção: o registro de incidentes grava só no
  banco `ops` do `postgres-ops`, e só por `detection/adapters/postgres.py`
  (ADR 0001). Nada aqui altera Airflow, os outros bancos, arquivos dos domínios
  ou o estado das DAGs.
- O schema do registro só cria o que falta (`IF NOT EXISTS`); nada de `DROP`,
  `TRUNCATE` ou alteração de tabela existente sem migração explícita e pedido.
- Durante o desenvolvimento da fase 2, o acesso ao `ops` usa o usuário dono do
  banco. Ao fim do desenvolvimento, ele é trocado por um usuário restrito ao
  schema `detection` (ADR 0001).
- Sem AI: este repositório não chama modelos. Ele extrai; quem interpreta é o
  `lakehouse-ai-ops`.
- Pacotes de incidente nunca contêm linhas de dados, valores de parâmetros,
  credenciais ou variáveis de ambiente com segredo. Cada seção do pacote tem
  limite de tamanho.
- Credenciais só por variáveis de ambiente (`.env` não versionado).
- O formato do pacote é o contrato com o `lakehouse-ai-ops`: mudar ou remover
  um campo exige subir `package_version`; acrescentar campo não exige.
- Este repositório não altera os outros. Mudanças necessárias nos logs do core
  ou na infra são feitas nos repositórios deles e registradas como ponto de
  melhoria no `lakehouse-ai-ops`.
- Os subpacotes (`detection` hoje; `collection` e `history` depois) importam
  só os modelos compartilhados (`incident.py`), nunca uns aos outros.
- A API de detecção escuta só em `127.0.0.1` e não tem autenticação própria.
- Não versione `.env` nem pacotes coletados (`incidents/`).

## Convenções

- Idioma: documentação, `AGENTS.md`, comentários e docstrings em pt-BR;
  código, identificadores e nomes de arquivos em inglês; mensagens de commit
  em inglês. Não traduza nomes de tecnologias, comandos ou identificadores.
- Dependências novas usam versão fixada.
- Ao citar um ADR de outro repositório, inclua o nome dele (ex.: "ADR 0012 do
  `lakehouse-platform-core-mvp`"); ADRs deste repositório são citados só pelo
  número. Pontos de melhoria são citados pelo ID (`MP-NN`).

## Validação

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest -q
```

- Toda mudança tem teste em `tests/`, com a fonte e o banco simulados; os testes não
  acessam o ambiente.
- Mudança que afete o que a API devolve: rode também o teste manual do
  documento da fase e registre o resultado em "Andamento".

Informe o resultado de cada validação. Se algo não pôde ser executado, diga
isso explicitamente.

## Manutenção da documentação

| Mudança | Atualize |
| --- | --- |
| setup, estrutura do repositório ou comandos | `README.md` |
| endpoints, respostas, fontes ou formato do pacote | o documento da fase em `docs/phases/` |
| decisão estrutural tomada ou revertida | novo ADR em `docs/decisions/` e índice |
| restrições, validação ou organização | este arquivo |

- Documente apenas o que está implementado e verificado; o que for planejado
  fica marcado como planejado.
- Não duplique conteúdo entre documentos; use links.
- Mantenha `tests/test_documentation.py` passando.
