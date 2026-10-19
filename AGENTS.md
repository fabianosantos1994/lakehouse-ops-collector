# AGENTS.md

Coletor de informações de operação da plataforma lakehouse: monta o pacote de
cada incidente de DAG e, nas fases seguintes, detecta incidentes e coleta
histórico. O desenvolvimento segue o plano de execução de
`docs/phases/phase-1-manual-collector.md`, uma etapa por vez.

## Mapa do repositório

| Caminho | Conteúdo |
| --- | --- |
| `src/lakehouse_ops_collector/package.py` | formato do pacote (contrato com o `lakehouse-ai-ops`) |
| `src/lakehouse_ops_collector/collection/` | coletor do incidente |
| `docs/phases/` | roteiro de cada fase |
| `docs/decisions/` | ADRs do repositório e índice |
| `tests/` | testes do repositório |

## Leitura sob demanda

| Necessidade | Fonte |
| --- | --- |
| o que coletar, arquitetura e formato do pacote | `docs/phases/phase-1-manual-collector.md` (seção relevante) |
| roteiro geral, incidentes de referência e pontos de melhoria (`MP-NN`) | `docs/proposals/ai-troubleshooting-hands-on.md`, `evaluation/reference-incidents/` e `docs/improvements.md` do `lakehouse-ai-ops` |
| eventos `lh_*`, categorias de erro e evidências | ADR 0012 do `lakehouse-platform-core-mvp` |
| serviços, portas e Connections do ambiente | `docs/architecture.md` do `lakehouse-infra` |

## Restrições

- Somente leitura: nada aqui altera Airflow, bancos, arquivos dos domínios ou
  o estado das DAGs.
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
- Os subpacotes planejados (`collection`, `detection`, `history`) importam só
  o formato do pacote, nunca uns aos outros.
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

- Toda mudança no coletor tem teste em `tests/`, com adapters falsos; os
  testes não acessam o ambiente.
- Ao concluir uma etapa do plano da fase 1, rode também o teste manual dela
  nos 3 casos e registre o resultado no documento da fase.

Informe o resultado de cada validação. Se algo não pôde ser executado, diga
isso explicitamente.

## Manutenção da documentação

| Mudança | Atualize |
| --- | --- |
| setup, estrutura do repositório ou comandos | `README.md` |
| o que é coletado, fontes ou formato do pacote | o documento da fase em `docs/phases/` |
| decisão estrutural tomada ou revertida | novo ADR em `docs/decisions/` e índice |
| restrições, validação ou organização | este arquivo |

- Documente apenas o que está implementado e verificado; o que for planejado
  fica marcado como planejado.
- Não duplique conteúdo entre documentos; use links.
- Mantenha `tests/test_documentation.py` passando.
