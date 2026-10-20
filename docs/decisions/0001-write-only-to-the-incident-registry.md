# 0001. Escrita só no banco do registro de incidentes

- Status: Aceita
- Data: 2026-10-20

## Contexto

Até a fase 1, o repositório era somente leitura. A fase 2 cria o registro de
incidentes, uma tabela PostgreSQL que o poller atualiza a cada rodada. O banco
é o `ops` do serviço `postgres-ops`, na stack `ops` do `lakehouse-infra`
(ADR 0013 do `lakehouse-infra`).

## Decisão

- O repositório grava em um único lugar: o banco `ops` do `postgres-ops`.
  Airflow, os outros bancos, os arquivos dos domínios e o estado das DAGs
  continuam somente leitura.
- O único código que grava fica em `detection/adapters/postgres.py`.
- O schema (`detection/schema.sql`) é deste repositório e é aplicado pelo
  comando `python -m lakehouse_ops_collector.detection.registry init`. Ele só
  cria o que falta (`IF NOT EXISTS`) e não apaga nem altera dados.
- Durante o desenvolvimento da fase 2, o código usa o usuário `ops`, criado
  pela imagem do PostgreSQL como dono do banco, com todos os privilégios.

## Consequências

- O poller pode registrar e atualizar incidentes sem tocar no Airflow nem no
  `platform_metadata`.
- Pendente para o fim do desenvolvimento: trocar o usuário `ops` por um
  usuário restrito ao schema `detection`, com permissão só para ler e gravar
  a tabela `incident`.
- Mudanças de schema em um banco que já tem a tabela exigem migração
  explícita; o `init` não altera tabelas existentes.
