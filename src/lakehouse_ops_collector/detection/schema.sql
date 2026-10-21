-- Registro de incidentes da detecção, no banco ops do postgres-ops.
-- Aplicado por `python -m lakehouse_ops_collector.detection.registry init`.
-- Só cria o que não existe: rodar de novo não altera nada.

CREATE SCHEMA IF NOT EXISTS detection;

CREATE TABLE IF NOT EXISTS detection.incident (
    dag_id        text        NOT NULL,
    run_id        text        NOT NULL,
    task_id       text        NOT NULL,
    map_index     integer     NOT NULL DEFAULT -1,
    status        text        NOT NULL CHECK (status IN ('failed', 'running', 'recovered')),
    -- As tentativas no mesmo formato da API (lista `attempts`).
    attempts      jsonb       NOT NULL,
    -- end_date da última tentativa; base do `since` do poller. Vazio enquanto
    -- a tentativa atual não terminou.
    last_end_date timestamptz,
    -- Quando e por quem o registro foi criado e alterado por último. `_ds` é o
    -- modo que gravou (hoje só `poller`); outros modos poderão alimentar a tabela.
    created_at_dt timestamptz NOT NULL DEFAULT now(),
    created_at_ds text        NOT NULL,
    updated_at_dt timestamptz NOT NULL DEFAULT now(),
    updated_at_ds text        NOT NULL,
    -- A chave do incidente: um incidente por task, nunca duplicado.
    PRIMARY KEY (dag_id, run_id, task_id, map_index)
);
