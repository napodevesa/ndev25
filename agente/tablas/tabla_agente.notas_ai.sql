CREATE TABLE IF NOT EXISTS agente.notas_ai (
    id                  SERIAL          PRIMARY KEY,
    run_id              VARCHAR(40)     NOT NULL,
    snapshot_date       DATE            NOT NULL,
    generado_en         TIMESTAMPTZ     DEFAULT NOW(),
    modelo_ai           VARCHAR(50)     DEFAULT 'claude-sonnet-4-20250514',

    -- Contexto macro y sector al momento de la nota
    estado_macro        VARCHAR(20),
    estado_sector       VARCHAR(100),   -- top_tickers_aligned

    -- Resumen de la corrida
    n_stock             SMALLINT,       -- empresas con señal stock
    n_csp               SMALLINT,       -- empresas con señal cash_secured_put
    n_total             SMALLINT,       -- total señales activas

    -- Nota estructurada de Claude
    resumen             TEXT,
    oportunidades_stock TEXT,
    oportunidades_csp   TEXT,
    alertas             TEXT,
    nota_completa       TEXT,

    -- Score
    score_conviction    SMALLINT,       -- 0-100

    -- Metadata
    tokens_usados       INTEGER,
    prompt_version      VARCHAR(10)     DEFAULT 'v1'
);

CREATE INDEX IF NOT EXISTS idx_notas_ai_snapshot
    ON agente.notas_ai (snapshot_date DESC);