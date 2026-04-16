CREATE TABLE IF NOT EXISTS agente.earnings_calendar (
    ticker              VARCHAR(10)     NOT NULL,
    fecha_earnings      DATE            NOT NULL,
    eps_estimado        NUMERIC(10,4),
    eps_real            NUMERIC(10,4),
    sorpresa_pct        NUMERIC(8,4),
    timing              VARCHAR(20),    -- BMO | AMC
    confirmado          BOOLEAN,        -- TRUE=ya reportó
    creado_en           TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (ticker, fecha_earnings)
);

CREATE INDEX IF NOT EXISTS idx_earnings_ticker
    ON agente.earnings_calendar (ticker, fecha_earnings DESC);
CREATE INDEX IF NOT EXISTS idx_earnings_fecha
    ON agente.earnings_calendar (fecha_earnings);