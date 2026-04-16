CREATE TABLE IF NOT EXISTS sector.sector_raw (

    id              SERIAL          PRIMARY KEY,
    ticker          VARCHAR(10)     NOT NULL
                        REFERENCES sector.sector_etfs(ticker),
    fecha           DATE            NOT NULL,

    -- Precios del día
    open            NUMERIC(12,4),
    high            NUMERIC(12,4),
    low             NUMERIC(12,4),
    close           NUMERIC(12,4),
    volume          BIGINT,
    vwap            NUMERIC(12,4),

    -- Metadata de la corrida
    descargado_en   TIMESTAMPTZ     DEFAULT NOW(),
    run_id          VARCHAR(40),

    UNIQUE (ticker, fecha)          -- no duplicar mismo día
);

CREATE INDEX IF NOT EXISTS idx_sraw_ticker_fecha
    ON sector.sector_raw (ticker, fecha DESC);
CREATE INDEX IF NOT EXISTS idx_sraw_run_id
    ON sector.sector_raw (run_id);