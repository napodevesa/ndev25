CREATE TABLE IF NOT EXISTS ingest.precios (
    ticker          VARCHAR(10)     NOT NULL,
    fecha           DATE            NOT NULL,
    close_adj       NUMERIC(12,4)   NOT NULL,
    volume          BIGINT,
    run_id          VARCHAR(40),
    creado_en       TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, fecha)
);

CREATE INDEX IF NOT EXISTS idx_precios_ticker
    ON ingest.precios (ticker, fecha DESC);