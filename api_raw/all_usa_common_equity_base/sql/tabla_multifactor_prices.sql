-- ============================================================
--  TABLA: api_raw.multifactor_prices
--  Histórico diario de precios OHLCV ajustados por dividendos
--  para las ~3.000 empresas del universo.
--
--  Patrón idéntico a sector_raw:
--  - Primera corrida: 2 años de historia completa
--  - Corridas siguientes: solo el delta (último día nuevo)
--  - ON CONFLICT DO NOTHING: nunca duplica
--
--  Fuente: FMP /stable/historical-price-eod/dividend-adjusted
-- ============================================================

CREATE TABLE IF NOT EXISTS api_raw.multifactor_prices (

    id              SERIAL          PRIMARY KEY,
    ticker          VARCHAR(10)     NOT NULL,
    fecha           DATE            NOT NULL,

    -- Precios ajustados por dividendos y splits
    open_adj        NUMERIC(12,4),
    high_adj        NUMERIC(12,4),
    low_adj         NUMERIC(12,4),
    close_adj       NUMERIC(12,4)   NOT NULL,
    volume          BIGINT,

    -- Metadata
    descargado_en   TIMESTAMPTZ     DEFAULT NOW(),
    run_id          VARCHAR(40),

    UNIQUE (ticker, fecha)
);

-- Índice principal: "dame el histórico de este ticker ordenado por fecha"
CREATE INDEX IF NOT EXISTS idx_mprices_ticker_fecha
    ON api_raw.multifactor_prices (ticker, fecha DESC);

-- Para auditoría por corrida
CREATE INDEX IF NOT EXISTS idx_mprices_run_id
    ON api_raw.multifactor_prices (run_id);