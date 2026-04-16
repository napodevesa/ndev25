CREATE TABLE IF NOT EXISTS procesados.value_snapshot (
    snapshot_date           DATE             NOT NULL,
    ticker                  TEXT             NOT NULL,
    companyname             TEXT,
    sector                  TEXT,
    industry                TEXT,

    -- Trazabilidad de origen
    fecha_ratios_ttm        DATE             NOT NULL,
    fecha_keymetrics_ttm    DATE             NOT NULL,

    -- Infraestructura
    marketcapttm            DOUBLE PRECISION,
    market_cap_tier         TEXT,

    -- Value ratios (TTM)
	freecashflowyieldttm        DOUBLE PRECISION, --viene de keymetrics
    priceearningsratiottm        DOUBLE PRECISION, -- viene de ratios
    priceearningstogrowthratiottm DOUBLE PRECISION, -- viene de ratios

  
    created_at TIMESTAMP DEFAULT now(),

    PRIMARY KEY (snapshot_date, ticker)
);
