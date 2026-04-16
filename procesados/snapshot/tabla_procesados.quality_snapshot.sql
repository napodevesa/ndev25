
-- ============================================================
-- con las métricas de Quality y Value redefinidas
-- ============================================================

CREATE TABLE procesados.quality_snapshot (
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

    -- QUALITY (4 métricas)
    -- desde api_raw.multifactor_keymetrics_ttm
    roicttm                         DOUBLE PRECISION,
    netdebttoebitdattm              DOUBLE PRECISION,
    -- desde api_raw.multifactor_ratios_ttm
    operatingprofitmarginttm        DOUBLE PRECISION,
    debtequityratiottm              DOUBLE PRECISION,

    created_at              TIMESTAMP        DEFAULT now(),

    PRIMARY KEY (snapshot_date, ticker)
);