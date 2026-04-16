-- ============================================================
-- DROP y recrear modelados.value_percentile_all_usa
-- Percentile Value con 3 métricas
-- ============================================================

--DROP TABLE IF EXISTS modelados.value_percentile_all_usa;

CREATE TABLE modelados.value_percentile_all_usa (
    snapshot_date           DATE             NOT NULL,
    ticker                  TEXT             NOT NULL,
    companyname             TEXT,
    sector                  TEXT             NOT NULL,
    industry                TEXT,
    market_cap_tier         TEXT             NOT NULL,
    benchmark_level_used    TEXT,
    benchmark_n_empresas    INTEGER,

    -- Valores ajustados (winsorized)
    pe_adj                  DOUBLE PRECISION,  -- priceearningsratiottm
    peg_adj                 DOUBLE PRECISION,  -- priceearningstogrowthratiottm
    fcfyield_adj            DOUBLE PRECISION,  -- freecashflowyieldttm

    -- Percentiles por benchmark
    pe_percentile           DOUBLE PRECISION,
    peg_percentile          DOUBLE PRECISION,
    fcfyield_percentile     DOUBLE PRECISION,

    -- Score final
    num_metrics_valid       INTEGER,
    value_score             DOUBLE PRECISION,
    value_rank              INTEGER,
    value_percentile        DOUBLE PRECISION,

    created_at              TIMESTAMP        DEFAULT now(),

    PRIMARY KEY (snapshot_date, ticker)
);
