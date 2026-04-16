-- ============================================================
-- DROP y recrear procesados.quality_benchmark_sector_industry_mktcap
-- Benchmark de Quality por Sector + Industry + Market Cap Tier
-- 4 métricas: roic, netdebt_ebitda, operating_margin, debt_equity
-- ============================================================

--DROP TABLE IF EXISTS procesados.quality_benchmark_sector_industry_mktcap;

CREATE TABLE procesados.quality_benchmark_sector_industry_mktcap (
    snapshot_date           DATE             NOT NULL,
    sector                  TEXT             NOT NULL,
    industry                TEXT             NOT NULL,
    market_cap_tier         TEXT             NOT NULL,
    n_empresas              INTEGER,

    -- QUALITY (4 métricas)
    media_roic              DOUBLE PRECISION,
    desvio_roic             DOUBLE PRECISION,
    media_ndebt_ebitda      DOUBLE PRECISION,
    desvio_ndebt_ebitda     DOUBLE PRECISION,
    media_operating_margin  DOUBLE PRECISION,
    desvio_operating_margin DOUBLE PRECISION,
    media_debt_equity       DOUBLE PRECISION,
    desvio_debt_equity      DOUBLE PRECISION,

    created_at              TIMESTAMP        DEFAULT now(),

    PRIMARY KEY (snapshot_date, sector, industry, market_cap_tier)
);