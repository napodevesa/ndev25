-- 1. QUALITY BENCHMARK GLOBAL
CREATE TABLE procesados.quality_benchmark_global_mktcap (
    snapshot_date    DATE NOT NULL,
    market_cap_tier  TEXT NOT NULL,
    n_empresas       INTEGER,
    -- Métricas (Media y Desvío para Z-Score)
    media_roic              DOUBLE PRECISION,
    desvio_roic             DOUBLE PRECISION,
    media_ndebt_ebitda      DOUBLE PRECISION,
    desvio_ndebt_ebitda     DOUBLE PRECISION,
    media_operating_margin  DOUBLE PRECISION,
    desvio_operating_margin DOUBLE PRECISION,
    media_debt_equity       DOUBLE PRECISION,
    desvio_debt_equity      DOUBLE PRECISION,
    created_at              TIMESTAMP DEFAULT now(),
    PRIMARY KEY (snapshot_date, market_cap_tier)
);

-- 2. MOMENTUM BENCHMARK GLOBAL
CREATE TABLE procesados.momentum_benchmark_global_mktcap (
    snapshot_date    DATE NOT NULL,
    market_cap_tier  TEXT NOT NULL,
    n_empresas       INTEGER,
    media_momentum   DOUBLE PRECISION,
    desvio_momentum  DOUBLE PRECISION,
    momentum_p25     DOUBLE PRECISION,
    momentum_p50     DOUBLE PRECISION,
    momentum_p75     DOUBLE PRECISION,
    created_at       TIMESTAMP DEFAULT now(),
    PRIMARY KEY (snapshot_date, market_cap_tier)
);

-- 3. VALUE BENCHMARK GLOBAL
CREATE TABLE procesados.value_benchmark_global_mktcap (
    snapshot_date    DATE NOT NULL,
    market_cap_tier  TEXT NOT NULL,
    n_empresas       INTEGER,
    pe_p25           DOUBLE PRECISION,
    pe_p50           DOUBLE PRECISION,
    pe_p75           DOUBLE PRECISION,
    peg_p25          DOUBLE PRECISION,
    peg_p50          DOUBLE PRECISION,
    peg_p75          DOUBLE PRECISION,
    fcfyield_p25     DOUBLE PRECISION,
    fcfyield_p50     DOUBLE PRECISION,
    fcfyield_p75     DOUBLE PRECISION,
    created_at       TIMESTAMP DEFAULT now(),
    PRIMARY KEY (snapshot_date, market_cap_tier)
);