CREATE TABLE IF NOT EXISTS modelados.momentum_zscore_global (
    ticker TEXT NOT NULL,
    market_cap_tier TEXT NOT NULL,
    snapshot_date DATE NOT NULL,

    benchmark_level_used TEXT DEFAULT 'global_mktcap',
    benchmark_n_empresas INTEGER,

    z_momentum_12m_1m_raw DOUBLE PRECISION,
    z_momentum_12m_1m_winsor DOUBLE PRECISION,
    
    num_metrics_valid INTEGER,
    momentum_score_global DOUBLE PRECISION,
    momentum_rank_global INTEGER,
    momentum_percentile_global DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT now(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE TABLE IF NOT EXISTS modelados.quality_zscore_global (
    ticker                  TEXT NOT NULL,
    market_cap_tier         TEXT NOT NULL,
    snapshot_date           DATE NOT NULL,
    benchmark_level_used    TEXT DEFAULT 'global_mktcap',
    benchmark_n_empresas    INTEGER NOT NULL,
 
    -- Z-Scores RAW
    z_roic_raw              DOUBLE PRECISION,
    z_netdebt_ebitda_raw    DOUBLE PRECISION,
    z_operating_margin_raw  DOUBLE PRECISION,
    z_debt_equity_raw       DOUBLE PRECISION,
 
    -- Z-Scores clampeados [-3, 3]
    z_roic                  DOUBLE PRECISION,
    z_netdebt_ebitda        DOUBLE PRECISION,
    z_operating_margin      DOUBLE PRECISION,
    z_debt_equity           DOUBLE PRECISION,
 
    -- Score final
    num_metrics_valid       INTEGER,
    quality_score_global    DOUBLE PRECISION,
    quality_rank_global     INTEGER,
    quality_percentile_global DOUBLE PRECISION,
 
    created_at              TIMESTAMP DEFAULT now(),
 
    PRIMARY KEY (snapshot_date, ticker)
);

CREATE TABLE IF NOT EXISTS modelados.value_percentile_global (
    snapshot_date           DATE NOT NULL,
    ticker                  TEXT NOT NULL,
    market_cap_tier         TEXT NOT NULL,
    benchmark_level_used    TEXT DEFAULT 'global_mktcap',
    benchmark_n_empresas    INTEGER,

    -- Valores ajustados (winsorized)
    pe_adj                  DOUBLE PRECISION,
    peg_adj                 DOUBLE PRECISION,
    fcfyield_adj            DOUBLE PRECISION,

    -- Percentiles vs Todo el Market Cap Tier
    pe_percentile_global       DOUBLE PRECISION,
    peg_percentile_global      DOUBLE PRECISION,
    fcfyield_percentile_global DOUBLE PRECISION,

    -- Score final
    num_metrics_valid       INTEGER,
    value_score_global      DOUBLE PRECISION,
    value_rank_global       INTEGER,
    value_percentile_global DOUBLE PRECISION,

    created_at              TIMESTAMP DEFAULT now(),

    PRIMARY KEY (snapshot_date, ticker)
);