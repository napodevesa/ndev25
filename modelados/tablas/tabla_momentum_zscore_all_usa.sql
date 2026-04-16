CREATE TABLE IF NOT EXISTS modelados.momentum_zscore_all_usa (
    ticker TEXT NOT NULL,
    sector TEXT NOT NULL,
    industry TEXT NOT NULL,
    market_cap_tier TEXT NOT NULL,

    snapshot_date DATE NOT NULL,

    benchmark_level_used TEXT,
    benchmark_n_empresas INTEGER,

    z_momentum_12m_1m DOUBLE PRECISION,
    num_metrics_valid INTEGER,

    z_momentum_12m_1m_winsor DOUBLE PRECISION,
    momentum_score DOUBLE PRECISION,
    momentum_rank INTEGER,
    momentum_percentile DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT now(),

    PRIMARY KEY (ticker, snapshot_date)
);
