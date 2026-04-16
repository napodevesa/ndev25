CREATE TABLE IF NOT EXISTS multifactor.multifactor_sector_industry (
    snapshot_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    sector TEXT NOT NULL,
    industry TEXT,
    market_cap_tier TEXT NOT NULL,

    -- Percentiles relativos a Sector/Industria (del modelo all_usa)
    quality_percentile DOUBLE PRECISION,
    value_percentile DOUBLE PRECISION,

    -- Pesos
    w_quality DOUBLE PRECISION,
    w_value DOUBLE PRECISION,
    num_factors_valid INTEGER,

    -- Resultados finales sectoriales
    multifactor_score_adjusted DOUBLE PRECISION,
    multifactor_rank INTEGER,
    multifactor_percentile DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT now(),

    PRIMARY KEY (snapshot_date, ticker)
);
