CREATE TABLE IF NOT EXISTS multifactor.multifactor_global_mktcap (
    snapshot_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    market_cap_tier TEXT NOT NULL,

    -- Factores Fundamentales
    quality_percentile_global DOUBLE PRECISION,
    value_percentile_global DOUBLE PRECISION,

    -- Pesos (Suma de w_quality + w_value debe ser 1.0)
    w_quality DOUBLE PRECISION,
    w_value DOUBLE PRECISION,
    num_factors_valid INTEGER,

    -- Resultados finales (Score de 0 a 1)
    multifactor_score_global DOUBLE PRECISION,
    multifactor_rank_global INTEGER,
    multifactor_percentile_global DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT now(),

    PRIMARY KEY (snapshot_date, ticker)
);