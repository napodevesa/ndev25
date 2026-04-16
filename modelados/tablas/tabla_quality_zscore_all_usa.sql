CREATE TABLE modelados.quality_zscore_all_usa (
    ticker                  TEXT             NOT NULL,
    sector                  TEXT             NOT NULL,
    industry                TEXT             NOT NULL,
    market_cap_tier         TEXT             NOT NULL,
    snapshot_date           DATE             NOT NULL,
    benchmark_level_used    TEXT             NOT NULL,
    benchmark_n_empresas    INTEGER          NOT NULL,
 
    -- Z-Scores RAW (antes de clamping)
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
    quality_score           DOUBLE PRECISION,
    quality_rank            INTEGER,
    quality_percentile      DOUBLE PRECISION,
 
    created_at              TIMESTAMP        DEFAULT now(),
 
    PRIMARY KEY (snapshot_date, ticker)
);