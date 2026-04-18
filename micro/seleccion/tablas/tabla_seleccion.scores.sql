DROP TABLE IF EXISTS seleccion.scores CASCADE;

CREATE TABLE IF NOT EXISTS seleccion.scores (

    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    sector                  TEXT,
    industry                TEXT,
    market_cap_tier         TEXT,

    -- QUALITY percentiles (0-100)
    p_roic                  NUMERIC(6,2),
    p_operating_margin      NUMERIC(6,2),
    p_fcf_quality           NUMERIC(6,2),
    p_interest_coverage     NUMERIC(6,2),
    p_income_quality        NUMERIC(6,2),
    quality_score           NUMERIC(6,2),

    -- VALUE percentiles (0-100)
    p_price_to_fcf          NUMERIC(6,2),
    p_ev_to_ebitda          NUMERIC(6,2),
    p_price_to_earnings     NUMERIC(6,2),
    p_price_to_book         NUMERIC(6,2),
    value_score             NUMERIC(6,2),

    -- SCORE FINAL
    multifactor_score       NUMERIC(6,2),
    multifactor_rank        INTEGER,
    multifactor_percentile  NUMERIC(6,2),

    -- Valores reales para filtro absoluto
    roic_value              NUMERIC(16,4),
    net_debt_to_ebitda      NUMERIC(16,4),
    debt_to_equity          NUMERIC(16,4),
    fcf_per_share           NUMERIC(16,4),

    run_id                  VARCHAR(40),
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_scores_snapshot
    ON seleccion.scores (snapshot_date, multifactor_rank);
CREATE INDEX IF NOT EXISTS idx_scores_sector
    ON seleccion.scores (snapshot_date, sector, multifactor_rank);