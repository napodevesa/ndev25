CREATE TABLE IF NOT EXISTS agente.top_seleccion (
    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- Clasificación
    sector                  TEXT,
    industry                TEXT,
    market_cap_tier         TEXT,
    contexto                VARCHAR(30),
    instrumento             VARCHAR(30),
    flag_timing             VARCHAR(30),

    -- Scores base
    quality_percentile      NUMERIC(6,2),
    value_percentile        NUMERIC(6,2),
    piotroski_score         SMALLINT,
    altman_z_score          NUMERIC(8,4),

    -- Técnicos clave
    rsi_14_semanal          NUMERIC(6,2),
    precio_vs_ma200         NUMERIC(8,4),
    volume_ratio_20d        NUMERIC(8,4),

    -- Regresiones
    roic_signo              SMALLINT,
    roic_confiable          BOOLEAN,
    net_debt_ebitda_signo   SMALLINT,

    -- Alineación sectorial
    sector_alineado         VARCHAR(20),    -- ALIGNED | NEUTRAL

    -- Score final
    score_conviccion        NUMERIC(6,2),   -- 0-100
    rank_conviccion         SMALLINT,       -- 1 = mejor

    -- Sizing
    target_position_size    NUMERIC(4,2),

    -- Metadata
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_topsel_date
    ON agente.top_seleccion (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_topsel_score
    ON agente.top_seleccion (snapshot_date, score_conviccion DESC);