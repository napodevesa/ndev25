-- ============================================================
--  TABLA 1: seleccion.scores
--  Score Quality + Value para las 3.000 empresas
--  Una fila por ticker por snapshot_date
-- ============================================================
CREATE TABLE IF NOT EXISTS seleccion.scores (

    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- Clasificación
    sector                  TEXT,
    industry                TEXT,
    market_cap_tier         TEXT,           -- large | mid | small | micro

    -- ── QUALITY (60%)
    -- Inputs normalizados (percentil 0-100 dentro del universo)
    p_roic                  NUMERIC(6,2),   -- returnOnInvestedCapital
    p_operating_margin      NUMERIC(6,2),   -- operatingProfitMarginTTM
    p_fcf_quality           NUMERIC(6,2),   -- freeCashFlowOperatingCashFlowRatio
    p_interest_coverage     NUMERIC(6,2),   -- interestCoverageRatioTTM
    p_income_quality        NUMERIC(6,2),   -- incomeQuality

    -- Score Quality final (0-100)
    quality_score           NUMERIC(6,2),

    -- ── VALUE (40%)
    -- Inputs normalizados (percentil 0-100, invertido — menor ratio = mejor)
    p_price_to_fcf          NUMERIC(6,2),   -- priceToFreeCashFlowRatioTTM
    p_ev_to_ebitda          NUMERIC(6,2),   -- evToEBITDA
    p_price_to_earnings     NUMERIC(6,2),   -- priceToEarningsRatioTTM
    p_price_to_book         NUMERIC(6,2),   -- priceToBookRatioTTM

    -- Score Value final (0-100)
    value_score             NUMERIC(6,2),

    -- ── SCORE FINAL
    -- Quality 60% + Value 40%
    multifactor_score       NUMERIC(6,2),
    multifactor_rank        INTEGER,        -- 1 = mejor del universo
    multifactor_percentile  NUMERIC(6,2),   -- 0-100

    -- ── Filtros absolutos (para referencia)
    roic_value              NUMERIC(10,6),  -- valor real para filtro
    net_debt_to_ebitda      NUMERIC(10,6),
    debt_to_equity          NUMERIC(10,6),
    fcf_per_share           NUMERIC(10,6),

    -- ── Metadata
    run_id                  VARCHAR(40),
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_scores_snapshot
    ON seleccion.scores (snapshot_date, multifactor_rank);
CREATE INDEX IF NOT EXISTS idx_scores_sector
    ON seleccion.scores (snapshot_date, sector, multifactor_rank);


-- ============================================================
--  TABLA 2: seleccion.universo
--  Las ~700 empresas que pasan el filtro absoluto
--  Se deriva de seleccion.scores
-- ============================================================
CREATE TABLE IF NOT EXISTS seleccion.universo (

    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- Del score
    sector                  TEXT,
    industry                TEXT,
    market_cap_tier         TEXT,
    quality_score           NUMERIC(6,2),
    value_score             NUMERIC(6,2),
    multifactor_score       NUMERIC(6,2),
    multifactor_rank        INTEGER,
    multifactor_percentile  NUMERIC(6,2),

    -- Valores reales de los filtros aplicados
    roic_value              NUMERIC(10,6),
    net_debt_to_ebitda      NUMERIC(10,6),
    debt_to_equity          NUMERIC(10,6),
    fcf_per_share           NUMERIC(10,6),

    -- Metadata
    run_id                  VARCHAR(40),
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_universo_snapshot
    ON seleccion.universo (snapshot_date, multifactor_rank);


-- ============================================================
--  TABLA 3: seleccion.enriquecimiento
--  Las ~700 empresas del universo + todo el contexto adicional
--  Esta tabla reemplaza agente.fundamental_snapshot
--  Es el input directo del agente de trading
-- ============================================================
CREATE TABLE IF NOT EXISTS seleccion.enriquecimiento (

    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- Del universo
    sector                  TEXT,
    industry                TEXT,
    market_cap_tier         TEXT,
    quality_score           NUMERIC(6,2),
    value_score             NUMERIC(6,2),
    multifactor_score       NUMERIC(6,2),
    multifactor_rank        INTEGER,
    multifactor_percentile  NUMERIC(6,2),

    -- ── Técnicos (calculados desde ingest.precios)
    rsi_14_diario           NUMERIC(6,2),
    rsi_14_semanal          NUMERIC(6,2),
    precio_vs_ma200         NUMERIC(8,4),
    dist_max_52w            NUMERIC(8,4),
    vol_realizada_30d       NUMERIC(8,4),
    vol_realizada_90d       NUMERIC(8,4),
    volume_ratio_20d        NUMERIC(8,4),
    obv_slope               NUMERIC(16,6),
    volume_trend_20d        NUMERIC(8,4),
    momentum_3m             NUMERIC(8,4),
    momentum_6m             NUMERIC(8,4),
    momentum_12m            NUMERIC(8,4),

    -- ── Altman Z-score
    altman_z_score          NUMERIC(8,4),
    altman_zona             VARCHAR(20),
    -- safe (>2.99) | grey (1.81-2.99) | distress (<1.81)

    -- ── Piotroski F-score
    piotroski_score         SMALLINT,       -- 0-9
    piotroski_categoria     VARCHAR(20),
    -- fuerte (>=7) | neutral (4-6) | debil (<=3)

    -- ── Regresiones anuales
    roic_tendencia          NUMERIC(12,6),  -- pendiente anualizada
    roic_signo              SMALLINT,       -- 1=mejora | -1=deteriora
    roic_r2                 NUMERIC(6,4),
    roic_confiable          BOOLEAN,        -- n>=4 AND r2>=0.60

    deuda_tendencia         NUMERIC(12,6),
    deuda_signo             SMALLINT,       -- -1=mejora | 1=deteriora
    deuda_r2                NUMERIC(6,4),
    deuda_confiable         BOOLEAN,

    -- ── Conexión con capas anteriores
    estado_macro            VARCHAR(20),    -- de macro.macro_diagnostico
    sector_etf              VARCHAR(10),    -- XLK, XLV, etc.
    sector_alineado         VARCHAR(20),    -- ALIGNED | NEUTRAL

    -- ── Metadata
    run_id                  VARCHAR(40),
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_enriq_snapshot
    ON seleccion.enriquecimiento (snapshot_date, multifactor_rank);
CREATE INDEX IF NOT EXISTS idx_enriq_sector
    ON seleccion.enriquecimiento (snapshot_date, sector, multifactor_rank);
CREATE INDEX IF NOT EXISTS idx_enriq_alineado
    ON seleccion.enriquecimiento (snapshot_date, sector_alineado, multifactor_rank);