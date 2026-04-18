-- ============================================================
--  TABLA 1: agente.decision
--  Motor determinístico — señal por empresa
--  Reemplaza agente.trade_decision_direction
-- ============================================================
CREATE TABLE IF NOT EXISTS agente.decision (

    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- Clasificación fundamental
    sector                  TEXT,
    industry                TEXT,
    market_cap_tier         TEXT,
    multifactor_score       NUMERIC(6,2),
    multifactor_rank        INTEGER,

    -- Contexto estructural
    contexto                VARCHAR(30),
    -- structural_quality | solid_but_expensive |
    -- improving | structural_risk | structural_neutral

    -- Timing técnico
    timing                  VARCHAR(30),
    -- good_entry | pullback_in_uptrend |
    -- overbought | below_ma200 | neutral

    -- Macro
    estado_macro            VARCHAR(20),
    macro_factor            NUMERIC(4,2),   -- 0.70 a 1.10
    sector_alineado         VARCHAR(20),    -- ALIGNED | NEUTRAL

    -- Decisión
    exposicion              VARCHAR(20),    -- long_core | income_core | none
    instrumento             VARCHAR(30),    -- stock | cash_secured_put | none
    flag_timing             VARCHAR(30),
    -- tecnico_confirmado | pullback_comprable |
    -- esperar_pullback | macro_defensivo | fundamental_only

    -- Sizing
    target_position_size    NUMERIC(4,2),   -- 0.00 a 1.00

    -- Scores de convicción
    score_conviccion        NUMERIC(6,2),   -- 0-100
    rank_conviccion         SMALLINT,       -- 1 = mayor convicción

    -- Contexto de la decisión
    riesgo_principal        TEXT,
    notas                   TEXT,

    -- Estado
    trade_status            VARCHAR(20),    -- active | no_trade
    agent_version           VARCHAR(10),

    -- Metadata
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_decision_snapshot
    ON agente.decision (snapshot_date, trade_status);
CREATE INDEX IF NOT EXISTS idx_decision_instrumento
    ON agente.decision (snapshot_date, instrumento);
CREATE INDEX IF NOT EXISTS idx_decision_rank
    ON agente.decision (snapshot_date, rank_conviccion);


-- ============================================================
--  TABLA 2: agente.top
--  Top 25 empresas por score de convicción
--  Reemplaza agente.top_seleccion
-- ============================================================
CREATE TABLE IF NOT EXISTS agente.top (

    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- Clasificación
    sector                  TEXT,
    industry                TEXT,
    market_cap_tier         TEXT,

    -- Scores
    quality_score           NUMERIC(6,2),
    value_score             NUMERIC(6,2),
    multifactor_score       NUMERIC(6,2),
    score_conviccion        NUMERIC(6,2),
    rank_conviccion         SMALLINT,

    -- Decisión
    contexto                VARCHAR(30),
    instrumento             VARCHAR(30),
    flag_timing             VARCHAR(30),
    sector_alineado         VARCHAR(20),
    target_position_size    NUMERIC(4,2),

    -- Técnicos clave
    rsi_14_semanal          NUMERIC(6,2),
    precio_vs_ma200         NUMERIC(8,4),
    volume_ratio_20d        NUMERIC(8,4),

    -- Salud
    altman_z_score          NUMERIC(8,4),
    piotroski_score         SMALLINT,
    roic_signo              SMALLINT,
    roic_confiable          BOOLEAN,

    -- Metadata
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_top_snapshot
    ON agente.top (snapshot_date, rank_conviccion);


-- ============================================================
--  TABLA 3: agente.notas_ai
--  Nota Claude por corrida mensual
-- ============================================================
CREATE TABLE IF NOT EXISTS agente.notas_ai (

    id                  SERIAL          PRIMARY KEY,
    run_id              VARCHAR(40)     NOT NULL,
    snapshot_date       DATE            NOT NULL,
    generado_en         TIMESTAMPTZ     DEFAULT NOW(),
    modelo_ai           VARCHAR(50)     DEFAULT 'claude-sonnet-4-20250514',

    -- Contexto al momento de la nota
    estado_macro        VARCHAR(20),
    sector_top          TEXT,           -- top_tickers_aligned
    n_activas           SMALLINT,
    n_stock             SMALLINT,
    n_csp               SMALLINT,

    -- Nota estructurada
    resumen             TEXT,
    oportunidades_stock TEXT,
    oportunidades_csp   TEXT,
    alertas             TEXT,
    nota_completa       TEXT,

    -- Score
    score_conviction    SMALLINT,       -- 0-100

    -- Metadata
    tokens_usados       INTEGER,
    prompt_version      VARCHAR(10)     DEFAULT 'v1'
);

CREATE INDEX IF NOT EXISTS idx_notas_snapshot
    ON agente.notas_ai (snapshot_date DESC);