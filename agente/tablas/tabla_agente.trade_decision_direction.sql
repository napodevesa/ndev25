-- ============================================================
--  TABLA: agente.trade_decision_direction
--  Una fila por ticker por snapshot_date.
--  Motor determinístico de decisiones de trading.
-- ============================================================

CREATE TABLE IF NOT EXISTS agente.trade_decision_direction (

    ticker                  VARCHAR(10)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- ── Clasificación estructural
    contexto                VARCHAR(30),
    -- structural_quality | solid_but_expensive | improving
    -- structural_risk    | structural_neutral

    exposicion_buscada      VARCHAR(20),
    -- long_core | income_core | long_tactical | none

    naturaleza_trade        VARCHAR(20),
    -- thesis | tactical | no_trade

    tipo_expresion          VARCHAR(30),
    -- direccional_fundamental

    -- ── Dirección y timing
    direccion               VARCHAR(20),
    -- alcista | neutral_bajista | neutral | none

    instrumento             VARCHAR(30),
    -- stock | cash_secured_put | none

    flag_timing             VARCHAR(30),
    -- tecnico_confirmado | pullback_comprable
    -- esperar_pullback   | macro_defensivo | fundamental_only

    -- ── Sizing
    target_position_size    NUMERIC(4,2),   -- 0.00 a 1.00

    -- ── Contexto de la decisión
    riesgo_principal        VARCHAR(100),
    notas_pre_trade         TEXT,

    -- ── Estado
    trade_status            VARCHAR(20),
    -- active | no_trade

    agent_version           VARCHAR(10),

    -- ── Metadata
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

-- Índices para queries frecuentes del agente
CREATE INDEX IF NOT EXISTS idx_tdirection_date
    ON agente.trade_decision_direction (snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_tdirection_status
    ON agente.trade_decision_direction (snapshot_date, trade_status);

CREATE INDEX IF NOT EXISTS idx_tdirection_instrumento
    ON agente.trade_decision_direction (snapshot_date, instrumento);

CREATE INDEX IF NOT EXISTS idx_tdirection_contexto
    ON agente.trade_decision_direction (snapshot_date, contexto);