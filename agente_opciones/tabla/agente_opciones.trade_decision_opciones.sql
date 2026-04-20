DROP TABLE IF EXISTS agente_opciones.trade_decision_opciones CASCADE;

CREATE TABLE IF NOT EXISTS agente_opciones.trade_decision_opciones (

    ticker                  VARCHAR(20)     NOT NULL,
    snapshot_date           DATE            NOT NULL,

    -- ── Dirección y contexto fundamental
    direccion               VARCHAR(20),
    contexto                VARCHAR(30),
    tendencia_fundamental   VARCHAR(30),    -- nuevo v3.0

    -- ── Contexto macro
    estado_macro            VARCHAR(20),
    regimen_vix             VARCHAR(20),
    vix                     NUMERIC(6,2),

    -- ── Volatilidad implícita
    nivel_iv                VARCHAR(10),    -- baja | media | alta
    iv_promedio             NUMERIC(8,4),
    term_structure          VARCHAR(20),    -- backwardation | contango | flat

    -- ── Liquidez
    liquidez                VARCHAR(20),    -- liquido | semi_liquido | iliquido

    -- ── Estrategia
    estrategia              VARCHAR(30),
    -- cash_secured_put | bull_put_spread | iron_condor
    -- jade_lizard | calendar_spread | no_trade
    delta_objetivo          NUMERIC(5,2),

    -- ── Mejor put seleccionado
    put_strike              NUMERIC(10,2),
    put_delta               NUMERIC(8,4),
    put_theta               NUMERIC(8,4),
    put_iv                  NUMERIC(8,4),
    put_dte                 INTEGER,

    -- ── Mejor call seleccionado (para IC y jade lizard)
    call_strike             NUMERIC(10,2),
    call_delta              NUMERIC(8,4),   -- nuevo v3.0
    call_theta              NUMERIC(8,4),   -- nuevo v3.0
    call_iv                 NUMERIC(8,4),   -- nuevo v3.0

    -- ── Sizing
    sizing                  NUMERIC(4,2),

    -- ── Estado y trazabilidad
    trade_status            VARCHAR(20),    -- active | no_trade
    notas                   TEXT,
    agent_version           VARCHAR(10),

    -- ── Metadata
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en          TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_topciones_date
    ON agente_opciones.trade_decision_opciones (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_topciones_estrategia
    ON agente_opciones.trade_decision_opciones (snapshot_date, estrategia);
CREATE INDEX IF NOT EXISTS idx_topciones_status
    ON agente_opciones.trade_decision_opciones (snapshot_date, trade_status);
CREATE INDEX IF NOT EXISTS idx_topciones_macro
    ON agente_opciones.trade_decision_opciones (snapshot_date, estado_macro);