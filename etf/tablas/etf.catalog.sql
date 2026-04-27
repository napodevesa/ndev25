CREATE SCHEMA etf;

-- Catálogo de ETFs
CREATE TABLE etf.catalog (
    ticker          VARCHAR(10)     PRIMARY KEY,
    nombre          TEXT            NOT NULL,
    categoria       VARCHAR(30)     NOT NULL,
    -- sector_gics | tematico | commodity_metal |
    -- commodity_energia | commodity_agricola |
    -- internacional | renta_fija
    subcategoria    VARCHAR(30),
    region          VARCHAR(20)     DEFAULT 'USA',
    descripcion     TEXT,
    activo          BOOLEAN         DEFAULT TRUE,
    creado_en       TIMESTAMPTZ     DEFAULT NOW()
);

-- Precios históricos
CREATE TABLE etf.precios (
    ticker          VARCHAR(10)     NOT NULL,
    fecha           DATE            NOT NULL,
    close_adj       NUMERIC(12,4)   NOT NULL,
    volume          BIGINT,
    run_id          VARCHAR(40),
    creado_en       TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (ticker, fecha)
);

-- Snapshot semanal con técnicos
CREATE TABLE etf.snapshot (
    ticker          VARCHAR(10)     NOT NULL,
    snapshot_date   DATE            NOT NULL,
    categoria       VARCHAR(30),
    subcategoria    VARCHAR(30),

    -- RS ratio vs SPY
    rs_vs_spy       NUMERIC(10,6),
    rs_percentil    NUMERIC(6,2),

    -- RSI
    rsi_14_diario   NUMERIC(6,2),
    rsi_14_semanal  NUMERIC(6,2),

    -- Retornos
    ret_1m          NUMERIC(8,4),
    ret_3m          NUMERIC(8,4),
    ret_6m          NUMERIC(8,4),
    ret_1y          NUMERIC(8,4),

    -- Volatilidad
    vol_realizada_30d NUMERIC(8,4),
    vol_realizada_90d NUMERIC(8,4),

    -- Volumen
    volume_ratio_20d  NUMERIC(8,4),
    obv_slope         NUMERIC(16,6),

    -- Momentum
    momentum_3m     NUMERIC(8,4),
    momentum_6m     NUMERIC(8,4),

    -- Distancia máximo 52 semanas
    dist_max_52w    NUMERIC(8,4),

    -- Estado
    estado          VARCHAR(20),
    -- LEADING_STRONG | LEADING_WEAK | 
    -- NEUTRAL | LAGGING

    -- Conexión macro
    estado_macro    VARCHAR(20),
    alineacion_macro VARCHAR(20),   -- ALIGNED | NEUTRAL

    -- Metadata
    run_id          VARCHAR(40),
    creado_en       TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

-- Señal por ETF
CREATE TABLE etf.signal (
    ticker          VARCHAR(10)     NOT NULL,
    snapshot_date   DATE            NOT NULL,
    categoria       VARCHAR(30),

    -- Señal
    señal           VARCHAR(20),
    -- COMPRAR | ESPERAR_PULLBACK | NEUTRAL | EVITAR

    -- Score de convicción
    score           NUMERIC(6,2),

    -- Contexto
    estado_macro    VARCHAR(20),
    razon           TEXT,

    -- Metadata
    run_id          VARCHAR(40),
    creado_en       TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

-- Portafolio sugerido por macro
CREATE TABLE etf.portfolio (
    snapshot_date   DATE            NOT NULL,
    estado_macro    VARCHAR(20)     NOT NULL,
    ticker          VARCHAR(10)     NOT NULL,
    nombre          TEXT,
    categoria       VARCHAR(30),
    peso_pct        NUMERIC(5,2),   -- % del portafolio
    razon           TEXT,
    run_id          VARCHAR(40),
    creado_en       TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (snapshot_date, ticker)
);

-- Índices
CREATE INDEX idx_etf_precios_ticker
    ON etf.precios (ticker, fecha DESC);
CREATE INDEX idx_etf_snapshot_date
    ON etf.snapshot (snapshot_date DESC, rs_percentil DESC);
CREATE INDEX idx_etf_signal_date
    ON etf.signal (snapshot_date DESC, score DESC);