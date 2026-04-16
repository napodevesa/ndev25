-- ============================================================
--  TABLA: seleccion.technical
--  JOIN entre universo_trabajo (~700 empresas) y
--  procesados.multifactor_technical (indicadores técnicos)
--
--  Se regenera en cada corrida — siempre refleja
--  las empresas que pasaron el filtro absoluto HOY
--  con sus indicadores técnicos más recientes.
-- ============================================================

CREATE TABLE IF NOT EXISTS seleccion.technical (
    ticker              VARCHAR(10)     NOT NULL,
    fecha_de_consulta   DATE            NOT NULL,

    -- RSI
    rsi_14_diario       NUMERIC(6,2),
    rsi_14_semanal      NUMERIC(6,2),

    -- Tendencia de precio
    precio_vs_ma200     NUMERIC(8,4),
    dist_max_52w        NUMERIC(8,4),

    -- Volatilidad
    vol_realizada_30d   NUMERIC(8,4),
    vol_realizada_90d   NUMERIC(8,4),

    -- Volumen
    volume_ratio_20d    NUMERIC(8,4),
    obv_slope           NUMERIC(16,6),
    volume_trend_20d    NUMERIC(8,4),

    -- Metadata
    run_id              VARCHAR(40),
    creado_en           TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, fecha_de_consulta)
);

CREATE INDEX IF NOT EXISTS idx_seltech_ticker
    ON seleccion.technical (ticker, fecha_de_consulta DESC);