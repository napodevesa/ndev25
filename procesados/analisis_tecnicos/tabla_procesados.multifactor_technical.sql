CREATE TABLE IF NOT EXISTS procesados.multifactor_technical (
    ticker              VARCHAR(10)     NOT NULL,
    fecha_de_consulta   DATE            NOT NULL,

    -- RSI
    rsi_14_diario       NUMERIC(6,2),
    rsi_14_semanal      NUMERIC(6,2),

    -- Tendencia de precio
    precio_vs_ma200     NUMERIC(8,4),   -- % sobre/bajo MA200
    dist_max_52w        NUMERIC(8,4),   -- % desde máximo 52W

    -- Volatilidad realizada anualizada
    vol_realizada_30d   NUMERIC(8,4),
    vol_realizada_90d   NUMERIC(8,4),

    -- Volumen
    volume_ratio_20d    NUMERIC(8,4),   -- vol actual / media 20d
    obv_slope           NUMERIC(16,6),  -- pendiente OBV normalizada
    volume_trend_20d    NUMERIC(8,4),   -- pendiente volumen normalizada

    -- Metadata
    run_id              VARCHAR(40),
    updated_at          TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, fecha_de_consulta)
);

CREATE INDEX IF NOT EXISTS idx_mtech_ticker_fecha
    ON procesados.multifactor_technical (ticker, fecha_de_consulta DESC);