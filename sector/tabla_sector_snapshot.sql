-- ============================================================
--  CAPA SECTOR — TABLA 3: sector_snapshot
--  Una fila por ticker por corrida.
--  Guarda todos los indicadores calculados por el script Python
--  más el ranking y estado determinístico.
--
--  Equivalente a macro_diagnostico pero con una fila por ticker.
-- ============================================================
 
CREATE TABLE IF NOT EXISTS sector.sector_snapshot (
 
    id              SERIAL          PRIMARY KEY,
    run_id          VARCHAR(40)     NOT NULL,
    calculado_en    TIMESTAMPTZ     DEFAULT NOW(),
    fecha_ultimo    DATE,                           -- último cierre usado
 
    -- Identificación del ETF
    ticker          VARCHAR(10)     NOT NULL
                        REFERENCES sector.sector_etfs(ticker),
    tipo            VARCHAR(20),                    -- sector | industria | refugio
    sector_gics     VARCHAR(50),
    sector_etf      VARCHAR(10),                    -- ETF padre (XLK, XLV, etc.)
    industria       VARCHAR(100),
 
    -- ── Precio
    close           NUMERIC(12,4),
 
    -- ── Fuerza relativa vs SPY
    rs_vs_spy       NUMERIC(10,6),                  -- ratio crudo precio/spy
    rsi_rs_diario   NUMERIC(6,2),                   -- RSI del RS en timeframe diario
    rsi_rs_semanal  NUMERIC(6,2),                   -- RSI del RS en timeframe semanal
    slope_rs        NUMERIC(12,6),                  -- pendiente del RS semanal
    rs_percentil    NUMERIC(6,2),                   -- percentil del RS en últimos 52W (0-100)
 
    -- ── Retornos de precio absoluto
    ret_1m          NUMERIC(8,2),                   -- retorno 1 mes  (%)
    ret_3m          NUMERIC(8,2),                   -- retorno 3 meses (%)
    ret_6m          NUMERIC(8,2),                   -- retorno 6 meses (%)
    ret_1a          NUMERIC(8,2),                   -- retorno 1 año  (%)
    dist_max_52w    NUMERIC(8,2),                   -- % desde el máximo 52W (negativo)
 
    -- ── Volumen
    vol_ratio       NUMERIC(8,4),                   -- vol actual / media 20d
    obv_slope       NUMERIC(16,6),                  -- pendiente del OBV
 
    -- ── RSI del precio
    rsi_precio      NUMERIC(6,2),
 
    -- ── Score compuesto (calculado por las vistas SQL)
    score_momentum  NUMERIC(6,2),                   -- 0-100: combina RS + retornos + slope
    score_volumen   NUMERIC(6,2),                   -- 0-100: confirma el momentum con volumen
    score_total     NUMERIC(6,2),                   -- 0-100: score final ponderado
 
    -- ── Estado determinístico (motor de reglas SQL)
    estado          VARCHAR(20),
    -- LEADING_STRONG  → lidera con volumen confirmado
    -- LEADING_WEAK    → lidera pero sin confirmación de volumen
    -- NEUTRAL         → sin dirección clara
    -- LAGGING         → debajo del mercado
 
    -- ── Alineación con régimen macro
    estado_macro        VARCHAR(20),                -- copiado de macro_diagnostico
    alineacion_macro    VARCHAR(20),
    -- ALIGNED     → el sector es favorecido por el régimen macro actual
    -- CONTRARIAN  → lidera pero va contra el régimen
    -- NEUTRAL     → sin señal clara de alineación
 
    -- ── Ranking dentro del universo (calculado por vista)
    rank_total          SMALLINT,                   -- ranking global (1 = mejor)
    rank_en_sector      SMALLINT                    -- ranking dentro de su sector padre
);
 
-- ── Índices ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_snap_run_id
    ON sector.sector_snapshot (run_id);
CREATE INDEX IF NOT EXISTS idx_snap_ticker_run
    ON sector.sector_snapshot (ticker, run_id);
CREATE INDEX IF NOT EXISTS idx_snap_calculado
    ON sector.sector_snapshot (calculado_en DESC);
CREATE INDEX IF NOT EXISTS idx_snap_estado
    ON sector.sector_snapshot (estado, calculado_en DESC);
CREATE INDEX IF NOT EXISTS idx_snap_rank
    ON sector.sector_snapshot (run_id, rank_total);
 
 