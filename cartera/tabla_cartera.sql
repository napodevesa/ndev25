-- ============================================================
--  SCHEMA: cartera
--  Track record verificable del sistema de inversión.
--  Independiente del sistema de señales.
--  Fuente: IBKR Paper Trading via Flex Query o carga manual.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS cartera;

-- ============================================================
--  TABLA: cartera.trade_results
--  Un registro por trade ejecutado.
--  Vinculado a la señal que lo generó en agente.top_seleccion
-- ============================================================

CREATE TABLE IF NOT EXISTS cartera.trade_results (

    id                  SERIAL          PRIMARY KEY,

    -- ── Identificación del trade
    ticker              VARCHAR(10)     NOT NULL,
    instrumento         VARCHAR(30)     NOT NULL,
    -- stock | cash_secured_put | bull_put_spread | 
    -- iron_condor | jade_lizard | calendar_spread

    estrategia          VARCHAR(30),    -- detalle para opciones

    -- ── Entrada
    fecha_entrada       DATE            NOT NULL,
    precio_entrada      NUMERIC(10,4)   NOT NULL,
    cantidad            INTEGER         NOT NULL,
    strike_entrada      NUMERIC(10,2),  -- para opciones
    vto_entrada         DATE,           -- vencimiento para opciones
    dte_entrada         INTEGER,        -- días al vencimiento al entrar

    -- ── Salida
    fecha_salida        DATE,
    precio_salida       NUMERIC(10,4),
    motivo_salida       VARCHAR(30),
    -- vencimiento | stop_loss | take_profit | 
    -- cierre_manual | asignacion

    -- ── Resultado
    pnl_usd             NUMERIC(10,2),
    pnl_pct             NUMERIC(8,4),
    resultado           VARCHAR(10),    -- win | loss | open

    -- ── Contexto de la señal que generó el trade
    snapshot_date       DATE,           -- fecha del snapshot
    score_conviccion    NUMERIC(6,2),
    flag_timing         VARCHAR(30),
    sector_alineado     VARCHAR(20),
    estado_macro        VARCHAR(20),    -- macro al momento de la señal
    sector              TEXT,
    instrumento_señal   VARCHAR(30),    -- lo que sugirió el agente

    -- ── Trazabilidad IBKR
    ibkr_trade_id       VARCHAR(50),    -- ID único de IBKR Flex Query
    fuente              VARCHAR(20)     DEFAULT 'manual',
    -- manual | flex_query

    -- ── Metadata
    creado_en           TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en      TIMESTAMPTZ     DEFAULT NOW()
);

-- ── Índices ────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_tr_ticker
    ON cartera.trade_results (ticker, fecha_entrada DESC);

CREATE INDEX IF NOT EXISTS idx_tr_fecha
    ON cartera.trade_results (fecha_entrada DESC);

CREATE INDEX IF NOT EXISTS idx_tr_resultado
    ON cartera.trade_results (resultado, fecha_entrada DESC);

CREATE INDEX IF NOT EXISTS idx_tr_instrumento
    ON cartera.trade_results (instrumento, fecha_entrada DESC);

CREATE INDEX IF NOT EXISTS idx_tr_snapshot
    ON cartera.trade_results (snapshot_date);


-- ============================================================
--  TABLA: cartera.metricas_resumen
--  Métricas calculadas por período.
--  Se actualiza con sync_ibkr_trades.py
-- ============================================================

CREATE TABLE IF NOT EXISTS cartera.metricas_resumen (

    id                  SERIAL          PRIMARY KEY,
    fecha_calculo       DATE            NOT NULL,
    periodo             VARCHAR(20)     NOT NULL,
    -- semanal | mensual | total

    -- ── Actividad
    n_trades            INTEGER,
    n_wins              INTEGER,
    n_losses            INTEGER,
    n_open              INTEGER,
    win_rate            NUMERIC(6,4),   -- 0.68 = 68%

    -- ── P&L
    pnl_total_usd       NUMERIC(12,2),
    pnl_promedio_usd    NUMERIC(10,2),
    pnl_mejor_trade     NUMERIC(10,2),
    pnl_peor_trade      NUMERIC(10,2),

    -- ── Riesgo
    sharpe              NUMERIC(8,4),
    max_drawdown_pct    NUMERIC(8,4),
    volatilidad_retornos NUMERIC(8,4),

    -- ── Por instrumento
    pnl_stock           NUMERIC(12,2),
    pnl_opciones        NUMERIC(12,2),
    win_rate_stock      NUMERIC(6,4),
    win_rate_opciones   NUMERIC(6,4),

    UNIQUE (fecha_calculo, periodo)
);


-- ============================================================
--  TABLA: cartera.vs_benchmark
--  Comparación diaria vs SPY.
--  Permite graficar equity curve vs benchmark.
-- ============================================================

CREATE TABLE IF NOT EXISTS cartera.vs_benchmark (

    fecha               DATE            PRIMARY KEY,

    -- ── Tu cartera
    equity_usd          NUMERIC(12,2),  -- valor total de la cartera
    retorno_diario_pct  NUMERIC(8,4),
    retorno_acum_pct    NUMERIC(8,4),   -- desde inicio

    -- ── SPY benchmark
    spy_precio          NUMERIC(10,4),
    spy_retorno_diario  NUMERIC(8,4),
    spy_retorno_acum    NUMERIC(8,4),   -- desde inicio del track record

    -- ── Alpha
    alpha_diario        NUMERIC(8,4),   -- retorno_diario - spy_diario
    alpha_acumulado     NUMERIC(8,4),   -- retorno_acum - spy_acum

    creado_en           TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vb_fecha
    ON cartera.vs_benchmark (fecha DESC);