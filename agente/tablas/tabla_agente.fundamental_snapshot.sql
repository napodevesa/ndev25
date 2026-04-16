-- ============================================================
--  TABLA: agente.fundamental_snapshot
--  Una fila por ticker por snapshot_date.
--  Une toda la información de las capas anteriores:
--  multifactor + regresiones + scores + técnicos
--
--  Fundamentals: frecuencia trimestral
--  Técnicos:     frecuencia semanal
-- ============================================================

CREATE TABLE IF NOT EXISTS agente.fundamental_snapshot (

    ticker                      VARCHAR(10)     NOT NULL,
    snapshot_date               DATE            NOT NULL,

    -- ── Multifactor (Quality 60% + Value 40%)
    quality_percentile          NUMERIC(6,2),
    value_percentile            NUMERIC(6,2),

    -- ── Regresiones anuales ROIC
    roic_trend                  NUMERIC(12,6),   -- pendiente anualizada
    roic_signo                  SMALLINT,        -- 1=mejora · -1=deteriora
    roic_confiable              BOOLEAN,         -- n_points>=4 y R²>=0.60

    -- ── Regresiones anuales deuda
    net_debt_ebitda_trend       NUMERIC(12,6),
    net_debt_ebitda_signo       SMALLINT,        -- -1=mejora (baja) · 1=deteriora
    net_debt_ebitda_confiable   BOOLEAN,

    -- ── Scores
    altman_z_score              NUMERIC(8,4),    -- >2.99=seguro · <1.81=riesgo
    piotroski_score             SMALLINT,        -- 0-9 · >=7=fuerte

    -- ── Técnicos (actualizados semanalmente)
    rsi_14_diario               NUMERIC(6,2),
    rsi_14_semanal              NUMERIC(6,2),
    precio_vs_ma200             NUMERIC(8,4),    -- % sobre/bajo MA200
    dist_max_52w                NUMERIC(8,4),    -- % desde máximo anual
    vol_realizada_30d           NUMERIC(8,4),
    vol_realizada_90d           NUMERIC(8,4),
    volume_ratio_20d            NUMERIC(8,4),
    obv_slope                   NUMERIC(16,6),
    volume_trend_20d            NUMERIC(8,4),

    -- ── Metadata
    creado_en                   TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en              TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, snapshot_date)
);

-- Índices útiles para agente_trading
CREATE INDEX IF NOT EXISTS idx_fsnap_date
    ON agente.fundamental_snapshot (snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_fsnap_quality
    ON agente.fundamental_snapshot (snapshot_date, quality_percentile DESC);

CREATE INDEX IF NOT EXISTS idx_fsnap_altman
    ON agente.fundamental_snapshot (snapshot_date, altman_z_score DESC);

CREATE INDEX IF NOT EXISTS idx_fsnap_piotroski
    ON agente.fundamental_snapshot (snapshot_date, piotroski_score DESC);