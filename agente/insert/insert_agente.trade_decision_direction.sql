-- ============================================================
--  agente.trade_decision_direction  |  v4.0
--  Mejoras: técnicos reales + regresiones confiables + macro
-- ============================================================

INSERT INTO agente.trade_decision_direction (
    ticker, snapshot_date, contexto, exposicion_buscada,
    naturaleza_trade, tipo_expresion, direccion, instrumento,
    flag_timing, target_position_size, riesgo_principal,
    notas_pre_trade, trade_status, agent_version
)

WITH

-- ── Estado macro vigente (Capa MACRO)
estado_macro AS (
    SELECT estado_macro, score_riesgo
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC
    LIMIT 1
),

-- ── Base: fundamental_snapshot con estado macro
base_inputs AS (
    SELECT
        fs.ticker,
        fs.snapshot_date,
        fs.quality_percentile,
        fs.value_percentile,
        fs.altman_z_score,
        fs.piotroski_score,

        -- Regresiones con confiabilidad
        fs.roic_signo,
        fs.roic_confiable,
        fs.net_debt_ebitda_signo,
        fs.net_debt_ebitda_confiable,

        -- Técnicos reales (reemplazan momentum_percentile)
        fs.rsi_14_semanal,
        fs.precio_vs_ma200,
        fs.volume_ratio_20d,
        fs.obv_slope,
        fs.vol_realizada_30d,

        -- Macro
        em.estado_macro,
        em.score_riesgo

    FROM agente.fundamental_snapshot fs
    CROSS JOIN estado_macro em
    WHERE fs.snapshot_date = '2026-04-01'
),

-- ── 1. CONTEXTO ESTRUCTURAL
-- Clasifica cada empresa según su calidad fundamental
contexto_empresa AS (
    SELECT *,
        CASE
            -- Riesgo estructural: cualquier señal de alerta grave
            WHEN altman_z_score < 1.81                          THEN 'structural_risk'
            WHEN piotroski_score <= 3                           THEN 'structural_risk'
            WHEN roic_signo = -1 AND roic_confiable             THEN 'structural_risk'

            -- Calidad estructural: todo alineado
            WHEN quality_percentile >= 70
             AND value_percentile   >= 40
             AND altman_z_score     >= 2.99
             AND piotroski_score    >= 6
             AND roic_signo         = 1                         THEN 'structural_quality'

            -- Sólida pero cara: buena empresa, valuación exigente
            WHEN quality_percentile >= 70
             AND value_percentile   < 40
             AND altman_z_score     >= 2.99                     THEN 'solid_but_expensive'

            -- Mejorando: calidad media pero tendencias positivas
            WHEN quality_percentile >= 50
             AND roic_signo         = 1
             AND net_debt_ebitda_signo = -1                     THEN 'improving'

            ELSE 'structural_neutral'
        END AS contexto
    FROM base_inputs
),

-- ── 2. TIMING TÉCNICO
-- Evalúa si el momento técnico acompaña la tesis fundamental
timing_tecnico AS (
    SELECT *,
        CASE
            -- Sobrecomprada — esperar pullback
            WHEN rsi_14_semanal > 75                            THEN 'overbought'
            -- Zona óptima de entrada: tendencia alcista sin sobrecompra
            WHEN rsi_14_semanal BETWEEN 40 AND 65
             AND precio_vs_ma200 > 0
             AND volume_ratio_20d >= 0.8                        THEN 'good_entry'
            -- Pullback en uptrend: RSI bajo pero sobre MA200
            WHEN rsi_14_semanal < 40
             AND precio_vs_ma200 > 0                            THEN 'pullback_in_uptrend'
            -- Bajo la MA200: tendencia bajista
            WHEN precio_vs_ma200 < -5                           THEN 'below_ma200'
            ELSE 'neutral'
        END AS timing
    FROM contexto_empresa
),

-- ── 3. AJUSTE POR RÉGIMEN MACRO
-- El estado macro modifica la agresividad de la posición
macro_ajuste AS (
    SELECT *,
        CASE estado_macro
            WHEN 'EXPANSION'   THEN 1.10   -- más agresivo
            WHEN 'RECOVERY'    THEN 1.05
            WHEN 'SLOWDOWN'    THEN 0.85   -- más conservador
            WHEN 'CONTRACTION' THEN 0.70   -- muy conservador
            ELSE 1.00
        END AS macro_factor,

        -- En CONTRACTION y SLOWDOWN preferimos income sobre directional
        CASE
            WHEN estado_macro IN ('CONTRACTION', 'SLOWDOWN') THEN TRUE
            ELSE FALSE
        END AS preferir_income

    FROM timing_tecnico
),

-- ── 4. INTENCIÓN Y DIRECCIÓN
intencion_trade AS (
    SELECT *,
        CASE contexto
            WHEN 'structural_quality'  THEN 'long_core'
            WHEN 'solid_but_expensive' THEN 'income_core'
            WHEN 'improving'           THEN 'long_tactical'
            ELSE 'none'
        END AS exposicion_buscada,

        CASE contexto
            WHEN 'structural_quality'  THEN 'thesis'
            WHEN 'solid_but_expensive' THEN 'tactical'
            WHEN 'improving'           THEN 'tactical'
            ELSE 'no_trade'
        END AS naturaleza_trade,

        CASE
            WHEN contexto = 'structural_quality'
             AND timing NOT IN ('overbought', 'below_ma200')    THEN 'alcista'
            WHEN contexto = 'solid_but_expensive'               THEN 'neutral_bajista'
            WHEN contexto = 'improving'
             AND timing = 'good_entry'                          THEN 'alcista'
            ELSE 'none'
        END AS direccion

    FROM macro_ajuste
),

-- ── 5. INSTRUMENTACIÓN
-- Decide entre stock, cash_secured_put o ninguno
instrumentacion AS (
    SELECT *,
        CASE
            -- No operar si hay riesgo estructural o timing muy malo
            WHEN contexto = 'structural_risk'                   THEN 'none'
            WHEN timing = 'below_ma200'                         THEN 'none'
            WHEN exposicion_buscada = 'none'                    THEN 'none'

            -- En régimen defensivo → preferir income siempre
            WHEN preferir_income
             AND contexto IN ('structural_quality',
                              'solid_but_expensive')            THEN 'cash_secured_put'

            -- Calidad con buen timing → compra directa
            WHEN exposicion_buscada = 'long_core'
             AND timing IN ('good_entry', 'pullback_in_uptrend')
             AND obv_slope > 0                                  THEN 'stock'

            -- Calidad con timing sobrecomprado o volumen débil → put
            WHEN exposicion_buscada = 'long_core'
             AND timing = 'overbought'                          THEN 'cash_secured_put'

            -- Income o improving → put
            WHEN exposicion_buscada IN ('income_core',
                                        'long_tactical')        THEN 'cash_secured_put'

            ELSE 'none'
        END AS instrumento

    FROM intencion_trade
),

-- ── 6. SIZING DINÁMICO
-- Combina calidad fundamental + timing técnico + ajuste macro
sizing AS (
    SELECT *,
        CASE
            WHEN instrumento = 'none' THEN 0.0
            ELSE ROUND(
                LEAST(1.0,
                    -- Base: calidad y valor
                    ( (quality_percentile / 100.0) * 0.60
                    + (value_percentile   / 100.0) * 0.40 )

                    -- Bonus por Piotroski alto
                    * CASE
                        WHEN piotroski_score >= 7 THEN 1.10
                        WHEN piotroski_score >= 5 THEN 1.00
                        ELSE 0.85
                      END

                    -- Ajuste por timing técnico
                    * CASE timing
                        WHEN 'good_entry'          THEN 1.00
                        WHEN 'pullback_in_uptrend' THEN 0.90
                        WHEN 'neutral'             THEN 0.80
                        WHEN 'overbought'          THEN 0.70
                        ELSE 0.60
                      END

                    -- Ajuste por régimen macro
                    * macro_factor
                )
            , 2)
        END AS target_position_size

    FROM instrumentacion
),

-- ── 7. DECISIÓN FINAL
final_decision AS (
    SELECT
        ticker,
        snapshot_date,
        contexto,
        exposicion_buscada,
        naturaleza_trade,
        'direccional_fundamental' AS tipo_expresion,
        direccion,
        instrumento,

        -- Flag de timing: indica qué señales técnicas están activas
        CASE
            WHEN timing = 'good_entry'
             AND obv_slope > 0          THEN 'tecnico_confirmado'
            WHEN timing = 'pullback_in_uptrend'
                                        THEN 'pullback_comprable'
            WHEN timing = 'overbought'  THEN 'esperar_pullback'
            WHEN preferir_income        THEN 'macro_defensivo'
            ELSE                             'fundamental_only'
        END AS flag_timing,

        target_position_size,

        CASE contexto
            WHEN 'structural_quality'  THEN 'Riesgo mercado general'
            WHEN 'solid_but_expensive' THEN 'Riesgo corrección por valuación'
            WHEN 'improving'           THEN 'Riesgo reversión de tendencia'
            ELSE                            'Riesgo debilidad estructural'
        END AS riesgo_principal,

        -- Notas enriquecidas con todos los indicadores
        'Macro:' || estado_macro
            || ' | Q:' || quality_percentile
            || ' | V:' || value_percentile
            || ' | Altman:' || altman_z_score
            || ' | Piotroski:' || piotroski_score
            || ' | RSI_sem:' || rsi_14_semanal
            || ' | MA200:' || precio_vs_ma200
            || ' | Vol_ratio:' || volume_ratio_20d
            || ' | ROIC_signo:' || roic_signo
            || ' | Timing:' || timing
            AS notas_pre_trade,

        CASE
            WHEN instrumento = 'none' THEN 'no_trade'
            ELSE 'active'
        END AS trade_status,

        'v4.0' AS agent_version

    FROM sizing
    WHERE instrumento != 'none'   -- solo insertar los que tienen decisión
)

SELECT * FROM final_decision

ON CONFLICT (ticker, snapshot_date)
DO UPDATE SET
    contexto             = EXCLUDED.contexto,
    exposicion_buscada   = EXCLUDED.exposicion_buscada,
    direccion            = EXCLUDED.direccion,
    instrumento          = EXCLUDED.instrumento,
    flag_timing          = EXCLUDED.flag_timing,
    target_position_size = EXCLUDED.target_position_size,
    notas_pre_trade      = EXCLUDED.notas_pre_trade,
    trade_status         = EXCLUDED.trade_status,
    agent_version        = EXCLUDED.agent_version;