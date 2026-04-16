-- ============================================================
--  agente_opciones.trade_decision_opciones  |  v2.0
--
--  Mejoras sobre v1.1:
--    - Conexión con estado_macro (Capa MACRO)
--    - flag_timing de trade_decision_direction para DTE
--    - delta_objetivo por estrategia
--    - Greeks en output (theta, vega para evaluación de prima)
--    - Mejor selección de contrato: mejor theta/prima por delta
-- ============================================================

INSERT INTO agente_opciones.trade_decision_opciones (
    ticker, snapshot_date, direccion, contexto,
    estado_macro, regimen_vix, vix,
    nivel_iv, iv_promedio, term_structure, liquidez,
    estrategia, delta_objetivo,
    put_strike, put_delta, put_theta, put_iv, put_dte,
    call_strike,
    sizing, trade_status, notas, agent_version
)

WITH

-- ── Estado macro vigente (Capa MACRO)
estado_macro AS (
    SELECT estado_macro, score_riesgo
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC
    LIMIT 1
),

-- ── Base: empresas sin riesgo estructural
-- Incorpora flag_timing para refinar DTE
base AS (
    SELECT
        d.ticker,
        d.snapshot_date,
        CASE WHEN d.direccion = 'none' THEN 'neutral'
             ELSE d.direccion
        END AS direccion,
        d.contexto,
        d.target_position_size AS sizing,
        d.flag_timing,
        em.estado_macro,
        em.score_riesgo
    FROM agente.trade_decision_direction d
    CROSS JOIN estado_macro em
    WHERE d.snapshot_date = '2026-04-01'   -- ← ajustar cada corrida
),

-- ── VIX del día — viene de Capa MACRO
vix_dia AS (
    SELECT
        vix,
        CASE
            WHEN vix <= 15 THEN 'complacencia'
            WHEN vix <= 25 THEN 'normal'
            WHEN vix <= 35 THEN 'elevado'
            ELSE                'panico'
        END AS regimen
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC
    LIMIT 1
),

-- ── IV promedio y nivel por ticker
-- Solo contratos de la última fecha disponible
iv_por_ticker AS (
    SELECT
        ticker,
        ROUND(AVG(iv), 4)                                       AS iv_promedio,
        ROUND(AVG(CASE WHEN contract_type = 'put'  THEN iv END), 4) AS iv_puts,
        CASE
            WHEN AVG(iv) < 0.20                 THEN 'baja'
            WHEN AVG(iv) BETWEEN 0.20 AND 0.40  THEN 'media'
            ELSE                                     'alta'
        END AS nivel_iv
    FROM agente_opciones.contratos_raw
    WHERE fecha = (SELECT MAX(fecha) FROM agente_opciones.contratos_raw)
    GROUP BY ticker
),

-- ── Term structure: IV corto plazo vs largo plazo
term_structure AS (
    SELECT
        ticker,
        ROUND(AVG(CASE WHEN dte <= 35 THEN iv END), 4) AS iv_corto,
        ROUND(AVG(CASE WHEN dte >  35 THEN iv END), 4) AS iv_largo,
        CASE
            WHEN AVG(CASE WHEN dte <= 35 THEN iv END)
               > AVG(CASE WHEN dte >  35 THEN iv END) THEN 'backwardation'
            WHEN AVG(CASE WHEN dte <= 35 THEN iv END)
               < AVG(CASE WHEN dte >  35 THEN iv END) THEN 'contango'
            ELSE 'flat'
        END AS term_structure
    FROM agente_opciones.contratos_raw
    WHERE fecha = (SELECT MAX(fecha) FROM agente_opciones.contratos_raw)
    GROUP BY ticker
),

-- ── Liquidez por ticker
liquidez AS (
    SELECT
        ticker,
        MAX(CASE WHEN contract_type = 'put'  THEN oi END) AS max_oi_put,
        CASE
            WHEN MAX(CASE WHEN contract_type = 'put' THEN oi END) >= 50  THEN 'liquido'
            WHEN MAX(CASE WHEN contract_type = 'put' THEN oi END) >= 10  THEN 'semi_liquido'
            ELSE 'iliquido'
        END AS liquidez
    FROM agente_opciones.contratos_raw
    WHERE fecha = (SELECT MAX(fecha) FROM agente_opciones.contratos_raw)
    GROUP BY ticker
),

-- ── Mejor PUT por ticker
-- Criterio: mayor |theta| (más prima por día) dentro del rango delta 25-35
-- Ya vienen filtrados por delta en la ingesta
mejor_put AS (
    SELECT DISTINCT ON (ticker)
        ticker,
        strike      AS put_strike,
        delta       AS put_delta,
        theta       AS put_theta,
        vega        AS put_vega,
        iv          AS put_iv,
        dte         AS put_dte,
        close_price AS put_precio
    FROM agente_opciones.contratos_raw
    WHERE contract_type = 'put'
      AND fecha = (SELECT MAX(fecha) FROM agente_opciones.contratos_raw)
      AND oi >= 10                          -- liquidez mínima
    ORDER BY ticker, ABS(theta) DESC        -- máxima prima diaria
),

-- ── Mejor CALL por ticker (para iron condor y jade lizard)
mejor_call AS (
    SELECT DISTINCT ON (ticker)
        ticker,
        strike      AS call_strike,
        delta       AS call_delta,
        iv          AS call_iv,
        dte         AS call_dte
    FROM agente_opciones.contratos_raw
    WHERE contract_type = 'call'
      AND fecha = (SELECT MAX(fecha) FROM agente_opciones.contratos_raw)
      AND oi >= 10
    ORDER BY ticker, ABS(theta) DESC
),

-- ── JOIN completo
joined AS (
    SELECT
        b.ticker,
        b.snapshot_date,
        b.direccion,
        b.contexto,
        b.sizing,
        b.flag_timing,
        b.estado_macro,
        b.score_riesgo,
        v.vix,
        v.regimen                           AS regimen_vix,
        iv.nivel_iv,
        iv.iv_promedio,
        ts.term_structure,
        lq.liquidez,
        mp.put_strike,
        mp.put_delta,
        mp.put_theta,
        mp.put_iv,
        mp.put_dte,
        mc.call_strike
    FROM base                b
    CROSS JOIN vix_dia       v
    LEFT JOIN iv_por_ticker  iv ON iv.ticker = b.ticker
    LEFT JOIN term_structure ts ON ts.ticker = b.ticker
    LEFT JOIN liquidez       lq ON lq.ticker = b.ticker
    LEFT JOIN mejor_put      mp ON mp.ticker = b.ticker
    LEFT JOIN mejor_call     mc ON mc.ticker = b.ticker
),

-- ── Selección de estrategia
-- Prioridad: macro → liquidez → IV → direccion + regimen
estrategia AS (
    SELECT *,
        CASE

            -- Filtros eliminatorios
            WHEN liquidez  = 'iliquido'     THEN 'no_trade'
            WHEN nivel_iv  = 'baja'         THEN 'no_trade'
            WHEN put_strike IS NULL         THEN 'no_trade'

            -- CONTRACTION: solo income defensivo, sin calls
            WHEN estado_macro = 'CONTRACTION'
             AND nivel_iv IN ('media','alta')
             AND direccion IN ('alcista','neutral')
                THEN 'cash_secured_put'

            -- SLOWDOWN: income preferido, bull_put_spread si hay convicción
            WHEN estado_macro = 'SLOWDOWN'
             AND direccion = 'neutral'
             AND nivel_iv IN ('media','alta')
                THEN 'iron_condor'

            WHEN estado_macro = 'SLOWDOWN'
             AND direccion = 'alcista'
             AND nivel_iv = 'alta'
                THEN 'bull_put_spread'

            WHEN estado_macro = 'SLOWDOWN'
             AND direccion = 'alcista'
             AND nivel_iv = 'media'
                THEN 'cash_secured_put'

            -- EXPANSION / RECOVERY: más agresivo
            WHEN estado_macro IN ('EXPANSION','RECOVERY')
             AND direccion = 'alcista'
             AND nivel_iv = 'alta'
             AND regimen_vix IN ('elevado','panico')
                THEN 'jade_lizard'

            WHEN estado_macro IN ('EXPANSION','RECOVERY')
             AND direccion = 'alcista'
             AND nivel_iv IN ('media','alta')
                THEN 'bull_put_spread'

            WHEN estado_macro IN ('EXPANSION','RECOVERY')
             AND direccion = 'neutral'
             AND nivel_iv IN ('media','alta')
                THEN 'iron_condor'

            -- Backwardation + neutral → calendar spread
            WHEN term_structure = 'backwardation'
             AND direccion = 'neutral'
             AND nivel_iv IN ('media','alta')
                THEN 'calendar_spread'

            ELSE 'no_trade'
        END AS estrategia

    FROM joined
),

-- ── Delta objetivo por estrategia
delta_objetivo AS (
    SELECT *,
        CASE estrategia
            WHEN 'cash_secured_put' THEN 0.30
            WHEN 'bull_put_spread'  THEN 0.25
            WHEN 'iron_condor'      THEN 0.20
            WHEN 'jade_lizard'      THEN 0.35
            WHEN 'calendar_spread'  THEN 0.25
            ELSE NULL
        END AS delta_objetivo
    FROM estrategia
),

-- ── DTE sugerido según flag_timing
dte_sugerido AS (
    SELECT *,
        CASE
            WHEN estrategia = 'no_trade' THEN NULL
            -- Timing técnico confirmado → DTE corto (capturás el movimiento)
            WHEN flag_timing IN ('tecnico_confirmado',
                                 'pullback_comprable') THEN 30
            -- Sin timing técnico → DTE más largo (dejás trabajar el tiempo)
            WHEN flag_timing = 'fundamental_only'     THEN 45
            -- Régimen defensivo → DTE intermedio
            WHEN flag_timing = 'macro_defensivo'      THEN 38
            ELSE 40
        END AS dte_objetivo
    FROM delta_objetivo
),

-- ── Sizing final ajustado
sizing_final AS (
    SELECT *,
        CASE
            WHEN estrategia = 'no_trade' THEN 0.0
            ELSE ROUND(
                LEAST(1.0,
                    sizing

                    -- Ajuste por régimen VIX (más pánico = más prima = más sizing)
                    * CASE regimen_vix
                        WHEN 'panico'       THEN 1.20
                        WHEN 'elevado'      THEN 1.10
                        WHEN 'normal'       THEN 1.00
                        WHEN 'complacencia' THEN 0.80
                        ELSE                     1.00
                      END

                    -- Penalización por liquidez baja
                    * CASE liquidez
                        WHEN 'semi_liquido' THEN 0.50
                        ELSE                     1.00
                      END

                    -- Penalización por score_riesgo macro alto
                    * CASE
                        WHEN score_riesgo >= 70 THEN 0.80
                        WHEN score_riesgo >= 50 THEN 0.90
                        ELSE                         1.00
                      END
                )
            , 2)
        END AS sizing_ajustado

    FROM dte_sugerido
),

-- ── Decisión final
final AS (
    SELECT
        ticker,
        snapshot_date,
        direccion,
        contexto,
        estado_macro,
        regimen_vix,
        ROUND(vix, 2)                       AS vix,
        nivel_iv,
        iv_promedio,
        term_structure,
        liquidez,
        estrategia,
        delta_objetivo,
        put_strike,
        put_delta,
        put_theta,
        put_iv,
        COALESCE(put_dte, dte_objetivo)     AS put_dte,
        call_strike,
        sizing_ajustado                     AS sizing,

        CASE
            WHEN estrategia = 'no_trade' THEN 'no_trade'
            ELSE 'active'
        END AS trade_status,

        -- Notas enriquecidas
        'Macro:' || estado_macro
            || ' | VIX:' || ROUND(vix,1)
            || ' | Régimen:' || regimen_vix
            || ' | IV:' || nivel_iv
            || ' (' || iv_promedio || ')'
            || ' | Liquidez:' || liquidez
            || ' | Term:' || term_structure
            || ' | Timing:' || flag_timing
            || ' | DTE_obj:' || COALESCE(dte_objetivo::TEXT, '-')
            || ' | Delta_obj:' || COALESCE(delta_objetivo::TEXT, '-')
            AS notas,

        'v2.0' AS agent_version

    FROM sizing_final
    WHERE estrategia != 'no_trade'     -- solo insertar señales activas
)

SELECT
    ticker, snapshot_date, direccion, contexto,
    estado_macro, regimen_vix, vix,
    nivel_iv, iv_promedio, term_structure, liquidez,
    estrategia, delta_objetivo,
    put_strike, put_delta, put_theta, put_iv, put_dte,
    call_strike,
    sizing, trade_status, notas, agent_version
FROM final

ON CONFLICT (ticker, snapshot_date)
DO UPDATE SET
    direccion      = EXCLUDED.direccion,
    contexto       = EXCLUDED.contexto,
    estado_macro   = EXCLUDED.estado_macro,
    regimen_vix    = EXCLUDED.regimen_vix,
    vix            = EXCLUDED.vix,
    nivel_iv       = EXCLUDED.nivel_iv,
    iv_promedio    = EXCLUDED.iv_promedio,
    term_structure = EXCLUDED.term_structure,
    liquidez       = EXCLUDED.liquidez,
    estrategia     = EXCLUDED.estrategia,
    delta_objetivo = EXCLUDED.delta_objetivo,
    put_strike     = EXCLUDED.put_strike,
    put_delta      = EXCLUDED.put_delta,
    put_theta      = EXCLUDED.put_theta,
    put_iv         = EXCLUDED.put_iv,
    put_dte        = EXCLUDED.put_dte,
    call_strike    = EXCLUDED.call_strike,
    sizing         = EXCLUDED.sizing,
    trade_status   = EXCLUDED.trade_status,
    notas          = EXCLUDED.notas,
    agent_version  = EXCLUDED.agent_version;