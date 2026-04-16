-- ============================================================
--  CAPA SECTOR — VISTAS SQL
--  Flujo: sector_snapshot → v_sector_scores → v_sector_ranking
--                                           → v_sector_diagnostico
--
--  Mismo patrón que Capa MACRO:
--  datos crudos en tabla → lógica en vistas → agente lee vista final
-- ============================================================


-- ────────────────────────────────────────────────────────────
--  VISTA 1: v_sector_scores
--  Lee el último snapshot y calcula los 3 scores por ticker.
--
--  score_momentum : fuerza relativa + retornos (0-100)
--  score_volumen  : confirmación de volumen + OBV  (0-100)
--  score_total    : ponderación final (0-100)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW sector.v_sector_scores AS

WITH ultimo_run AS (
    SELECT MAX(run_id) AS run_id
    FROM sector.sector_snapshot
),

base AS (
    SELECT s.*
    FROM sector.sector_snapshot s
    JOIN ultimo_run u ON s.run_id = u.run_id
),

sm AS (
    SELECT
        id, run_id, calculado_en, fecha_ultimo,
        ticker, tipo, sector_gics, sector_etf, industria, close,
        rs_vs_spy, rsi_rs_diario, rsi_rs_semanal, slope_rs, rs_percentil,
        ret_1m, ret_3m, ret_6m, ret_1a, dist_max_52w,
        vol_ratio, obv_slope, rsi_precio,
        estado_macro, alineacion_macro,

        -- score_momentum
        LEAST(100, GREATEST(0, ROUND(
            (COALESCE(rsi_rs_semanal, 50) * 0.40)
          + (LEAST(100, GREATEST(0, COALESCE(ret_3m, 0) * 2 + 50)) * 0.35)
          + (COALESCE(rs_percentil, 50) * 0.25)
        , 1))) AS score_momentum,

        -- score_volumen
        LEAST(100, GREATEST(0, ROUND(
            (LEAST(100, GREATEST(0, (COALESCE(vol_ratio, 1.0) - 0.5) * 100)) * 0.60)
          + (CASE
                WHEN COALESCE(obv_slope, 0) > 0 THEN 75
                WHEN COALESCE(obv_slope, 0) = 0 THEN 50
                ELSE 25
             END * 0.40)
        , 1))) AS score_volumen

    FROM base
)

SELECT
    id, run_id, calculado_en, fecha_ultimo,
    ticker, tipo, sector_gics, sector_etf, industria, close,
    rs_vs_spy, rsi_rs_diario, rsi_rs_semanal, slope_rs, rs_percentil,
    ret_1m, ret_3m, ret_6m, ret_1a, dist_max_52w,
    vol_ratio, obv_slope, rsi_precio,
    estado_macro, alineacion_macro,
    score_momentum,
    score_volumen,

    -- score_total — puede referenciar score_momentum y score_volumen
    -- porque ya son columnas del SELECT anterior (CTE sm)
    LEAST(100, GREATEST(0, ROUND(
        score_momentum * 0.65
      + score_volumen  * 0.35
    , 1))) AS score_total

FROM sm;


-- ────────────────────────────────────────────────────────────
--  VISTA 2: v_sector_ranking
--  Agrega el estado determinístico y el ranking sobre scores.
--
--  Estado por ticker:
--    LEADING_STRONG → score_total >= 65 y volumen confirma
--    LEADING_WEAK   → score_momentum alto pero volumen débil
--    NEUTRAL        → sin señal clara
--    LAGGING        → score_total < 35
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW sector.v_sector_ranking AS

WITH base AS (
    SELECT * FROM sector.v_sector_scores
)

SELECT
    base.*,

    -- ── Estado determinístico
    CASE
        WHEN base.score_total >= 65 AND base.score_volumen >= 60
        THEN 'LEADING_STRONG'

        WHEN base.score_momentum >= 60 AND base.score_volumen < 60
        THEN 'LEADING_WEAK'

        WHEN base.score_total < 35
        THEN 'LAGGING'

        ELSE 'NEUTRAL'
    END AS estado,

    -- ── Ranking global
    RANK() OVER (
        ORDER BY base.score_total DESC NULLS LAST
    ) AS rank_total,

    -- ── Ranking dentro del sector padre
    RANK() OVER (
        PARTITION BY base.sector_etf
        ORDER BY base.score_total DESC NULLS LAST
    ) AS rank_en_sector

FROM base;


-- ────────────────────────────────────────────────────────────
--  VISTA 3: v_sector_diagnostico
--  Vista final que consumen los agentes.
--  Filtra solo industrias, agrega contexto y top picks.
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW sector.v_sector_diagnostico AS

WITH ranking AS (
    SELECT * FROM sector.v_sector_ranking
    WHERE tipo = 'industria'   -- solo ETFs de industria, no sectoriales
),

-- Top 3 industrias alineadas con el régimen macro
top_aligned AS (
    SELECT STRING_AGG(ticker, ', ' ORDER BY rank_total) AS top_tickers_aligned
    FROM (
        SELECT ticker, rank_total
        FROM ranking
        WHERE alineacion_macro = 'ALIGNED'
          AND estado IN ('LEADING_STRONG', 'LEADING_WEAK')
        ORDER BY rank_total
        LIMIT 3
    ) t
),

-- Top 3 industrias globales (independiente del macro)
top_global AS (
    SELECT STRING_AGG(ticker, ', ' ORDER BY rank_total) AS top_tickers_global
    FROM (
        SELECT ticker, rank_total
        FROM ranking
        ORDER BY rank_total
        LIMIT 3
    ) t
),

-- Conteo de señales
conteo AS (
    SELECT
        COUNT(*) FILTER (WHERE estado = 'LEADING_STRONG')  AS n_leading_strong,
        COUNT(*) FILTER (WHERE estado = 'LEADING_WEAK')    AS n_leading_weak,
        COUNT(*) FILTER (WHERE estado = 'NEUTRAL')         AS n_neutral,
        COUNT(*) FILTER (WHERE estado = 'LAGGING')         AS n_lagging,
        COUNT(*) FILTER (WHERE alineacion_macro = 'ALIGNED'
                           AND estado IN ('LEADING_STRONG','LEADING_WEAK')) AS n_aligned,
        ROUND(AVG(score_total), 1)                         AS score_universo,
        MAX(run_id)                                        AS run_id,
        MAX(calculado_en)                                  AS calculado_en,
        MAX(estado_macro)                                  AS estado_macro
    FROM ranking
)

SELECT
    c.run_id,
    c.calculado_en,
    c.estado_macro,

    -- Resumen del universo
    c.n_leading_strong,
    c.n_leading_weak,
    c.n_neutral,
    c.n_lagging,
    c.n_aligned,
    c.score_universo,          -- score promedio — mide el "tono" del mercado

    -- Top picks
    ta.top_tickers_aligned,    -- mejores industrias alineadas con el macro
    tg.top_tickers_global,     -- mejores industrias sin filtro macro

    -- Señal de rotación
    CASE
        WHEN c.n_leading_strong >= 5  THEN 'ROTACION_CLARA'
        WHEN c.n_leading_strong >= 3  THEN 'ROTACION_MODERADA'
        WHEN c.n_lagging >= 30        THEN 'MERCADO_DEBIL'
        ELSE                               'SIN_TENDENCIA'
    END AS señal_rotacion

FROM conteo c
CROSS JOIN top_aligned ta
CROSS JOIN top_global  tg;


-- ════════════════════════════════════════════════════════════
--  QUERIES DE VERIFICACIÓN
-- ════════════════════════════════════════════════════════════

-- 1. Ver top 10 industrias con scores y estado:
-- SELECT ticker, industria, sector_gics, estado, alineacion_macro,
--        score_momentum, score_volumen, score_total, rank_total,
--        ret_3m, rsi_rs_semanal
-- FROM sector.v_sector_ranking
-- WHERE tipo = 'industria'
-- ORDER BY rank_total
-- LIMIT 10;

-- 2. Ver diagnóstico resumen del universo:
-- SELECT * FROM sector.v_sector_diagnostico;

-- 3. Ver solo los LEADING_STRONG alineados con macro:
-- SELECT ticker, industria, score_total, ret_3m, ret_6m, alineacion_macro
-- FROM sector.v_sector_ranking
-- WHERE estado = 'LEADING_STRONG'
--   AND alineacion_macro = 'ALIGNED'
-- ORDER BY score_total DESC;

-- 4. Ver ranking por sector (top industria de cada sector):
-- SELECT DISTINCT ON (sector_etf)
--        sector_etf, ticker, industria, score_total, estado
-- FROM sector.v_sector_ranking
-- WHERE tipo = 'industria'
-- ORDER BY sector_etf, score_total DESC;