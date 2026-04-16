INSERT INTO agente.top_seleccion (
    ticker, snapshot_date,
    sector, industry, market_cap_tier,
    contexto, instrumento, flag_timing,
    quality_percentile, value_percentile,
    piotroski_score, altman_z_score,
    rsi_14_semanal, precio_vs_ma200, volume_ratio_20d,
    roic_signo, roic_confiable, net_debt_ebitda_signo,
    sector_alineado,
    score_conviccion, rank_conviccion,
    target_position_size
)

WITH

-- ── Estado macro y sectores alineados
macro AS (
    SELECT estado_macro
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC
    LIMIT 1
),

-- ── Mapa sector GICS → ETF sectorial → alineación
sector_map AS (
    SELECT
        fs.ticker,
        CASE
            WHEN fs.sector = 'Technology'              THEN 'XLK'
            WHEN fs.sector = 'Healthcare'              THEN 'XLV'
            WHEN fs.sector = 'Consumer Defensive'      THEN 'XLP'
            WHEN fs.sector = 'Utilities'               THEN 'XLU'
            WHEN fs.sector = 'Energy'                  THEN 'XLE'
            WHEN fs.sector = 'Financial Services'      THEN 'XLF'
            WHEN fs.sector = 'Industrials'             THEN 'XLI'
            WHEN fs.sector = 'Basic Materials'         THEN 'XLB'
            WHEN fs.sector = 'Real Estate'             THEN 'XLRE'
            WHEN fs.sector = 'Communication Services'  THEN 'XLC'
            WHEN fs.sector = 'Consumer Cyclical'       THEN 'XLY'
            ELSE NULL
        END AS etf_sector
    FROM agente.fundamental_snapshot fs
    WHERE fs.snapshot_date = '2026-04-01'
),

-- ── Alineación: ¿el ETF del sector está ALIGNED?
alineacion AS (
    SELECT
        sm.ticker,
        CASE
            WHEN sr.alineacion_macro = 'ALIGNED' THEN 'ALIGNED'
            ELSE 'NEUTRAL'
        END AS sector_alineado
    FROM sector_map sm
    LEFT JOIN sector.v_sector_ranking sr
        ON  sr.ticker = sm.etf_sector
        AND sr.tipo   = 'sector'
),

-- ── Score de convicción
scored AS (
    SELECT
        d.ticker,
        d.snapshot_date,
        fs.sector,
        fs.industry,
        fs.market_cap_tier,
        d.contexto,
        d.instrumento,
        d.flag_timing,
        fs.quality_percentile,
        fs.value_percentile,
        fs.piotroski_score,
        fs.altman_z_score,
        fs.rsi_14_semanal,
        fs.precio_vs_ma200,
        fs.volume_ratio_20d,
        fs.roic_signo,
        fs.roic_confiable,
        fs.net_debt_ebitda_signo,
        a.sector_alineado,
        d.target_position_size,

        -- ── Score de convicción 0-100
        ROUND(
            -- Base fundamental 50%
            (COALESCE(fs.quality_percentile, 50) * 0.35
           + COALESCE(fs.value_percentile,   50) * 0.15)

            -- Timing técnico 30%
            + CASE d.flag_timing
                WHEN 'tecnico_confirmado'  THEN 30
                WHEN 'pullback_comprable'  THEN 25
                WHEN 'macro_defensivo'     THEN 15
                WHEN 'esperar_pullback'    THEN 10
                ELSE                           12
              END

            -- Salud financiera 15%
            + CASE
                WHEN COALESCE(fs.piotroski_score, 0) >= 7 THEN 15
                WHEN COALESCE(fs.piotroski_score, 0) >= 5 THEN 10
                ELSE                                           5
              END

            -- Bonus alineación sectorial 5%
            + CASE WHEN a.sector_alineado = 'ALIGNED' THEN 5 ELSE 0 END

        , 1) AS score_conviccion

    FROM agente.trade_decision_direction d
    JOIN agente.fundamental_snapshot fs
        ON  fs.ticker        = d.ticker
        AND fs.snapshot_date = d.snapshot_date
    LEFT JOIN alineacion a
        ON  a.ticker = d.ticker
    WHERE d.snapshot_date = '2026-04-01'
      AND d.trade_status   = 'active'
),

-- ── Ranking y filtro top 50
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY score_conviccion DESC) AS rank_conviccion
    FROM scored
)

SELECT
    ticker, snapshot_date,
    sector, industry, market_cap_tier,
    contexto, instrumento, flag_timing,
    quality_percentile, value_percentile,
    piotroski_score, altman_z_score,
    rsi_14_semanal, precio_vs_ma200, volume_ratio_20d,
    roic_signo, roic_confiable, net_debt_ebitda_signo,
    sector_alineado,
    score_conviccion, rank_conviccion,
    target_position_size
FROM ranked
WHERE rank_conviccion <= 50     -- top 50 empresas

ON CONFLICT (ticker, snapshot_date)
DO UPDATE SET
    score_conviccion     = EXCLUDED.score_conviccion,
    rank_conviccion      = EXCLUDED.rank_conviccion,
    sector_alineado      = EXCLUDED.sector_alineado,
    flag_timing          = EXCLUDED.flag_timing,
    instrumento          = EXCLUDED.instrumento,
    target_position_size = EXCLUDED.target_position_size,
    creado_en            = NOW();