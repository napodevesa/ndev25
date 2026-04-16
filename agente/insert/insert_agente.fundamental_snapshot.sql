INSERT INTO agente.fundamental_snapshot (
    ticker,
    snapshot_date,
    quality_percentile,
    value_percentile,
    roic_trend,
    roic_signo,
    roic_confiable,
    net_debt_ebitda_trend,
    net_debt_ebitda_signo,
    net_debt_ebitda_confiable,
    altman_z_score,
    piotroski_score,
    rsi_14_diario,
    rsi_14_semanal,
    precio_vs_ma200,
    dist_max_52w,
    vol_realizada_30d,
    vol_realizada_90d,
    volume_ratio_20d,
    obv_slope,
    volume_trend_20d,
    sector,              -- nuevo
    industry,            -- nuevo
    market_cap_tier      -- nuevo
)
SELECT
    u.ticker,
    u.snapshot_date,
    mf.quality_percentile,
    mf.value_percentile,
    roic.tendencia          AS roic_trend,
    roic.signo_tendencia    AS roic_signo,
    roic.confiable          AS roic_confiable,
    deuda.tendencia         AS net_debt_ebitda_trend,
    deuda.signo_tendencia   AS net_debt_ebitda_signo,
    deuda.confiable         AS net_debt_ebitda_confiable,
    s.altman_z_score,
    s.piotroski_score,
    t.rsi_14_diario,
    t.rsi_14_semanal,
    t.precio_vs_ma200,
    t.dist_max_52w,
    t.vol_realizada_30d,
    t.vol_realizada_90d,
    t.volume_ratio_20d,
    t.obv_slope,
    t.volume_trend_20d,
    mf.sector,              -- nuevo
    mf.industry,            -- nuevo
    mf.market_cap_tier      -- nuevo
FROM seleccion.universo_trabajo u
LEFT JOIN multifactor.multifactor_sector_industry mf
    ON  mf.ticker        = u.ticker
    AND mf.snapshot_date = u.snapshot_date
LEFT JOIN seleccion.regresiones roic
    ON  roic.ticker        = u.ticker
    AND roic.snapshot_date = u.snapshot_date
    AND roic.metrica       = 'roic'
LEFT JOIN seleccion.regresiones deuda
    ON  deuda.ticker        = u.ticker
    AND deuda.snapshot_date = u.snapshot_date
    AND deuda.metrica       = 'netDebtToEBITDA'
LEFT JOIN seleccion.scores s
    ON  s.ticker        = u.ticker
    AND s.snapshot_date = u.snapshot_date
LEFT JOIN seleccion.technical t
    ON  t.ticker            = u.ticker
    AND t.fecha_de_consulta = '2026-04-08'
WHERE u.snapshot_date = '2026-04-01'
ON CONFLICT (ticker, snapshot_date)
DO UPDATE SET
    quality_percentile        = EXCLUDED.quality_percentile,
    value_percentile          = EXCLUDED.value_percentile,
    roic_trend                = EXCLUDED.roic_trend,
    roic_signo                = EXCLUDED.roic_signo,
    roic_confiable            = EXCLUDED.roic_confiable,
    net_debt_ebitda_trend     = EXCLUDED.net_debt_ebitda_trend,
    net_debt_ebitda_signo     = EXCLUDED.net_debt_ebitda_signo,
    net_debt_ebitda_confiable = EXCLUDED.net_debt_ebitda_confiable,
    altman_z_score            = EXCLUDED.altman_z_score,
    piotroski_score           = EXCLUDED.piotroski_score,
    rsi_14_diario             = EXCLUDED.rsi_14_diario,
    rsi_14_semanal            = EXCLUDED.rsi_14_semanal,
    precio_vs_ma200           = EXCLUDED.precio_vs_ma200,
    dist_max_52w              = EXCLUDED.dist_max_52w,
    vol_realizada_30d         = EXCLUDED.vol_realizada_30d,
    vol_realizada_90d         = EXCLUDED.vol_realizada_90d,
    volume_ratio_20d          = EXCLUDED.volume_ratio_20d,
    obv_slope                 = EXCLUDED.obv_slope,
    volume_trend_20d          = EXCLUDED.volume_trend_20d,
    sector                    = EXCLUDED.sector,
    industry                  = EXCLUDED.industry,
    market_cap_tier           = EXCLUDED.market_cap_tier,
    actualizado_en            = NOW();