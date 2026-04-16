-- Limpiar el día actual y reinsertar
-- (siempre refleja el universo_trabajo vigente)
DELETE FROM seleccion.technical
WHERE fecha_de_consulta = CURRENT_DATE;

INSERT INTO seleccion.technical (
    ticker, fecha_de_consulta,
    rsi_14_diario, rsi_14_semanal,
    precio_vs_ma200, dist_max_52w,
    vol_realizada_30d, vol_realizada_90d,
    volume_ratio_20d, obv_slope, volume_trend_20d,
    run_id
)
SELECT
    u.ticker,
    CURRENT_DATE,
    t.rsi_14_diario,
    t.rsi_14_semanal,
    t.precio_vs_ma200,
    t.dist_max_52w,
    t.vol_realizada_30d,
    t.vol_realizada_90d,
    t.volume_ratio_20d,
    t.obv_slope,
    t.volume_trend_20d,
    t.run_id
FROM seleccion.universo_trabajo u
LEFT JOIN procesados.multifactor_technical t
    ON  t.ticker            = u.ticker
    AND t.fecha_de_consulta = CURRENT_DATE;