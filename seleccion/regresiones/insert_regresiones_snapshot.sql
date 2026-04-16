INSERT INTO seleccion.regresiones_snapshot (
    ticker,
    snapshot_date,
    roic_trend,
    net_debt_ebitda_trend
)
SELECT
    r.ticker,
    r.snapshot_date,
    MAX(CASE WHEN r.metrica = 'roic'
             THEN r.signo_tendencia END),
    MAX(CASE WHEN r.metrica = 'netDebtToEBITDA'
             THEN r.signo_tendencia END)
FROM seleccion.regresiones r
WHERE r.snapshot_date = '2026-04-01'
GROUP BY
    r.ticker,
    r.snapshot_date
ON CONFLICT (ticker, snapshot_date)
DO NOTHING;