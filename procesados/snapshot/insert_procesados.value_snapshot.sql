-- ============================================================
-- INSERT: procesados.value_snapshot
-- Fecha hardcodeada: modificar fechas según el mes
-- ============================================================

INSERT INTO procesados.value_snapshot
SELECT
    DATE '2026-04-01'       AS snapshot_date,
    r.ticker,
    u.companyname,
    u.sector,
    u.industry,
    r.fecha_de_consulta     AS fecha_ratios_ttm,
    km.fecha_de_consulta    AS fecha_keymetrics_ttm,
    km.marketcapttm,
    CASE
        WHEN km.marketcapttm >= 200000000000 THEN 'mega'
        WHEN km.marketcapttm >= 10000000000  THEN 'large'
        WHEN km.marketcapttm >= 2000000000   THEN 'mid'
        WHEN km.marketcapttm >= 300000000    THEN 'small'
        ELSE 'micro'
    END                     AS market_cap_tier,
    -- VALUE — desde keymetrics
    km.freecashflowyieldttm,
    -- VALUE — desde ratios
    r.priceearningsratiottm,
    r.priceearningstogrowthratiottm
FROM limpieza.multifactor_ratios_ttm_limpios r
JOIN limpieza.multifactor_keymetrics_ttm_limpios km
    ON r.ticker = km.ticker
JOIN universos.stock_opciones_2026 s               -- universo de trabajo 2026
    ON r.ticker = s.ticker
JOIN universos.all_usa_common_equity_base u         -- companyname, sector, industry
    ON r.ticker = u.ticker
WHERE r.fecha_de_consulta  = DATE '2026-04-07'     -- << CAMBIAR CADA MES r=ratios
  AND km.fecha_de_consulta = DATE '2026-04-07';    -- << CAMBIAR CADA MES km=keymetrics

-- Verificación
SELECT COUNT(*) AS registros_insertados
FROM procesados.value_snapshot
WHERE snapshot_date = DATE '2026-04-01';
