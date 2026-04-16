
-- ============================================================
-- INSERT: procesados.quality_benchmark_sector_mktcap
-- Benchmark Quality por Sector + Market Cap Tier (fallback)
-- Fecha hardcodeada: modificar según el mes
-- ============================================================
 
INSERT INTO procesados.quality_benchmark_sector_mktcap (
    snapshot_date,
    sector,
    market_cap_tier,
    n_empresas,
    media_roic,
    desvio_roic,
    media_ndebt_ebitda,
    desvio_ndebt_ebitda,
    media_operating_margin,
    desvio_operating_margin,
    media_debt_equity,
    desvio_debt_equity
)
SELECT
    DATE '2026-04-01'       AS snapshot_date,      -- << CAMBIAR CADA MES
    q.sector,
    q.market_cap_tier,
    COUNT(*)                AS n_empresas,
    -- ROIC
    AVG(q.roicttm)                                          AS media_roic,
    COALESCE(STDDEV(q.roicttm), 0.0001)                     AS desvio_roic,
    -- Net Debt / EBITDA (negativos → 0)
    AVG(CASE WHEN q.netdebttoebitdattm < 0 THEN 0
             ELSE q.netdebttoebitdattm END)                  AS media_ndebt_ebitda,
    COALESCE(STDDEV(CASE WHEN q.netdebttoebitdattm < 0 THEN 0
                         ELSE q.netdebttoebitdattm END),
             0.0001)                                         AS desvio_ndebt_ebitda,
    -- Operating Margin
    AVG(q.operatingprofitmarginttm)                          AS media_operating_margin,
    COALESCE(STDDEV(q.operatingprofitmarginttm), 0.0001)     AS desvio_operating_margin,
    -- Debt / Equity
    AVG(q.debtequityratiottm)                                AS media_debt_equity,
    COALESCE(STDDEV(q.debtequityratiottm), 0.0001)           AS desvio_debt_equity
FROM procesados.quality_snapshot q
WHERE q.snapshot_date = DATE '2026-04-01'          -- << CAMBIAR CADA MES
  AND q.sector          IS NOT NULL
  AND q.market_cap_tier IS NOT NULL
GROUP BY
    q.sector,
    q.market_cap_tier;
 
-- Verificación
SELECT COUNT(*) AS grupos_insertados
FROM procesados.quality_benchmark_sector_mktcap
WHERE snapshot_date = DATE '2026-04-01';
