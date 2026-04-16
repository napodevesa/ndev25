INSERT INTO procesados.momentum_benchmark_global_mktcap (
    snapshot_date,
    market_cap_tier,
    n_empresas,
    media_momentum,
    desvio_momentum,
    momentum_p25,
    momentum_p50,
    momentum_p75
)
SELECT
    DATE '2026-04-01' AS snapshot_date,
    m.market_cap_tier,
    COUNT(*) AS n_empresas,
    AVG(
        CASE 
            WHEN m.momentum_12m_1m > 3.0  THEN 3.0
            WHEN m.momentum_12m_1m < -0.8 THEN -0.8
            ELSE m.momentum_12m_1m
        END
    ) AS media_momentum,
    COALESCE(
        STDDEV(
            CASE 
                WHEN m.momentum_12m_1m > 3.0  THEN 3.0
                WHEN m.momentum_12m_1m < -0.8 THEN -0.8
                ELSE m.momentum_12m_1m
            END
        ), 0.0001
    ) AS desvio_momentum,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CASE WHEN m.momentum_12m_1m > 3.0 THEN 3.0 WHEN m.momentum_12m_1m < -0.8 THEN -0.8 ELSE m.momentum_12m_1m END) AS momentum_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CASE WHEN m.momentum_12m_1m > 3.0 THEN 3.0 WHEN m.momentum_12m_1m < -0.8 THEN -0.8 ELSE m.momentum_12m_1m END) AS momentum_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CASE WHEN m.momentum_12m_1m > 3.0 THEN 3.0 WHEN m.momentum_12m_1m < -0.8 THEN -0.8 ELSE m.momentum_12m_1m END) AS momentum_p75
FROM procesados.momentum_snapshot m
WHERE m.snapshot_date = DATE '2026-04-01'
  AND m.market_cap_tier IS NOT NULL
  AND m.momentum_12m_1m IS NOT NULL
GROUP BY m.market_cap_tier;


-- ============================================================
-- INSERT: procesados.quality_benchmark_global_mktcap
-- Benchmark Quality GLOBAL por Market Cap Tier
-- Fecha hardcodeada: modificar según el mes
-- ============================================================

INSERT INTO procesados.quality_benchmark_global_mktcap (
    snapshot_date,
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
    DATE '2026-04-01'        AS snapshot_date,      -- << CAMBIAR CADA MES
    q.market_cap_tier,
    COUNT(*)                 AS n_empresas,
    -- ROIC
    AVG(q.roicttm)                                          AS media_roic,
    COALESCE(STDDEV(q.roicttm), 0.0001)                     AS desvio_roic,
    -- Net Debt / EBITDA (negativos → 0 para estabilidad)
    AVG(CASE WHEN q.netdebttoebitdattm < 0 THEN 0 
             ELSE q.netdebttoebitdattm END)                 AS media_ndebt_ebitda,
    COALESCE(STDDEV(CASE WHEN q.netdebttoebitdattm < 0 THEN 0 
                          ELSE q.netdebttoebitdattm END), 
             0.0001)                                        AS desvio_ndebt_ebitda,
    -- Operating Margin
    AVG(q.operatingprofitmarginttm)                         AS media_operating_margin,
    COALESCE(STDDEV(q.operatingprofitmarginttm), 0.0001)    AS desvio_operating_margin,
    -- Debt / Equity
    AVG(q.debtequityratiottm)                               AS media_debt_equity,
    COALESCE(STDDEV(q.debtequityratiottm), 0.0001)          AS desvio_debt_equity
FROM procesados.quality_snapshot q
WHERE q.snapshot_date = DATE '2026-04-01'           -- << CAMBIAR CADA MES
  AND q.market_cap_tier IS NOT NULL
GROUP BY
    q.market_cap_tier;

-- Verificación
SELECT market_cap_tier, n_empresas, media_roic
FROM procesados.quality_benchmark_global_mktcap
WHERE snapshot_date = DATE '2026-04-01';

-- ============================================================
-- INSERT: procesados.value_benchmark_global_mktcap
-- Benchmark Value GLOBAL por Market Cap Tier (Percentiles)
-- Fecha hardcodeada: modificar según el mes
-- ============================================================

INSERT INTO procesados.value_benchmark_global_mktcap (
    snapshot_date,
    market_cap_tier,
    n_empresas,
    pe_p25, pe_p50, pe_p75,
    peg_p25, peg_p50, peg_p75,
    fcfyield_p25, fcfyield_p50, fcfyield_p75
)
SELECT
    DATE '2026-04-01'        AS snapshot_date,      -- << CAMBIAR CADA MES
    v.market_cap_tier,
    COUNT(*)                 AS n_empresas,

    -- PER (excluir negativos y outliers > 100)
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY 
        CASE WHEN v.priceearningsratiottm <= 0 
              OR v.priceearningsratiottm > 100 
             THEN NULL ELSE v.priceearningsratiottm END)         AS pe_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY 
        CASE WHEN v.priceearningsratiottm <= 0 
              OR v.priceearningsratiottm > 100 
             THEN NULL ELSE v.priceearningsratiottm END)         AS pe_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
        CASE WHEN v.priceearningsratiottm <= 0 
              OR v.priceearningsratiottm > 100 
             THEN NULL ELSE v.priceearningsratiottm END)         AS pe_p75,

    -- PEG (excluir negativos y outliers > 10)
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY 
        CASE WHEN v.priceearningstogrowthratiottm <= 0 
              OR v.priceearningstogrowthratiottm > 10 
             THEN NULL ELSE v.priceearningstogrowthratiottm END) AS peg_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY 
        CASE WHEN v.priceearningstogrowthratiottm <= 0 
              OR v.priceearningstogrowthratiottm > 10 
             THEN NULL ELSE v.priceearningstogrowthratiottm END) AS peg_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
        CASE WHEN v.priceearningstogrowthratiottm <= 0 
              OR v.priceearningstogrowthratiottm > 10 
             THEN NULL ELSE v.priceearningstogrowthratiottm END) AS peg_p75,

    -- FCF Yield (excluir negativos)
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY 
        CASE WHEN v.freecashflowyieldttm <= 0 
             THEN NULL ELSE v.freecashflowyieldttm END)          AS fcfyield_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY 
        CASE WHEN v.freecashflowyieldttm <= 0 
             THEN NULL ELSE v.freecashflowyieldttm END)          AS fcfyield_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY 
        CASE WHEN v.freecashflowyieldttm <= 0 
             THEN NULL ELSE v.freecashflowyieldttm END)          AS fcfyield_p75

FROM procesados.value_snapshot v
WHERE v.snapshot_date   = DATE '2026-04-01'        -- << CAMBIAR CADA MES
  AND v.market_cap_tier IS NOT NULL
GROUP BY
    v.market_cap_tier;

-- Verificación
SELECT market_cap_tier, n_empresas, pe_p50 AS mediana_pe, fcfyield_p50 AS mediana_fcfy
FROM procesados.value_benchmark_global_mktcap
WHERE snapshot_date = DATE '2026-04-01';