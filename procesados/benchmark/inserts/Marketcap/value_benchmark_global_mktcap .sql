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