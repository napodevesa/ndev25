CREATE TABLE IF NOT EXISTS modelados.value_zscore_sp500 AS
SELECT
    r.ticker,
    r.sector,
    r.industry,
    r.market_cap_tier,

    -- ============================
    -- Z-SCORES INDIVIDUALES (VALUE)
    -- ============================

    -- 1) P/E TTM (menor es mejor → invertimos signo)
    -1 * (
        (r.priceearningsratiottm - b.mean_pe_ttm)
        / NULLIF(b.std_pe_ttm, 0)
    ) AS z_pe,

    -- 2) P/S TTM (menor es mejor → invertimos signo)
    -1 * (
        (r.pricetosalesratiottm - b.mean_ps_ttm)
        / NULLIF(b.std_ps_ttm, 0)
    ) AS z_ps,

    -- 3) P/FCF TTM (menor es mejor → invertimos signo)
    -1 * (
        (r.pricetofreecashflowsratiottm - b.mean_pfcf_ttm)
        / NULLIF(b.std_pfcf_ttm, 0)
    ) AS z_pfcf,

    -- ============================
    -- ⭐ Z-SCORE TOTAL VALUE
    -- ============================
    (
          -1 * ( (r.priceearningsratiottm       - b.mean_pe_ttm)   / NULLIF(b.std_pe_ttm, 0) )
        + -1 * ( (r.pricetosalesratiottm         - b.mean_ps_ttm)   / NULLIF(b.std_ps_ttm, 0) )
        + -1 * ( (r.pricetofreecashflowsratiottm - b.mean_pfcf_ttm) / NULLIF(b.std_pfcf_ttm, 0) )
    ) / 3.0 AS z_value_total

FROM procesados.value_sp500 r

JOIN modelados.value_benchmark_sp500 b
    ON r.sector = b.sector
   AND r.industry = b.industry
   AND r.market_cap_tier = b.market_cap_tier;
