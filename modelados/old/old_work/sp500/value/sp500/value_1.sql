CREATE TABLE IF NOT EXISTS modelados.value_benchmark_sp500 AS
SELECT
    -- Fecha del benchmark (última disponible)
    MAX(fecha_consulta_keymetrics) AS fecha,

    sector,
    industry,
    market_cap_tier,

    -- VALUE: Price-based metrics (mean & stddev)
    AVG(priceearningsratiottm) AS mean_pe_ttm,
    COALESCE(STDDEV(priceearningsratiottm), 0.0001) AS std_pe_ttm,

    AVG(pricetosalesratiottm) AS mean_ps_ttm,
    COALESCE(STDDEV(pricetosalesratiottm), 0.0001) AS std_ps_ttm,

    AVG(pricetofreecashflowsratiottm) AS mean_pfcf_ttm,
    COALESCE(STDDEV(pricetofreecashflowsratiottm), 0.0001) AS std_pfcf_ttm

FROM procesados.value_sp500

WHERE
    sector IS NOT NULL
    AND industry IS NOT NULL
    AND market_cap_tier IS NOT NULL

GROUP BY
    sector,
    industry,
    market_cap_tier

ORDER BY
    sector,
    industry,
    market_cap_tier;
