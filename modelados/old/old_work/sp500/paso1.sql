CREATE TABLE modelados.benchmark_sector_industry_mktcap AS
SELECT
    -- Fecha del benchmark: tomamos la última fecha disponible de keymetrics
    MAX(fecha_consulta_keymetrics) AS fecha,

    sector,
    industry,
    market_cap_tier,

    -- Métricas fundamentales: medias y desvíos
    AVG(freecashflowpersharettm)      AS media_fcfps,
    COALESCE(STDDEV(freecashflowpersharettm), 0.0001) AS desvio_fcfps,

    AVG(roicttm)                      AS media_roic,
    COALESCE(STDDEV(roicttm), 0.0001) AS desvio_roic,

    AVG(netdebttoebitdattm)           AS media_ndebt_ebitda,
    COALESCE(STDDEV(netdebttoebitdattm), 0.0001) AS desvio_ndebt_ebitda,

    AVG(freecashflowyieldttm)         AS media_fcf_yield,
    COALESCE(STDDEV(freecashflowyieldttm), 0.0001) AS desvio_fcf_yield,

    AVG(cashflowtodebtratiottm)       AS media_cf_debt,
    COALESCE(STDDEV(cashflowtodebtratiottm), 0.0001) AS desvio_cf_debt

FROM procesados.multifactor_sp500_ratios_keymetrics

WHERE sector IS NOT NULL
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
