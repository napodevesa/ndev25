CREATE TABLE modelados.mid400_zscore_sp500 AS
SELECT
    r.ticker,
    r.fecha_consulta_keymetrics,
    r.fecha_consulta_ratios,
    r.sector,
    r.industry,
    r.market_cap_tier,

    -- ============================
    -- Z-SCORES INDIVIDUALES
    -- ============================

    -- 1) FCF per Share (mayor es mejor → normal)
    (r.freecashflowpersharettm - b.media_fcfps)
        / NULLIF(b.desvio_fcfps, 0) AS z_fcfps,

    -- 2) ROIC (mayor es mejor → normal)
    (r.roicttm - b.media_roic)
        / NULLIF(b.desvio_roic, 0) AS z_roic,

    -- 3) Net Debt / EBITDA (MENOR ES MEJOR → SE INVIERTE)
    -1 * (
        (r.netdebttoebitdattm - b.media_ndebt_ebitda)
        / NULLIF(b.desvio_ndebt_ebitda, 0)
    ) AS z_netdebt_ebitda,

    -- 4) FCF Yield (mayor es mejor → normal)
    (r.freecashflowyieldttm - b.media_fcf_yield)
        / NULLIF(b.desvio_fcf_yield, 0) AS z_fcf_yield,

    -- 5) Cash Flow to Debt (mayor es mejor → normal)
    (r.cashflowtodebtratiottm - b.media_cf_debt)
        / NULLIF(b.desvio_cf_debt, 0) AS z_cf_debt,

    -- ============================
    -- ⭐ Z-SCORE TOTAL (PROMEDIO)
    -- ============================
    (
          ( (r.freecashflowpersharettm - b.media_fcfps)     / NULLIF(b.desvio_fcfps, 0) )
        + ( (r.roicttm - b.media_roic)                      / NULLIF(b.desvio_roic, 0) )
        + ( -1 * ((r.netdebttoebitdattm - b.media_ndebt_ebitda) / NULLIF(b.desvio_ndebt_ebitda, 0)) )
        + ( (r.freecashflowyieldttm - b.media_fcf_yield)    / NULLIF(b.desvio_fcf_yield, 0) )
        + ( (r.cashflowtodebtratiottm - b.media_cf_debt)    / NULLIF(b.desvio_cf_debt, 0) )
    ) / 5 AS zscore_total

FROM procesados.multifactor_mid400_ratios_keymetrics r
LEFT JOIN modelados.mid400_benchmark_sector_industry_mktcap b
    ON r.sector          = b.sector
   AND r.industry        = b.industry
   AND r.market_cap_tier = b.market_cap_tier;