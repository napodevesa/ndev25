INSERT INTO modelados.value_percentile_all_usa
WITH snapshot AS (
    SELECT
        MAX(fecha_de_consulta_ratios_ttm)     AS f_ratios,
        MAX(fecha_de_consulta_keymetrics_ttm) AS f_keymetrics
    FROM procesados.value_all_usa
),

base AS (
    SELECT
        r.ticker,
        r.companyname,
        r.sector,
        r.industry,
        r.market_cap_tier,

        -- 📅 Fechas del snapshot
        s.f_ratios     AS fecha_de_consulta_ratios_ttm,
        s.f_keymetrics AS fecha_de_consulta_keymetrics_ttm,

        -- ============================
        -- Selección del benchmark
        -- ============================
        CASE
            WHEN b_si.n_empresas >= 5 THEN 'sector_industry_mktcap'
            ELSE 'sector_mktcap'
        END AS benchmark_level_used,

        CASE
            WHEN b_si.n_empresas >= 5 THEN b_si.n_empresas
            ELSE b_s.n_empresas
        END AS benchmark_n_empresas,

        -- ============================
        -- Ratios VALUE ajustados
        -- ============================
        CASE
            WHEN r.priceearningsratiottm > 0 THEN r.priceearningsratiottm
            ELSE NULL
        END AS pe_adj,

        r.pricetosalesratiottm AS ps,

        CASE
            WHEN r.pricetofreecashflowsratiottm > 0 THEN r.pricetofreecashflowsratiottm
            ELSE NULL
        END AS pfcf_adj,

        -- ============================
        -- Métricas válidas
        -- ============================
        (
            (CASE WHEN r.priceearningsratiottm > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN r.pricetosalesratiottm IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN r.pricetofreecashflowsratiottm > 0 THEN 1 ELSE 0 END)
        ) AS num_metrics_valid

    FROM procesados.value_all_usa r
    CROSS JOIN snapshot s

    -- 🔒 Benchmark sector + industry + mktcap (misma fecha)
    LEFT JOIN procesados.value_benchmark_sector_industry_mktcap b_si
        ON r.sector = b_si.sector
       AND r.industry = b_si.industry
       AND r.market_cap_tier = b_si.market_cap_tier
       AND b_si.fecha_de_consulta_ratios_ttm     = s.f_ratios
       AND b_si.fecha_de_consulta_keymetrics_ttm = s.f_keymetrics

    -- 🔒 Benchmark sector + mktcap (misma fecha)
    LEFT JOIN procesados.value_benchmark_sector_mktcap b_s
        ON r.sector = b_s.sector
       AND r.market_cap_tier = b_s.market_cap_tier
       AND b_s.fecha_de_consulta_ratios_ttm     = s.f_ratios
       AND b_s.fecha_de_consulta_keymetrics_ttm = s.f_keymetrics

    -- 🔒 Congelar universo AL FINAL
    WHERE
        r.fecha_de_consulta_ratios_ttm     = s.f_ratios
        AND r.fecha_de_consulta_keymetrics_ttm = s.f_keymetrics
),

percentiles AS (
    SELECT
        *,
        CASE
            WHEN benchmark_level_used = 'sector_industry_mktcap' THEN
                PERCENT_RANK() OVER (
                    PARTITION BY sector, industry, market_cap_tier
                    ORDER BY pe_adj
                )
            ELSE
                PERCENT_RANK() OVER (
                    PARTITION BY sector, market_cap_tier
                    ORDER BY pe_adj
                )
        END AS pe_percentile,

        CASE
            WHEN benchmark_level_used = 'sector_industry_mktcap' THEN
                PERCENT_RANK() OVER (
                    PARTITION BY sector, industry, market_cap_tier
                    ORDER BY ps
                )
            ELSE
                PERCENT_RANK() OVER (
                    PARTITION BY sector, market_cap_tier
                    ORDER BY ps
                )
        END AS ps_percentile,

        CASE
            WHEN benchmark_level_used = 'sector_industry_mktcap' THEN
                PERCENT_RANK() OVER (
                    PARTITION BY sector, industry, market_cap_tier
                    ORDER BY pfcf_adj
                )
            ELSE
                PERCENT_RANK() OVER (
                    PARTITION BY sector, market_cap_tier
                    ORDER BY pfcf_adj
                )
        END AS pfcf_percentile
    FROM base
)

SELECT
    *,
    (
        COALESCE(1 - pe_percentile, 0) +
        COALESCE(1 - ps_percentile, 0) +
        COALESCE(1 - pfcf_percentile, 0)
    ) / NULLIF(num_metrics_valid, 0) AS value_score,

    RANK() OVER (
        ORDER BY
            (
                COALESCE(1 - pe_percentile, 0) +
                COALESCE(1 - ps_percentile, 0) +
                COALESCE(1 - pfcf_percentile, 0)
            ) / NULLIF(num_metrics_valid, 0) DESC
    ) AS value_rank,

    PERCENT_RANK() OVER (
        ORDER BY
            (
                COALESCE(1 - pe_percentile, 0) +
                COALESCE(1 - ps_percentile, 0) +
                COALESCE(1 - pfcf_percentile, 0)
            ) / NULLIF(num_metrics_valid, 0)
    ) * 100 AS value_percentile
FROM percentiles;

