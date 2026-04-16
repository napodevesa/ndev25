CREATE TABLE IF NOT EXISTS modelados.value_percentile_all_usa AS
WITH base AS (
    SELECT
        r.ticker,
        r.companyname,
        r.sector,
        r.industry,
        r.market_cap_tier,

        -- Fechas (auditoría)
        r.fecha_de_consulta_ratios_ttm,
        r.fecha_de_consulta_keymetrics_ttm,

        -- ============================
        -- Selección de benchmark REAL
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

        -- PER: si <=0, no se considera
        CASE WHEN r.priceearningsratiottm > 0 THEN r.priceearningsratiottm ELSE NULL END AS pe_adj,

        -- P/S siempre positivo (normalmente)
        r.pricetosalesratiottm AS ps,

        -- P/FCF: si <=0, no se considera
        CASE WHEN r.pricetofreecashflowsratiottm > 0 THEN r.pricetofreecashflowsratiottm ELSE NULL END AS pfcf_adj,

        -- ============================
        -- Contar métricas válidas
        -- ============================
        ((CASE WHEN r.priceearningsratiottm > 0 THEN 1 ELSE 0 END) +
         (CASE WHEN r.pricetosalesratiottm IS NOT NULL THEN 1 ELSE 0 END) +
         (CASE WHEN r.pricetofreecashflowsratiottm > 0 THEN 1 ELSE 0 END)
        ) AS num_metrics_valid

    FROM procesados.value_all_usa r

    LEFT JOIN procesados.value_benchmark_sector_industry_mktcap b_si
        ON r.sector = b_si.sector
       AND r.industry = b_si.industry
       AND r.market_cap_tier = b_si.market_cap_tier

    LEFT JOIN procesados.value_benchmark_sector_mktcap b_s
        ON r.sector = b_s.sector
       AND r.market_cap_tier = b_s.market_cap_tier
),

percentiles AS (
    SELECT
        *,
        -- PERCENT_RANK sobre métricas ajustadas
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
    -- ============================
    -- SCORE VALUE promedio sobre métricas válidas
    -- ============================
    (
        COALESCE(1 - pe_percentile, 0) +
        COALESCE(1 - ps_percentile, 0) +
        COALESCE(1 - pfcf_percentile, 0)
    ) / NULLIF(num_metrics_valid, 0) AS value_score,

    -- Ranking absoluto (1 = más barata)
    RANK() OVER (
        ORDER BY
            (
                COALESCE(1 - pe_percentile, 0) +
                COALESCE(1 - ps_percentile, 0) +
                COALESCE(1 - pfcf_percentile, 0)
            ) / NULLIF(num_metrics_valid, 0) DESC
    ) AS value_rank,

    -- Percentil final (0–100)
    PERCENT_RANK() OVER (
        ORDER BY
            (
                COALESCE(1 - pe_percentile, 0) +
                COALESCE(1 - ps_percentile, 0) +
                COALESCE(1 - pfcf_percentile, 0)
            ) / NULLIF(num_metrics_valid, 0)
    ) * 100 AS value_percentile

FROM percentiles;
