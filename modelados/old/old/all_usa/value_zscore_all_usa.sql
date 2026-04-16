CREATE TABLE IF NOT EXISTS modelados.value_zscore_all_usa AS
WITH base AS (
    SELECT
        r.ticker,
        r.sector,
        r.industry,
        r.market_cap_tier,

        -- Fechas (auditoría)
        r.fecha_consulta_keymetrics,
        r.fecha_consulta_ratios,

        -- ============================
        -- Selección de benchmark REAL
        -- ============================
        CASE
            WHEN b_a.n_empresas >= 5 THEN 'sector_industry_mktcap'
            ELSE 'sector_mktcap'
        END AS benchmark_level_used,

        CASE
            WHEN b_a.n_empresas >= 5 THEN b_a.n_empresas
            ELSE b_b.n_empresas
        END AS benchmark_n_empresas,

        -- ============================
        -- Ratios VALUE
        -- ============================
        r.priceearningsratiottm        AS pe,
        r.pricetosalesratiottm         AS ps,
        r.pricetofreecashflowsratiottm AS pfcf,

        -- ============================
        -- Z-SCORES VALUE (FIX)
        -- MENOR = MEJOR → invertido
        -- ============================

        -- 1) P/E
        -1 * (
            (
                r.priceearningsratiottm -
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.media_pe
                    ELSE b_b.media_pe
                END
            ) / NULLIF(
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.desvio_pe
                    ELSE b_b.desvio_pe
                END,
                0
            )
        ) AS z_pe,

        -- 2) P/S
        -1 * (
            (
                r.pricetosalesratiottm -
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.media_ps
                    ELSE b_b.media_ps
                END
            ) / NULLIF(
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.desvio_ps
                    ELSE b_b.desvio_ps
                END,
                0
            )
        ) AS z_ps,

        -- 3) P/FCF
        -1 * (
            (
                r.pricetofreecashflowsratiottm -
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.media_pfcf
                    ELSE b_b.media_pfcf
                END
            ) / NULLIF(
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.desvio_pfcf
                    ELSE b_b.desvio_pfcf
                END,
                0
            )
        ) AS z_pfcf

    FROM procesados.value_all_usa r

    LEFT JOIN procesados.value_benchmark_sector_industry_mktcap b_a
        ON r.sector = b_a.sector
       AND r.industry = b_a.industry
       AND r.market_cap_tier = b_a.market_cap_tier

    LEFT JOIN procesados.value_benchmark_sector_mktcap b_b
        ON r.sector = b_b.sector
       AND r.market_cap_tier = b_b.market_cap_tier
)

SELECT
    *,
    -- ============================
    -- SCORE FINAL VALUE
    -- ============================
    (
        COALESCE(z_pe, 0) +
        COALESCE(z_ps, 0) +
        COALESCE(z_pfcf, 0)
    ) / 3.0 AS value_score,

    -- ⭐ Ranking absoluto (1 = más barata)
    RANK() OVER (ORDER BY
        (
            COALESCE(z_pe, 0) +
            COALESCE(z_ps, 0) +
            COALESCE(z_pfcf, 0)
        ) / 3.0 DESC
    ) AS value_rank,

    -- ⭐ Percentil (0–100)
    PERCENT_RANK() OVER (ORDER BY
        (
            COALESCE(z_pe, 0) +
            COALESCE(z_ps, 0) +
            COALESCE(z_pfcf, 0)
        ) / 3.0
    ) * 100 AS value_percentile

FROM base;
