CREATE TABLE IF NOT EXISTS modelados.quality_zscore_all_usa_old AS
WITH base AS (
    SELECT
        r.ticker,
        r.sector,
        r.industry,
        r.market_cap_tier,
        r.fecha_consulta_keymetrics,

        -- ============================
        -- Selección de benchmark
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
        -- Z-SCORES QUALITY
        -- ============================

        -- 1) Free Cash Flow per Share (mayor es mejor)
        (
            r.freecashflowpersharettm -
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.media_fcfps
                ELSE b_b.media_fcfps
            END
        ) / NULLIF(
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.desvio_fcfps
                ELSE b_b.desvio_fcfps
            END,
            0
        ) AS z_fcfps,

        -- 2) ROIC (mayor es mejor)
        (
            r.roicttm -
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.media_roic
                ELSE b_b.media_roic
            END
        ) / NULLIF(
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.desvio_roic
                ELSE b_b.desvio_roic
            END,
            0
        ) AS z_roic,

        -- 3) Net Debt / EBITDA (MENOR ES MEJOR → SE INVIERTE)
        -1 * (
            (
                r.netdebttoebitdattm -
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.media_ndebt_ebitda
                    ELSE b_b.media_ndebt_ebitda
                END
            ) / NULLIF(
                CASE
                    WHEN b_a.n_empresas >= 5 THEN b_a.desvio_ndebt_ebitda
                    ELSE b_b.desvio_ndebt_ebitda
                END,
                0
            )
        ) AS z_netdebt_ebitda,

        -- 4) Free Cash Flow Yield (mayor es mejor)
        (
            r.freecashflowyieldttm -
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.media_fcf_yield
                ELSE b_b.media_fcf_yield
            END
        ) / NULLIF(
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.desvio_fcf_yield
                ELSE b_b.desvio_fcf_yield
            END,
            0
        ) AS z_fcf_yield,

        -- 5) Cash Flow to Debt (mayor es mejor)
        (
            r.cashflowtodebtratiottm -
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.media_cf_debt
                ELSE b_b.media_cf_debt
            END
        ) / NULLIF(
            CASE
                WHEN b_a.n_empresas >= 5 THEN b_a.desvio_cf_debt
                ELSE b_b.desvio_cf_debt
            END,
            0
        ) AS z_cf_debt

    FROM procesados.quality_all_usa r

    LEFT JOIN procesados.quality_benchmark_sector_industry_mktcap b_a
        ON r.sector = b_a.sector
       AND r.industry = b_a.industry
       AND r.market_cap_tier = b_a.market_cap_tier

    LEFT JOIN procesados.quality_benchmark_sector_mktcap b_b
        ON r.sector = b_b.sector
       AND r.market_cap_tier = b_b.market_cap_tier
)

SELECT
    *,
    -- ============================
    -- ⭐ SCORE FINAL QUALITY
    -- ============================
    (
        COALESCE(z_fcfps, 0) +
        COALESCE(z_roic, 0) +
        COALESCE(z_netdebt_ebitda, 0) +
        COALESCE(z_fcf_yield, 0) +
        COALESCE(z_cf_debt, 0)
    ) / 5.0 AS quality_score
FROM base;
