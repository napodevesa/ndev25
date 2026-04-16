CREATE TABLE IF NOT EXISTS modelados.quality_zscore_all_usa AS
WITH base AS (
    SELECT
        r.ticker,
        r.sector,
        r.industry,
        r.market_cap_tier,
        r.fecha_de_consulta_keymetrics_ttm,
        r.fecha_de_consulta_ratios_ttm,

        -- ==================================
        -- Selección de benchmark real
        -- ==================================
        CASE
            WHEN b_a.n_empresas >= 5 THEN 'sector_industry_mktcap'
            ELSE 'sector_mktcap'
        END AS benchmark_level_used,

        CASE
            WHEN b_a.n_empresas >= 5 THEN b_a.n_empresas
            ELSE b_b.n_empresas
        END AS benchmark_n_empresas,

        -- ==================================
        -- Z-SCORES RAW
        -- ==================================
        (r.freecashflowpersharettm -
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_fcfps ELSE b_b.media_fcfps END
        ) / NULLIF(
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_fcfps ELSE b_b.desvio_fcfps END, 0
        ) AS z_fcfps_raw,

        (r.roicttm -
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_roic ELSE b_b.media_roic END
        ) / NULLIF(
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_roic ELSE b_b.desvio_roic END, 0
        ) AS z_roic_raw,

        -1 * (
            (r.netdebttoebitdattm -
                CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_ndebt_ebitda ELSE b_b.media_ndebt_ebitda END
            ) / NULLIF(
                CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_ndebt_ebitda ELSE b_b.desvio_ndebt_ebitda END, 0
            )
        ) AS z_netdebt_ebitda_raw,

        (r.freecashflowyieldttm -
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_fcf_yield ELSE b_b.media_fcf_yield END
        ) / NULLIF(
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_fcf_yield ELSE b_b.desvio_fcf_yield END, 0
        ) AS z_fcf_yield_raw,

        (r.cashflowtodebtratiottm -
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_cf_debt ELSE b_b.media_cf_debt END
        ) / NULLIF(
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_cf_debt ELSE b_b.desvio_cf_debt END, 0
        ) AS z_cf_debt_raw

    FROM procesados.quality_all_usa r
    LEFT JOIN procesados.quality_benchmark_sector_industry_mktcap b_a
        ON r.sector = b_a.sector
       AND r.industry = b_a.industry
       AND r.market_cap_tier = b_a.market_cap_tier
    LEFT JOIN procesados.quality_benchmark_sector_mktcap b_b
        ON r.sector = b_b.sector
       AND r.market_cap_tier = b_b.market_cap_tier
),

winsorized AS (
    SELECT
        *,
        -- Winsorización a [-3, 3] para robustez
        LEAST(3, GREATEST(-3, z_fcfps_raw))          AS z_fcfps,
        LEAST(3, GREATEST(-3, z_roic_raw))           AS z_roic,
        LEAST(3, GREATEST(-3, z_netdebt_ebitda_raw)) AS z_netdebt_ebitda,
        LEAST(3, GREATEST(-3, z_fcf_yield_raw))      AS z_fcf_yield,
        LEAST(3, GREATEST(-3, z_cf_debt_raw))        AS z_cf_debt,

        -- Conteo de métricas válidas para auditoría / control
        ((CASE WHEN z_fcfps_raw IS NOT NULL THEN 1 ELSE 0 END) +
         (CASE WHEN z_roic_raw IS NOT NULL THEN 1 ELSE 0 END) +
         (CASE WHEN z_netdebt_ebitda_raw IS NOT NULL THEN 1 ELSE 0 END) +
         (CASE WHEN z_fcf_yield_raw IS NOT NULL THEN 1 ELSE 0 END) +
         (CASE WHEN z_cf_debt_raw IS NOT NULL THEN 1 ELSE 0 END)
        ) AS num_metrics_valid
    FROM base
)

SELECT
    *,
    -- ==================================
    -- Asignación de pesos para Quality Score
    -- ==================================
    (0.15 * COALESCE(z_fcfps, 0) +
     0.30 * COALESCE(z_roic, 0) +
     0.15 * COALESCE(z_netdebt_ebitda, 0) +
     0.25 * COALESCE(z_fcf_yield, 0) +
     0.15 * COALESCE(z_cf_debt, 0)
    ) AS quality_score,

    -- Ranking y percentil
    RANK() OVER (ORDER BY
        (0.15 * COALESCE(z_fcfps, 0) +
         0.30 * COALESCE(z_roic, 0) +
         0.15 * COALESCE(z_netdebt_ebitda, 0) +
         0.25 * COALESCE(z_fcf_yield, 0) +
         0.15 * COALESCE(z_cf_debt, 0)
        ) DESC
    ) AS quality_rank,

    PERCENT_RANK() OVER (ORDER BY
        (0.15 * COALESCE(z_fcfps, 0) +
         0.30 * COALESCE(z_roic, 0) +
         0.15 * COALESCE(z_netdebt_ebitda, 0) +
         0.25 * COALESCE(z_fcf_yield, 0) +
         0.15 * COALESCE(z_cf_debt, 0)
        )
    ) * 100 AS quality_percentile

FROM winsorized;

