-- ============================================================
-- INSERT: modelados.quality_zscore_all_usa
-- Z-Score Quality — 4 métricas de igual peso (25% cada una)
-- Fecha hardcodeada: modificar según el mes
-- ============================================================

INSERT INTO modelados.quality_zscore_all_usa (
    ticker,
    sector,
    industry,
    market_cap_tier,
    snapshot_date,
    benchmark_level_used,
    benchmark_n_empresas,
    z_roic_raw,
    z_netdebt_ebitda_raw,
    z_operating_margin_raw,
    z_debt_equity_raw,
    z_roic,
    z_netdebt_ebitda,
    z_operating_margin,
    z_debt_equity,
    num_metrics_valid,
    quality_score,
    quality_rank,
    quality_percentile,
    created_at
)

WITH params AS (
    SELECT DATE '2026-04-01' AS snapshot_date          -- << CAMBIAR CADA MES
),

base AS (
    SELECT
        r.ticker,
        r.sector,
        COALESCE(r.industry, 'Unknown') AS industry,
        r.market_cap_tier,
        r.snapshot_date,

        CASE
            WHEN b_a.n_empresas >= 5 THEN 'sector_industry_mktcap'
            ELSE 'sector_mktcap'
        END AS benchmark_level_used,

        CASE
            WHEN b_a.n_empresas >= 5 THEN b_a.n_empresas
            ELSE b_b.n_empresas
        END AS benchmark_n_empresas,

        -- ROIC (más alto = mejor)
        (r.roicttm -
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_roic ELSE b_b.media_roic END
        ) / NULLIF(
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_roic ELSE b_b.desvio_roic END, 0
        ) AS z_roic_raw,

        -- Net Debt / EBITDA (más bajo = mejor → invertido)
        -1 * (
            (r.netdebttoebitdattm -
                CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_ndebt_ebitda ELSE b_b.media_ndebt_ebitda END
            ) / NULLIF(
                CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_ndebt_ebitda ELSE b_b.desvio_ndebt_ebitda END, 0
            )
        ) AS z_netdebt_ebitda_raw,

        -- Operating Margin (más alto = mejor)
        (r.operatingprofitmarginttm -
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_operating_margin ELSE b_b.media_operating_margin END
        ) / NULLIF(
            CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_operating_margin ELSE b_b.desvio_operating_margin END, 0
        ) AS z_operating_margin_raw,

        -- Debt / Equity (más bajo = mejor → invertido)
        -1 * (
            (r.debtequityratiottm -
                CASE WHEN b_a.n_empresas >= 5 THEN b_a.media_debt_equity ELSE b_b.media_debt_equity END
            ) / NULLIF(
                CASE WHEN b_a.n_empresas >= 5 THEN b_a.desvio_debt_equity ELSE b_b.desvio_debt_equity END, 0
            )
        ) AS z_debt_equity_raw

    FROM procesados.quality_snapshot r
    JOIN params p
      ON r.snapshot_date = p.snapshot_date

    LEFT JOIN procesados.quality_benchmark_sector_industry_mktcap b_a
        ON r.snapshot_date    = b_a.snapshot_date
       AND r.sector           = b_a.sector
       AND r.industry         = b_a.industry
       AND r.market_cap_tier  = b_a.market_cap_tier

    LEFT JOIN procesados.quality_benchmark_sector_mktcap b_b
        ON r.snapshot_date    = b_b.snapshot_date
       AND r.sector           = b_b.sector
       AND r.market_cap_tier  = b_b.market_cap_tier
),

winsorized AS (
    SELECT
        *,
        LEAST(3, GREATEST(-3, z_roic_raw))             AS z_roic,
        LEAST(3, GREATEST(-3, z_netdebt_ebitda_raw))   AS z_netdebt_ebitda,
        LEAST(3, GREATEST(-3, z_operating_margin_raw)) AS z_operating_margin,
        LEAST(3, GREATEST(-3, z_debt_equity_raw))      AS z_debt_equity,
        (
            (CASE WHEN z_roic_raw             IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN z_netdebt_ebitda_raw   IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN z_operating_margin_raw IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN z_debt_equity_raw      IS NOT NULL THEN 1 ELSE 0 END)
        ) AS num_metrics_valid
    FROM base
),

final AS (
    SELECT
        *,
        -- Igual peso: 25% cada métrica
        (
            0.25 * COALESCE(z_roic, 0) +
            0.25 * COALESCE(z_netdebt_ebitda, 0) +
            0.25 * COALESCE(z_operating_margin, 0) +
            0.25 * COALESCE(z_debt_equity, 0)
        ) AS quality_score,

        RANK() OVER (
            ORDER BY (
                0.25 * COALESCE(z_roic, 0) +
                0.25 * COALESCE(z_netdebt_ebitda, 0) +
                0.25 * COALESCE(z_operating_margin, 0) +
                0.25 * COALESCE(z_debt_equity, 0)
            ) DESC
        ) AS quality_rank,

        PERCENT_RANK() OVER (
            ORDER BY (
                0.25 * COALESCE(z_roic, 0) +
                0.25 * COALESCE(z_netdebt_ebitda, 0) +
                0.25 * COALESCE(z_operating_margin, 0) +
                0.25 * COALESCE(z_debt_equity, 0)
            )
        ) * 100 AS quality_percentile
    FROM winsorized
)

SELECT
    ticker,
    sector,
    industry,
    market_cap_tier,
    snapshot_date,
    benchmark_level_used,
    benchmark_n_empresas,
    z_roic_raw,
    z_netdebt_ebitda_raw,
    z_operating_margin_raw,
    z_debt_equity_raw,
    z_roic,
    z_netdebt_ebitda,
    z_operating_margin,
    z_debt_equity,
    num_metrics_valid,
    quality_score,
    quality_rank,
    quality_percentile,
    now() AS created_at
FROM final

ON CONFLICT (snapshot_date, ticker) DO NOTHING;

-- Verificación
SELECT COUNT(*) AS registros_insertados
FROM modelados.quality_zscore_all_usa
WHERE snapshot_date = DATE '2026-04-01';
