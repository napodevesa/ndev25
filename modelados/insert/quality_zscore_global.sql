-- ============================================================
-- INSERT: modelados.quality_zscore_global
-- Z-Score Quality GLOBAL — 4 métricas (25% c/u) vs Market Cap Tier
-- ============================================================

INSERT INTO modelados.quality_zscore_global (
    ticker,
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
    quality_score_global,
    quality_rank_global,
    quality_percentile_global,
    created_at
)

WITH params AS (
    SELECT DATE '2026-04-01' AS snapshot_date
),

base AS (
    SELECT
        r.ticker,
        r.market_cap_tier,
        r.snapshot_date,
        'global_mktcap' AS benchmark_level_used,
        b.n_empresas AS benchmark_n_empresas,

        -- ROIC (Z-Score Global)
        (r.roicttm - b.media_roic) / NULLIF(b.desvio_roic, 0) AS z_roic_raw,

        -- Net Debt / EBITDA (Invertido: menos es mejor)
        -1 * (r.netdebttoebitdattm - b.media_ndebt_ebitda) / NULLIF(b.desvio_ndebt_ebitda, 0) AS z_netdebt_ebitda_raw,

        -- Operating Margin (Z-Score Global)
        (r.operatingprofitmarginttm - b.media_operating_margin) / NULLIF(b.desvio_operating_margin, 0) AS z_operating_margin_raw,

        -- Debt / Equity (Invertido: menos es mejor)
        -1 * (r.debtequityratiottm - b.media_debt_equity) / NULLIF(b.desvio_debt_equity, 0) AS z_debt_equity_raw

    FROM procesados.quality_snapshot r
    JOIN params p ON r.snapshot_date = p.snapshot_date
    INNER JOIN procesados.quality_benchmark_global_mktcap b
        ON r.snapshot_date    = b.snapshot_date
       AND r.market_cap_tier  = b.market_cap_tier
),

winsorized AS (
    SELECT
        *,
        LEAST(3, GREATEST(-3, z_roic_raw))              AS z_roic,
        LEAST(3, GREATEST(-3, z_netdebt_ebitda_raw))    AS z_netdebt_ebitda,
        LEAST(3, GREATEST(-3, z_operating_margin_raw))  AS z_operating_margin,
        LEAST(3, GREATEST(-3, z_debt_equity_raw))       AS z_debt_equity,
        (
            (CASE WHEN z_roic_raw              IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN z_netdebt_ebitda_raw    IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN z_operating_margin_raw  IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN z_debt_equity_raw       IS NOT NULL THEN 1 ELSE 0 END)
        ) AS num_metrics_valid
    FROM base
),

final AS (
    SELECT
        *,
        -- Score Global (Igual peso 25%)
        (
            0.25 * COALESCE(z_roic, 0) +
            0.25 * COALESCE(z_netdebt_ebitda, 0) +
            0.25 * COALESCE(z_operating_margin, 0) +
            0.25 * COALESCE(z_debt_equity, 0)
        ) AS quality_score_global,

        RANK() OVER (
            ORDER BY (
                0.25 * COALESCE(z_roic, 0) +
                0.25 * COALESCE(z_netdebt_ebitda, 0) +
                0.25 * COALESCE(z_operating_margin, 0) +
                0.25 * COALESCE(z_debt_equity, 0)
            ) DESC
        ) AS quality_rank_global,

        PERCENT_RANK() OVER (
            ORDER BY (
                0.25 * COALESCE(z_roic, 0) +
                0.25 * COALESCE(z_netdebt_ebitda, 0) +
                0.25 * COALESCE(z_operating_margin, 0) +
                0.25 * COALESCE(z_debt_equity, 0)
            )
        ) * 100 AS quality_percentile_global
    FROM winsorized
)

SELECT
    ticker,
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
    quality_score_global,
    quality_rank_global,
    quality_percentile_global,
    now() AS created_at
FROM final
ON CONFLICT (snapshot_date, ticker) DO NOTHING;