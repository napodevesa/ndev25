INSERT INTO modelados.momentum_zscore_all_usa (
    ticker,
    sector,
    industry,
    market_cap_tier,
    snapshot_date,
    benchmark_level_used,
    benchmark_n_empresas,
    z_momentum_12m_1m,
    num_metrics_valid,
    z_momentum_12m_1m_winsor,
    momentum_score,
    momentum_rank,
    momentum_percentile,
    created_at
)

WITH params AS (
    SELECT DATE '2026-04-01' AS snapshot_date
),

base AS (
    SELECT
        m.ticker,
        m.sector,
        m.industry,
        m.market_cap_tier,
        m.snapshot_date,

        /* ============================
           Benchmark utilizado
        ============================ */
        CASE
            WHEN b_a.n_empresas >= 15 THEN 'sector_industry_mktcap'
            ELSE 'sector_mktcap'
        END AS benchmark_level_used,

        CASE
            WHEN b_a.n_empresas >= 15 THEN b_a.n_empresas
            ELSE b_b.n_empresas
        END AS benchmark_n_empresas,

        /* ============================
           Z-SCORE MOMENTUM (12m–1m)
        ============================ */
        CASE
            WHEN m.momentum_12m_1m IS NOT NULL THEN
                (
                    m.momentum_12m_1m -
                    CASE
                        WHEN b_a.n_empresas >= 15 THEN b_a.media_momentum
                        ELSE b_b.media_momentum
                    END
                ) / NULLIF(
                    CASE
                        WHEN b_a.n_empresas >= 15 THEN b_a.desvio_momentum
                        ELSE b_b.desvio_momentum
                    END,
                    0
                )
            ELSE NULL
        END AS z_momentum_12m_1m,

        CASE
            WHEN m.momentum_12m_1m IS NOT NULL THEN 1
            ELSE 0
        END AS num_metrics_valid

    FROM procesados.momentum_snapshot m
    JOIN params p
      ON m.snapshot_date = p.snapshot_date

    LEFT JOIN procesados.momentum_benchmark_sector_industry_mktcap b_a
      ON m.snapshot_date = b_a.snapshot_date
     AND m.sector = b_a.sector
     AND m.industry = b_a.industry
     AND m.market_cap_tier = b_a.market_cap_tier

    LEFT JOIN procesados.momentum_benchmark_sector_mktcap b_b
      ON m.snapshot_date = b_b.snapshot_date
     AND m.sector = b_b.sector
     AND m.market_cap_tier = b_b.market_cap_tier
),

winsorized AS (
    SELECT
        *,
        CASE
            WHEN z_momentum_12m_1m IS NOT NULL
            THEN LEAST(3, GREATEST(-3, z_momentum_12m_1m))
            ELSE NULL
        END AS z_momentum_12m_1m_winsor
    FROM base
),

final AS (
    SELECT
        *,
        z_momentum_12m_1m_winsor AS momentum_score,

        RANK() OVER (
            ORDER BY z_momentum_12m_1m_winsor DESC
        ) AS momentum_rank,

        PERCENT_RANK() OVER (
            ORDER BY z_momentum_12m_1m_winsor DESC
        ) * 100 AS momentum_percentile
    FROM winsorized
)

SELECT
    ticker,
    sector,
    COALESCE(industry, 'Unknown') AS industry,
    market_cap_tier,
    snapshot_date,
    benchmark_level_used,
    benchmark_n_empresas,
    z_momentum_12m_1m,
    num_metrics_valid,
    z_momentum_12m_1m_winsor,
    momentum_score,
    momentum_rank,
    momentum_percentile,
    NOW() AS created_at
FROM final

ON CONFLICT (snapshot_date, ticker) DO NOTHING;

