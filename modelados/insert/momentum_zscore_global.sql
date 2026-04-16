INSERT INTO modelados.momentum_zscore_global (
    ticker,
    market_cap_tier,
    snapshot_date,
    benchmark_level_used,
    benchmark_n_empresas,
    z_momentum_12m_1m_raw,
    z_momentum_12m_1m_winsor,
    num_metrics_valid,
    momentum_score_global,
    momentum_rank_global,
    momentum_percentile_global,
    created_at
)
WITH params AS (
    SELECT DATE '2026-04-01' AS snapshot_date
),

base AS (
    SELECT
        m.ticker,
        m.market_cap_tier,
        m.snapshot_date,
        'global_mktcap' AS benchmark_level_used,
        b.n_empresas AS benchmark_n_empresas,

        /* ============================
           Z-SCORE MOMENTUM GLOBAL
           Comparado contra su Market Cap Tier
        ============================ */
        CASE 
            WHEN m.momentum_12m_1m IS NOT NULL THEN
                (m.momentum_12m_1m - b.media_momentum) / NULLIF(b.desvio_momentum, 0)
            ELSE NULL
        END AS z_momentum_12m_1m_raw,

        CASE 
            WHEN m.momentum_12m_1m IS NOT NULL THEN 1 
            ELSE 0 
        END AS num_metrics_valid

    FROM procesados.momentum_snapshot m
    JOIN params p ON m.snapshot_date = p.snapshot_date
    -- JOIN directo al benchmark global por market_cap_tier
    INNER JOIN procesados.momentum_benchmark_global_mktcap b
      ON m.snapshot_date = b.snapshot_date
     AND m.market_cap_tier = b.market_cap_tier
),

winsorized AS (
    SELECT 
        *,
        CASE 
            WHEN z_momentum_12m_1m_raw IS NOT NULL 
            THEN LEAST(3, GREATEST(-3, z_momentum_12m_1m_raw))
            ELSE NULL 
        END AS z_momentum_12m_1m_winsor
    FROM base
),

final AS (
    SELECT 
        *,
        -- El score es el Z-Score winsorizado
        z_momentum_12m_1m_winsor AS momentum_score_global,

        -- Ranking Global (Dentro de todo el mercado)
        RANK() OVER (
            ORDER BY z_momentum_12m_1m_winsor DESC
        ) AS momentum_rank_global,

        -- Percentil Global
        PERCENT_RANK() OVER (
            ORDER BY z_momentum_12m_1m_winsor DESC
        ) * 100 AS momentum_percentile_global
    FROM winsorized
)

SELECT 
    ticker,
    market_cap_tier,
    snapshot_date,
    benchmark_level_used,
    benchmark_n_empresas,
    z_momentum_12m_1m_raw,
    z_momentum_12m_1m_winsor,
    num_metrics_valid,
    momentum_score_global,
    momentum_rank_global,
    momentum_percentile_global,
    NOW() AS created_at
FROM final
-- Evitamos duplicados si corremos el script dos veces
ON CONFLICT (ticker, snapshot_date) DO NOTHING;