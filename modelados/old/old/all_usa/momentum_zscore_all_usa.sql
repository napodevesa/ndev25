CREATE TABLE IF NOT EXISTS modelados.momentum_zscore_all_usa AS
WITH base AS (
    SELECT
        m.ticker,
        m.sector,
        m.industry,
        m.market_cap_tier,
        m.fecha_consulta_keymetrics,
        m.fecha_consulta_momentum,

        -- ============================
        -- Benchmark usado
        -- ============================
        CASE
            WHEN b_a.n_empresas >= 15 THEN 'sector_industry_mktcap'
            ELSE 'sector_mktcap'
        END AS benchmark_level_used,

        CASE
            WHEN b_a.n_empresas >= 15 THEN b_a.n_empresas
            ELSE b_b.n_empresas
        END AS benchmark_n_empresas,

        -- ============================
        -- Z-SCORE MOMENTUM (12M-1M)
        -- ============================
        (
            m.momentum_12m_1m -
            CASE
                WHEN b_a.n_empresas >= 15 THEN b_a.mean_momentum_12m_1m
                ELSE b_b.mean_momentum_12m_1m
            END
        ) / NULLIF(
            CASE
                WHEN b_a.n_empresas >= 15 THEN b_a.std_momentum_12m_1m
                ELSE b_b.std_momentum_12m_1m
            END,
            0
        ) AS z_momentum_12m_1m

    FROM procesados.momentum_all_usa m

    LEFT JOIN procesados.momentum_benchmark_sector_industry_mktcap b_a
        ON m.sector = b_a.sector
       AND m.industry = b_a.industry
       AND m.market_cap_tier = b_a.market_cap_tier

    LEFT JOIN procesados.momentum_benchmark_sector_mktcap b_b
        ON m.sector = b_b.sector
       AND m.market_cap_tier = b_b.market_cap_tier
)

SELECT
    *,
    -- Score final (simple, sin magia)
    z_momentum_12m_1m AS momentum_score,

    RANK() OVER (
        ORDER BY z_momentum_12m_1m DESC
    ) AS momentum_rank,

    PERCENT_RANK() OVER (
        ORDER BY z_momentum_12m_1m
    ) * 100 AS momentum_percentile

FROM base;

