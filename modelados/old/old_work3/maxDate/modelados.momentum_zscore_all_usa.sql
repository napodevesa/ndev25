CREATE TABLE IF NOT EXISTS modelados.momentum_zscore_all_usa AS

WITH snapshot AS (
    SELECT
        MAX(fecha_de_consulta_momentum)       AS f_momentum,
        MAX(fecha_de_consulta_keymetrics_ttm) AS f_keymetrics
    FROM procesados.momentum_all_usa
),

base AS (
    SELECT
        m.ticker,
        m.sector,
        m.industry,
        m.market_cap_tier,

        m.fecha_de_consulta_momentum,
        m.fecha_de_consulta_keymetrics_ttm,

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

        /* ============================
           Métricas válidas
        ============================ */
        CASE
            WHEN m.momentum_12m_1m IS NOT NULL THEN 1
            ELSE 0
        END AS num_metrics_valid

    FROM procesados.momentum_all_usa m
    JOIN snapshot s
      ON m.fecha_de_consulta_momentum       = s.f_momentum
     AND m.fecha_de_consulta_keymetrics_ttm = s.f_keymetrics

    /* ============================
       Benchmarks (misma fecha)
    ============================ */

    LEFT JOIN procesados.momentum_benchmark_sector_industry_mktcap b_a
      ON m.sector = b_a.sector
     AND m.industry = b_a.industry
     AND m.market_cap_tier = b_a.market_cap_tier
     AND b_a.fecha_de_consulta_momentum       = s.f_momentum
     AND b_a.fecha_de_consulta_keymetrics_ttm = s.f_keymetrics

    LEFT JOIN procesados.momentum_benchmark_sector_mktcap b_b
      ON m.sector = b_b.sector
     AND m.market_cap_tier = b_b.market_cap_tier
     AND b_b.fecha_de_consulta_momentum       = s.f_momentum
     AND b_b.fecha_de_consulta_keymetrics_ttm = s.f_keymetrics
),

winsorized AS (
    SELECT
        *,
        /* Limitar z-score a ±3 */
        CASE
            WHEN z_momentum_12m_1m IS NOT NULL
            THEN LEAST(3, GREATEST(-3, z_momentum_12m_1m))
            ELSE NULL
        END AS z_momentum_12m_1m_winsor
    FROM base
)

SELECT
    *,
    /* ============================
       Score final
    ============================ */
    z_momentum_12m_1m_winsor AS momentum_score,

    RANK() OVER (
        ORDER BY z_momentum_12m_1m_winsor DESC
    ) AS momentum_rank,

    PERCENT_RANK() OVER (
        ORDER BY z_momentum_12m_1m_winsor DESC
    ) * 100 AS momentum_percentile

FROM winsorized;
