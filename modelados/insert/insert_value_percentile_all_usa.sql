-- ============================================================
-- INSERT: modelados.value_percentile_all_usa
-- Percentile Value — 3 métricas de igual peso
-- Fecha hardcodeada: modificar según el mes
-- ============================================================

WITH params AS (
    SELECT DATE '2026-04-01' AS snapshot_date          -- << CAMBIAR CADA MES
),

base AS (
    SELECT
        p.snapshot_date,
        r.ticker,
        r.companyname,
        r.sector,
        COALESCE(r.industry, 'Unknown') AS industry,
        r.market_cap_tier,

        CASE
            WHEN b_si.n_empresas >= 5 THEN 'sector_industry_mktcap'
            ELSE 'sector_mktcap'
        END AS benchmark_level_used,

        CASE
            WHEN b_si.n_empresas >= 5 THEN b_si.n_empresas
            ELSE b_s.n_empresas
        END AS benchmark_n_empresas,

        -- PER (excluir negativos y outliers > 100)
        CASE
            WHEN r.priceearningsratiottm > 0
             AND r.priceearningsratiottm <= 100
            THEN r.priceearningsratiottm
            ELSE NULL
        END AS pe_adj,

        -- PEG (excluir negativos y outliers > 10)
        CASE
            WHEN r.priceearningstogrowthratiottm > 0
             AND r.priceearningstogrowthratiottm <= 10
            THEN r.priceearningstogrowthratiottm
            ELSE NULL
        END AS peg_adj,

        -- FCF Yield (excluir negativos)
        CASE
            WHEN r.freecashflowyieldttm > 0
            THEN r.freecashflowyieldttm
            ELSE NULL
        END AS fcfyield_adj,

        (
            (CASE WHEN r.priceearningsratiottm > 0
                   AND r.priceearningsratiottm <= 100       THEN 1 ELSE 0 END) +
            (CASE WHEN r.priceearningstogrowthratiottm > 0
                   AND r.priceearningstogrowthratiottm <= 10 THEN 1 ELSE 0 END) +
            (CASE WHEN r.freecashflowyieldttm > 0           THEN 1 ELSE 0 END)
        ) AS num_metrics_valid

    FROM procesados.value_snapshot r
    JOIN params p
        ON r.snapshot_date = p.snapshot_date

    LEFT JOIN procesados.value_benchmark_sector_industry_mktcap b_si
        ON r.sector          = b_si.sector
       AND r.industry        = b_si.industry
       AND r.market_cap_tier = b_si.market_cap_tier
       AND b_si.snapshot_date = p.snapshot_date

    LEFT JOIN procesados.value_benchmark_sector_mktcap b_s
        ON r.sector          = b_s.sector
       AND r.market_cap_tier = b_s.market_cap_tier
       AND b_s.snapshot_date = p.snapshot_date
),

percentiles AS (
    SELECT
        *,
        -- PER (menor = mejor → percentil invertido)
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

        -- PEG (menor = mejor → percentil invertido)
        CASE
            WHEN benchmark_level_used = 'sector_industry_mktcap' THEN
                PERCENT_RANK() OVER (
                    PARTITION BY sector, industry, market_cap_tier
                    ORDER BY peg_adj
                )
            ELSE
                PERCENT_RANK() OVER (
                    PARTITION BY sector, market_cap_tier
                    ORDER BY peg_adj
                )
        END AS peg_percentile,

        -- FCF Yield (mayor = mejor → percentil directo)
        CASE
            WHEN benchmark_level_used = 'sector_industry_mktcap' THEN
                PERCENT_RANK() OVER (
                    PARTITION BY sector, industry, market_cap_tier
                    ORDER BY fcfyield_adj DESC
                )
            ELSE
                PERCENT_RANK() OVER (
                    PARTITION BY sector, market_cap_tier
                    ORDER BY fcfyield_adj DESC
                )
        END AS fcfyield_percentile

    FROM base
)

INSERT INTO modelados.value_percentile_all_usa (
    snapshot_date,
    ticker,
    companyname,
    sector,
    industry,
    market_cap_tier,
    benchmark_level_used,
    benchmark_n_empresas,
    pe_adj,
    peg_adj,
    fcfyield_adj,
    pe_percentile,
    peg_percentile,
    fcfyield_percentile,
    num_metrics_valid,
    value_score,
    value_rank,
    value_percentile,
    created_at
)
SELECT
    snapshot_date,
    ticker,
    companyname,
    sector,
    industry,
    market_cap_tier,
    benchmark_level_used,
    benchmark_n_empresas,
    pe_adj,
    peg_adj,
    fcfyield_adj,
    pe_percentile,
    peg_percentile,
    fcfyield_percentile,
    num_metrics_valid,

    -- Value score: PE y PEG invertidos (menor = mejor), FCF Yield directo (mayor = mejor)
    (
        COALESCE(1 - pe_percentile, 0) +
        COALESCE(1 - peg_percentile, 0) +
        COALESCE(fcfyield_percentile, 0)
    ) / NULLIF(num_metrics_valid, 0)                AS value_score,

    RANK() OVER (
        ORDER BY (
            COALESCE(1 - pe_percentile, 0) +
            COALESCE(1 - peg_percentile, 0) +
            COALESCE(fcfyield_percentile, 0)
        ) / NULLIF(num_metrics_valid, 0) DESC
    )                                               AS value_rank,

    PERCENT_RANK() OVER (
        ORDER BY (
            COALESCE(1 - pe_percentile, 0) +
            COALESCE(1 - peg_percentile, 0) +
            COALESCE(fcfyield_percentile, 0)
        ) / NULLIF(num_metrics_valid, 0)
    ) * 100                                         AS value_percentile,

    now()                                           AS created_at

FROM percentiles

ON CONFLICT (snapshot_date, ticker) DO NOTHING;

-- Verificación
SELECT COUNT(*) AS registros_insertados
FROM modelados.value_percentile_all_usa
WHERE snapshot_date = DATE '2026-04-01';
