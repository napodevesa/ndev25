-- ============================================================
-- INSERT: modelados.value_percentile_global
-- Versión Corregida: Sin columna companyname
-- ============================================================

WITH params AS (
    SELECT DATE '2026-04-01' AS snapshot_date
),

base AS (
    SELECT
        p.snapshot_date,
        r.ticker,
        r.market_cap_tier,
        'global_mktcap' AS benchmark_level_used,
        b.n_empresas AS benchmark_n_empresas,

        -- PER (Ajuste: excluir negativos y > 100)
        CASE 
            WHEN r.priceearningsratiottm > 0 AND r.priceearningsratiottm <= 100 
            THEN r.priceearningsratiottm ELSE NULL 
        END AS pe_adj,

        -- PEG (Ajuste: excluir negativos y > 10)
        CASE 
            WHEN r.priceearningstogrowthratiottm > 0 AND r.priceearningstogrowthratiottm <= 10 
            THEN r.priceearningstogrowthratiottm ELSE NULL 
        END AS peg_adj,

        -- FCF Yield (Ajuste: excluir negativos)
        CASE 
            WHEN r.freecashflowyieldttm > 0 
            THEN r.freecashflowyieldttm ELSE NULL 
        END AS fcfyield_adj,

        (
            (CASE WHEN r.priceearningsratiottm > 0 AND r.priceearningsratiottm <= 100 THEN 1 ELSE 0 END) +
            (CASE WHEN r.priceearningstogrowthratiottm > 0 AND r.priceearningstogrowthratiottm <= 10 THEN 1 ELSE 0 END) +
            (CASE WHEN r.freecashflowyieldttm > 0 THEN 1 ELSE 0 END)
        ) AS num_metrics_valid

    FROM procesados.value_snapshot r
    JOIN params p ON r.snapshot_date = p.snapshot_date
    INNER JOIN procesados.value_benchmark_global_mktcap b
        ON r.snapshot_date = b.snapshot_date
       AND r.market_cap_tier = b.market_cap_tier
),

percentiles_global AS (
    SELECT
        *,
        -- Ranking relativo al Market Cap Tier
        PERCENT_RANK() OVER (PARTITION BY market_cap_tier ORDER BY pe_adj ASC) AS pe_p_rank,
        PERCENT_RANK() OVER (PARTITION BY market_cap_tier ORDER BY peg_adj ASC) AS peg_p_rank,
        PERCENT_RANK() OVER (PARTITION BY market_cap_tier ORDER BY fcfyield_adj DESC) AS fcfyield_p_rank
    FROM base
)

INSERT INTO modelados.value_percentile_global (
    snapshot_date,
    ticker,
    market_cap_tier,
    benchmark_level_used,
    benchmark_n_empresas,
    pe_adj,
    peg_adj,
    fcfyield_adj,
    pe_percentile_global,
    peg_percentile_global,
    fcfyield_percentile_global,
    num_metrics_valid,
    value_score_global,
    value_rank_global,
    value_percentile_global,
    created_at
)
SELECT
    snapshot_date,
    ticker,
    market_cap_tier,
    benchmark_level_used,
    benchmark_n_empresas,
    pe_adj,
    peg_adj,
    fcfyield_adj,
    (1 - pe_p_rank) AS pe_percentile_global,
    (1 - peg_p_rank) AS peg_percentile_global,
    fcfyield_p_rank AS fcfyield_percentile_global,
    num_metrics_valid,

    -- Score Global
    (
        COALESCE(1 - pe_p_rank, 0) + 
        COALESCE(1 - peg_p_rank, 0) + 
        COALESCE(fcfyield_p_rank, 0)
    ) / NULLIF(num_metrics_valid, 0) AS value_score_global,

    -- Rank Final
    RANK() OVER (ORDER BY (COALESCE(1 - pe_p_rank, 0) + COALESCE(1 - peg_p_rank, 0) + COALESCE(fcfyield_p_rank, 0)) / NULLIF(num_metrics_valid, 0) DESC) AS value_rank_global,

    -- Percentil Final
    PERCENT_RANK() OVER (ORDER BY (COALESCE(1 - pe_p_rank, 0) + COALESCE(1 - peg_p_rank, 0) + COALESCE(fcfyield_p_rank, 0)) / NULLIF(num_metrics_valid, 0) ASC) * 100 AS value_percentile_global,

    now() AS created_at
FROM percentiles_global
ON CONFLICT (snapshot_date, ticker) DO NOTHING;