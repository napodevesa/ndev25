-- ============================================================
-- INSERT: multifactor.multifactor_global_mktcap (V2)
-- Incluye Metadata de Sector e Industry
-- Pesos: Quality 60% | Value 40%
-- ============================================================

INSERT INTO multifactor.multifactor_global_mktcap (
    snapshot_date,
    ticker,
    sector,
    industry,
    market_cap_tier,
    quality_percentile_global,
    value_percentile_global,
    w_quality,
    w_value,
    num_factors_valid,
    multifactor_score_global,
    multifactor_rank_global,
    multifactor_percentile_global,
    created_at
)
WITH base AS (
    SELECT
        q.snapshot_date,
        q.ticker,
        -- Traemos sector e industry del modelo de valor (all_usa)
        v_meta.sector,
        v_meta.industry,
        q.market_cap_tier,
        q.quality_percentile_global,
        v.value_percentile_global,
        0.60 AS w_quality,
        0.40 AS w_value,
        (
            CASE WHEN q.quality_percentile_global IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.value_percentile_global    IS NOT NULL THEN 1 ELSE 0 END
        ) AS num_factors_valid
    FROM modelados.quality_zscore_global q
    -- Join para obtener los percentiles de valor globales
    LEFT JOIN modelados.value_percentile_global v
        ON q.ticker        = v.ticker
       AND q.snapshot_date = v.snapshot_date
    -- Join para obtener la metadata (Sector/Industry)
    LEFT JOIN modelados.value_percentile_all_usa v_meta
        ON q.ticker        = v_meta.ticker
       AND q.snapshot_date = v_meta.snapshot_date
    WHERE q.snapshot_date = DATE '2026-04-01'
)
SELECT
    snapshot_date,
    ticker,
    COALESCE(sector, 'Unknown') AS sector,
    COALESCE(industry, 'Unknown') AS industry,
    market_cap_tier,
    quality_percentile_global,
    value_percentile_global,
    w_quality,
    w_value,
    num_factors_valid,

    (COALESCE(quality_percentile_global, 0) * w_quality + 
     COALESCE(value_percentile_global, 0) * w_value) AS multifactor_score_global,

    RANK() OVER (
        ORDER BY (COALESCE(quality_percentile_global, 0) * w_quality + 
                  COALESCE(value_percentile_global, 0) * w_value) DESC
    ) AS multifactor_rank_global,

    PERCENT_RANK() OVER (
        ORDER BY (COALESCE(quality_percentile_global, 0) * w_quality + 
                  COALESCE(value_percentile_global, 0) * w_value) ASC
    ) * 100 AS multifactor_percentile_global,

    now() AS created_at
FROM base
ON CONFLICT (snapshot_date, ticker) DO NOTHING;