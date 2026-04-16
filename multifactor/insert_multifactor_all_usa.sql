-- ============================================================
-- INSERT: multifactor.multifactor_sector_industry
-- Pesos: Quality 60% | Value 40%
-- Enfoque: Relativo a Sector e Industria (Micro)
-- ============================================================

INSERT INTO multifactor.multifactor_sector_industry (
    snapshot_date,
    ticker,
    sector,
    industry,
    market_cap_tier,
    quality_percentile,
    value_percentile,
    w_quality,
    w_value,
    num_factors_valid,
    multifactor_score_adjusted,
    multifactor_rank,
    multifactor_percentile,
    created_at
)
WITH base AS (
    SELECT
        q.snapshot_date,
        q.ticker,
        q.sector,
        q.industry,
        q.market_cap_tier,
        q.quality_percentile,
        v.value_percentile,
        0.60 AS w_quality,
        0.40 AS w_value,
        (
            CASE WHEN q.quality_percentile IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN v.value_percentile   IS NOT NULL THEN 1 ELSE 0 END
        ) AS num_factors_valid
    FROM modelados.quality_zscore_all_usa q
    LEFT JOIN modelados.value_percentile_all_usa v
        ON q.ticker        = v.ticker
       AND v.snapshot_date = q.snapshot_date
    WHERE q.snapshot_date = DATE '2026-04-01' -- << CAMBIAR CADA MES
)
SELECT
    snapshot_date,
    ticker,
    sector,
    industry,
    market_cap_tier,
    quality_percentile,
    value_percentile,
    w_quality,
    w_value,
    num_factors_valid,

    -- Score: Suma ponderada de Calidad y Valor
    (
        COALESCE(quality_percentile, 0) * w_quality +
        COALESCE(value_percentile,   0) * w_value
    ) AS multifactor_score_adjusted,

    -- Rank Sectorial/Industria
    RANK() OVER (
        ORDER BY (
            COALESCE(quality_percentile, 0) * w_quality +
            COALESCE(value_percentile,   0) * w_value
        ) DESC
    ) AS multifactor_rank,

    -- Percentil Final
    PERCENT_RANK() OVER (
        ORDER BY (
            COALESCE(quality_percentile, 0) * w_quality +
            COALESCE(value_percentile,   0) * w_value
        )
    ) * 100 AS multifactor_percentile,

    now() AS created_at

FROM base
ON CONFLICT (snapshot_date, ticker) DO NOTHING;

-- Verificación
SELECT 
    sector, 
    COUNT(*) AS empresas, 
    ROUND(AVG(multifactor_score_adjusted)::numeric, 4) as score_promedio
FROM multifactor.multifactor_sector_industry
WHERE snapshot_date = DATE '2026-04-01'
GROUP BY sector
ORDER BY score_promedio DESC;