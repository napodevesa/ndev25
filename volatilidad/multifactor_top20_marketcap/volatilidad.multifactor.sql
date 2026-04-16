CREATE TABLE IF NOT EXISTS volatilidad.multifactor AS
SELECT
    ticker,
    sector,
    industry,
    market_cap_tier,
    CURRENT_DATE AS fecha,

    quality_percentile,
    value_percentile,
    momentum_percentile,
    num_factors_valid,

    multifactor_rank,
    multifactor_percentile AS multifactor_percentile_global,

    -- Percentil recalculado por market cap (0–100)
    ROUND(
        (
            (1 - PERCENT_RANK() OVER (
                PARTITION BY market_cap_tier
                ORDER BY multifactor_score_adjusted DESC
            )) * 100
        )::NUMERIC
    , 2) AS multifactor_percentile_mktcap

FROM estudiosfactores.multifactor_all_usa
WHERE market_cap_tier IN ('mega', 'large', 'mid')
  AND num_factors_valid = 3;
