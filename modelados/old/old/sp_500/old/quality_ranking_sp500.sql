CREATE TABLE IF NOT EXISTS modelados.quality_ranking_sp500 AS
SELECT
    *,
    RANK() OVER (ORDER BY quality_score DESC) AS quality_rank,
    PERCENT_RANK() OVER (ORDER BY quality_score) * 100 AS quality_percentile
FROM modelados.quality_zscore_sp500;
