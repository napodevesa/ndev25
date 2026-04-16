CREATE TABLE IF NOT EXISTS modelados.quality_ranking_all_usa AS
SELECT
    *,
    -- ⭐ Ranking absoluto (1 = mejor)
    RANK() OVER (ORDER BY quality_score DESC) AS quality_rank,

    -- ⭐ Percentil (0–100)
    PERCENT_RANK() OVER (ORDER BY quality_score) * 100 AS quality_percentile

FROM modelados.quality_zscore_all_usa;