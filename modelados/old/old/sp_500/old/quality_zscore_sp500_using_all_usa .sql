CREATE TABLE IF NOT EXISTS modelados.quality_zscore_sp500 AS
SELECT
    q.*
FROM modelados.quality_zscore_all_usa q
INNER JOIN universos.sp500 u
    ON q.ticker = u.ticker;
