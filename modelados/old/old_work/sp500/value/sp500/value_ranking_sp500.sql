CREATE TABLE IF NOT EXISTS modelados.value_ranking_sp500 AS
WITH base AS (
    SELECT
        ticker,
        sector,
        industry,
        market_cap_tier,

        -- Z-scores individuales
        z_pe,
        z_ps,
        z_pfcf,

        -- ============================
        -- ⭐ SCORE FINAL VALUE
        -- ============================
        (
            COALESCE(z_pe, 0) +
            COALESCE(z_ps, 0) +
            COALESCE(z_pfcf, 0)
        ) / 3.0 AS ranking_score

    FROM modelados.value_zscore_sp500
)

SELECT
    *,
    -- ============================
    -- ⭐ POSICIÓN DE RANKING
    -- ============================
    RANK() OVER (ORDER BY ranking_score DESC) AS ranking_position

FROM base;
