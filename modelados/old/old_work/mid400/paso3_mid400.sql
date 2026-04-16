CREATE TABLE IF NOT EXISTS modelados.mid400_ranking_sp500 AS
WITH base AS (
    SELECT
        ticker,
        fecha_consulta_keymetrics,
        fecha_consulta_ratios,
        sector,
        industry,
        market_cap_tier,

        z_fcfps,
        z_roic,
        z_netdebt_ebitda,
        z_fcf_yield,
        z_cf_debt,

        -- ⭐ SCORE FINAL: promedio simple de estos 5 z-scores
        (
            COALESCE(z_fcfps, 0) +
            COALESCE(z_roic, 0) +
            COALESCE(z_netdebt_ebitda, 0) +
            COALESCE(z_fcf_yield, 0) +
            COALESCE(z_cf_debt, 0)
        ) / 5.0
        AS ranking_score
    FROM modelados.mid400_zscore_sp500
)

SELECT
    *,
    -- ⭐ ranking_position: 1 = mejor empresa
    RANK() OVER (ORDER BY ranking_score DESC) AS ranking_position
FROM base;