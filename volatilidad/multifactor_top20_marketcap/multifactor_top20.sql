CREATE TABLE volatilidad.multifactor_top20 AS
SELECT *
FROM volatilidad.multifactor
WHERE fecha = CURRENT_DATE
  AND multifactor_percentile_mktcap >= 80
ORDER BY market_cap_tier, multifactor_percentile_mktcap DESC;