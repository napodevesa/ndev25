CREATE TABLE IF NOT EXISTS volatilidad.salud_ttm (
    ticker                          VARCHAR PRIMARY KEY,
    sector                          VARCHAR,
    industry                        VARCHAR,
    market_cap_tier                 VARCHAR,
    fecha                           DATE,

    roic_ttm                        NUMERIC,
    net_debt_to_ebitda_ttm          NUMERIC,
    debt_equity_ratio_ttm           NUMERIC,
    free_cash_flow_per_share_ttm    NUMERIC
);


INSERT INTO volatilidad.salud_ttm (
    ticker,
    sector,
    industry,
    market_cap_tier,
    fecha,
    roic_ttm,
    net_debt_to_ebitda_ttm,
    debt_equity_ratio_ttm,
    free_cash_flow_per_share_ttm
)
SELECT
    m.ticker,
    m.sector,
    m.industry,
    m.market_cap_tier,
    CURRENT_DATE                                        AS fecha,

    km.roicttm                                          AS roic_ttm,
    km.netdebttoebitdattm                               AS net_debt_to_ebitda_ttm,
    r.debtequityratiottm                                AS debt_equity_ratio_ttm,
    r.freecashflowpersharettm                           AS free_cash_flow_per_share_ttm

FROM estudiosfactores.multifactor_all_usa m

LEFT JOIN (
    SELECT *
    FROM limpieza.multifactor_keymetrics_ttm_limpios
    WHERE fecha_de_consulta = (
        SELECT MAX(fecha_de_consulta)
        FROM limpieza.multifactor_keymetrics_ttm_limpios
    )
) km
    ON m.ticker = km.ticker

LEFT JOIN (
    SELECT *
    FROM limpieza.multifactor_ratios_ttm_limpios
    WHERE fecha_de_consulta = (
        SELECT MAX(fecha_de_consulta)
        FROM limpieza.multifactor_ratios_ttm_limpios
    )
) r
    ON m.ticker = r.ticker

WHERE
    km.roicttm > 0.10
    AND km.netdebttoebitdattm < 3
    AND r.debtequityratiottm < 1
    AND r.freecashflowpersharettm > 0;
