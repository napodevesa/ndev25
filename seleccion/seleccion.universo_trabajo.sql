CREATE TABLE seleccion.universo_trabajo (
    ticker TEXT,
    snapshot_date DATE,
    motivo_inclusion TEXT,

    roicttm NUMERIC,
    netdebttoebitdattm NUMERIC,
    debtequityratiottm NUMERIC,
    freecashflowpersharettm NUMERIC,

    PRIMARY KEY (ticker, snapshot_date)
);


INSERT INTO seleccion.universo_trabajo (
    ticker,
    snapshot_date,
    motivo_inclusion,
    roicttm,
    netdebttoebitdattm,
    debtequityratiottm,
    freecashflowpersharettm
)
SELECT
    km.ticker,
    DATE '2026-04-01' AS snapshot_date,
    'salud_financiera' AS motivo_inclusion,

    km.roicttm,
    km.netdebttoebitdattm,
    r.debtequityratiottm,
    r.freecashflowpersharettm

FROM limpieza.multifactor_keymetrics_ttm_limpios km
JOIN limpieza.multifactor_ratios_ttm_limpios r
    ON km.ticker = r.ticker
    AND km.fecha_de_consulta = r.fecha_de_consulta

WHERE
    km.fecha_de_consulta = DATE '2026-04-07'
    AND r.fecha_de_consulta = DATE '2026-04-07'

    AND km.roicttm > 0.04
    AND km.netdebttoebitdattm < 3
    AND r.debtequityratiottm < 0.8
    AND r.freecashflowpersharettm > 0;
