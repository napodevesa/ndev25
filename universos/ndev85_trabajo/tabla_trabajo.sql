CREATE TABLE universos.universo_trabajo (
    ticker TEXT,
    snapshot_date DATE,
    motivo_inclusion TEXT,

    roicttm NUMERIC,
    netdebttoebitdattm NUMERIC,
    debtequityratiottm NUMERIC,
    freecashflowpersharettm NUMERIC,

    PRIMARY KEY (ticker, snapshot_date)
);
