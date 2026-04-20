DROP TABLE IF EXISTS agente_opciones.contratos_raw CASCADE;
DROP TABLE IF EXISTS agente_opciones.trade_decision_opciones CASCADE;

CREATE TABLE IF NOT EXISTS agente_opciones.contratos_raw (
    ticker          VARCHAR(10)     NOT NULL,
    opcion          VARCHAR(50)     NOT NULL,
    snapshot_date   DATE            NOT NULL,   -- nuevo
    fecha           DATE            NOT NULL,
    contract_type   VARCHAR(10)     NOT NULL,
    strike          NUMERIC(10,2),
    vto             DATE,
    dte             INTEGER,
    delta           NUMERIC(8,4),
    gamma           NUMERIC(8,4),
    theta           NUMERIC(8,4),
    vega            NUMERIC(8,4),
    iv              NUMERIC(8,4),
    oi              INTEGER,
    volume          INTEGER,
    vwap            NUMERIC(10,4),
    close_price     NUMERIC(10,4),
    run_id          VARCHAR(40),
    creado_en       TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (opcion, fecha)
);

CREATE INDEX IF NOT EXISTS idx_contratos_ticker_fecha
    ON agente_opciones.contratos_raw (ticker, fecha DESC);
CREATE INDEX IF NOT EXISTS idx_contratos_snapshot
    ON agente_opciones.contratos_raw (snapshot_date, ticker);