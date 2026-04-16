
CREATE TABLE IF NOT EXISTS agente_opciones.contratos_raw (
    -- Identificación
    ticker          VARCHAR(20)    NOT NULL,
    opcion          VARCHAR(50)    NOT NULL,
    contract_type   VARCHAR(10)    NOT NULL,
    -- Contrato
    strike          NUMERIC(10,2),
    vto             DATE,
    dte             INT,
    -- Greeks
    delta           NUMERIC(8,4),
    gamma           NUMERIC(8,4),
    theta           NUMERIC(8,4),
    vega            NUMERIC(8,4),
    -- Volatilidad
    iv              NUMERIC(8,4),
    -- Liquidez
    oi              INT,
    volume          INT,
    vwap            NUMERIC(10,4),
    close_price     NUMERIC(10,4),
    -- Trazabilidad
    fecha           DATE           NOT NULL,
    PRIMARY KEY (opcion, fecha)
);
