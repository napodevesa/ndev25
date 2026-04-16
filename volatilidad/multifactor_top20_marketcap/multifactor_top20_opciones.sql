CREATE TABLE IF NOT EXISTS volatilidad.multifactor_top20_opciones (
    ticker VARCHAR,          -- ADBE
    opcion VARCHAR,          -- O:ADBE260123C00220000

    contract_type VARCHAR,   -- call / put
    strike NUMERIC,
    vto DATE,
    dte INT,

    -- Greeks
    delta NUMERIC,
    gamma NUMERIC,
    theta NUMERIC,
    vega NUMERIC,

    -- Volatilidad
    iv NUMERIC,

    -- Liquidez
    oi INT,
    volume INT,
    vwap NUMERIC,
    close_price NUMERIC,

    fecha DATE               -- snapshot
);