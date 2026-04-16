CREATE TABLE selection.options_execution_candidates (
    ticker TEXT,
    opcion TEXT,
    contract_type TEXT,
    strike NUMERIC,
    vto DATE,
    dte INTEGER,

    delta NUMERIC,
    gamma NUMERIC,
    theta NUMERIC,
    vega NUMERIC,

    iv NUMERIC,
    oi INTEGER,
    volume INTEGER,

    vwap NUMERIC,
    close_price NUMERIC,

    fecha DATE,
    created_at TIMESTAMP DEFAULT now()
);
