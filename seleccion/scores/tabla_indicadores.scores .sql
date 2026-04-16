CREATE TABLE IF NOT EXISTS seleccion.scores (
    ticker TEXT NOT NULL,
    snapshot_date DATE NOT NULL,

    altman_z_score NUMERIC,
    piotroski_score INTEGER,

    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    PRIMARY KEY (ticker, snapshot_date)
);