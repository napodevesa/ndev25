
CREATE TABLE IF NOT EXISTS seleccion.regresiones (
    ticker              TEXT        NOT NULL,
    snapshot_date       DATE        NOT NULL,
    metrica             TEXT        NOT NULL,
    tipo_metrica        TEXT,
    frecuencia          TEXT,
    anios_calculados    NUMERIC,
    n_points            INTEGER,
    tendencia           NUMERIC,
    signo_tendencia     INTEGER,
    r2_tendencia        NUMERIC,
    ultimo_valor        NUMERIC,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    -- clave primaria compuesta
    CONSTRAINT pk_regresiones
    PRIMARY KEY (ticker, snapshot_date, metrica)
);
