CREATE TABLE seleccion.regresiones_snapshot (
    ticker TEXT NOT NULL,
    snapshot_date DATE NOT NULL,

    -- ============================
    -- METRICAS – TENDENCIAS (SEÑALES)
    -- ============================
    roic_trend INTEGER,
    net_debt_ebitda_trend INTEGER,

    created_at TIMESTAMP DEFAULT NOW(),

    -- PK compuesta por snapshot
    CONSTRAINT pk_regresiones_snapshot
        PRIMARY KEY (ticker, snapshot_date)
);


