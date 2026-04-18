DROP TABLE IF EXISTS ingest.keymetrics CASCADE;

CREATE TABLE IF NOT EXISTS ingest.keymetrics (
    ticker                          VARCHAR(10)     NOT NULL,
    fecha_consulta                  DATE            NOT NULL,
    periodo                         VARCHAR(10),
    fecha_reporte                   DATE,

    -- Ratios y métricas (valores pequeños)
    roic                            NUMERIC(16,4),
    roe                             NUMERIC(16,4),
    roa                             NUMERIC(16,4),
    roce                            NUMERIC(16,4),
    income_quality                  NUMERIC(16,4),
    ev_to_ebitda                    NUMERIC(16,4),
    ev_to_sales                     NUMERIC(16,4),
    ev_to_fcf                       NUMERIC(16,4),
    earnings_yield                  NUMERIC(16,4),
    fcf_yield                       NUMERIC(16,4),
    net_debt_to_ebitda              NUMERIC(16,4),

    -- Valores absolutos en dólares (pueden ser billones)
    invested_capital                NUMERIC(20,2),
    working_capital                 NUMERIC(20,2),
    market_cap                      NUMERIC(20,2),
    enterprise_value                NUMERIC(20,2),

    run_id                          VARCHAR(40),
    creado_en                       TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en                  TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, fecha_consulta)
);

CREATE INDEX IF NOT EXISTS idx_keymetrics_ticker
    ON ingest.keymetrics (ticker, fecha_consulta DESC);