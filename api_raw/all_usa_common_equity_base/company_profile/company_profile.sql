CREATE TABLE IF NOT EXISTS api_raw.company_profile (
    ticker               VARCHAR(20)  NOT NULL,
    companyname          VARCHAR(255),
    cik                  VARCHAR(20),
    exchange_short_name  VARCHAR(50),
    sector               VARCHAR(100),
    industry             VARCHAR(150),
    country              VARCHAR(50),
    is_adr               BOOLEAN,
    is_actively_trading  BOOLEAN,

    -- Auditoría / versionado
    fecha                DATE NOT NULL,
    source               VARCHAR(50) NOT NULL DEFAULT 'FMP',
    created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, fecha)
);
