CREATE TABLE IF NOT EXISTS api_raw.all_usa_list (
    ticker               VARCHAR(20) NOT NULL,
    name_stock           VARCHAR(255),
    exchange_short_name  VARCHAR(20),
    type                 VARCHAR(50),
    fecha                DATE NOT NULL,
    source               VARCHAR(50) NOT NULL DEFAULT 'FMP',
    created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, fecha)
);
