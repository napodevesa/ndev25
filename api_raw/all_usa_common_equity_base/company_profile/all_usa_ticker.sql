CREATE TABLE IF NOT EXISTS api_raw.all_usa_ticker (
    ticker        VARCHAR(20)  NOT NULL,
    exchange      VARCHAR(50),
    fecha         DATE         NOT NULL,
    source        VARCHAR(20)  NOT NULL DEFAULT 'FMP',
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (ticker, fecha)
);
INSERT INTO api_raw.all_usa_ticker (
    ticker,
    exchange,
    fecha,
    source
)
SELECT DISTINCT
    ticker,
    exchange_short_name AS exchange,
    fecha,
    source
FROM api_raw.all_usa_list
WHERE type = 'stock'
ON CONFLICT (ticker, fecha) DO NOTHING;