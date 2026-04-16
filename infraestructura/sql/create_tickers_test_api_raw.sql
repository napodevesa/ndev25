CREATE TABLE infraestructura.tickers_test_api_raw (
    ticker VARCHAR(10) PRIMARY KEY
);

INSERT INTO infraestructura.tickers_test_api_raw (ticker)
VALUES
('AAPL'),
('MSFT'),
('NVDA'),
('META'),
('TSLA')
ON CONFLICT DO NOTHING;