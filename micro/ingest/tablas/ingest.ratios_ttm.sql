DROP TABLE IF EXISTS ingest.ratios_ttm CASCADE;

CREATE TABLE IF NOT EXISTS ingest.ratios_ttm (
    ticker                                      VARCHAR(10)     NOT NULL,
    fecha_consulta                              DATE            NOT NULL,

    gross_profit_margin                         NUMERIC(16,4),
    ebit_margin                                 NUMERIC(16,4),
    ebitda_margin                               NUMERIC(16,4),
    operating_profit_margin                     NUMERIC(16,4),
    net_profit_margin                           NUMERIC(16,4),
    operating_cash_flow_sales_ratio             NUMERIC(16,4),
    free_cash_flow_operating_cash_flow_ratio    NUMERIC(16,4),
    interest_coverage_ratio                     NUMERIC(16,4),
    debt_service_coverage_ratio                 NUMERIC(16,4),
    current_ratio                               NUMERIC(16,4),
    quick_ratio                                 NUMERIC(16,4),
    debt_to_equity_ratio                        NUMERIC(16,4),
    debt_to_assets_ratio                        NUMERIC(16,4),
    long_term_debt_to_capital_ratio             NUMERIC(16,4),
    price_to_earnings_ratio                     NUMERIC(16,4),
    price_to_book_ratio                         NUMERIC(16,4),
    price_to_sales_ratio                        NUMERIC(16,4),
    price_to_free_cash_flow_ratio               NUMERIC(16,4),
    price_to_operating_cash_flow_ratio          NUMERIC(16,4),
    enterprise_value_multiple                   NUMERIC(16,4),
    dividend_yield                              NUMERIC(16,4),
    free_cash_flow_per_share                    NUMERIC(16,4),

    run_id                                      VARCHAR(40),
    creado_en                                   TIMESTAMPTZ     DEFAULT NOW(),
    actualizado_en                              TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, fecha_consulta)
);

CREATE INDEX IF NOT EXISTS idx_ratios_ticker
    ON ingest.ratios_ttm (ticker, fecha_consulta DESC);