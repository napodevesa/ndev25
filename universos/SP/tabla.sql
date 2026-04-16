CREATE TABLE universos.sp500 (
    ticker text PRIMARY KEY,
    companyname text,
    cik text,
    exchange text,
    exchangeshortname text,
    sector text,
    industry text,
    mktcap numeric,
    country text,                -- país donde está registrada la empresa
    isadr boolean DEFAULT false, -- si es ADR o no
    fecha date NOT NULL,         -- snapshot del universo
    source text DEFAULT 'manual',
    created_at timestamp DEFAULT now()
);