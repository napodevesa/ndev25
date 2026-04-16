--DROP TABLE IF EXISTS procesados.value_benchmark_sector_mktcap;

CREATE TABLE procesados.value_benchmark_sector_mktcap (
    snapshot_date       DATE    NOT NULL,
    sector              TEXT    NOT NULL,
    market_cap_tier     TEXT    NOT NULL,
    n_empresas          INTEGER,
    -- PER
    pe_p25              DOUBLE PRECISION,
    pe_p50              DOUBLE PRECISION,
    pe_p75              DOUBLE PRECISION,
    -- PEG
    peg_p25             DOUBLE PRECISION,
    peg_p50             DOUBLE PRECISION,
    peg_p75             DOUBLE PRECISION,
    -- FCF Yield
    fcfyield_p25        DOUBLE PRECISION,
    fcfyield_p50        DOUBLE PRECISION,
    fcfyield_p75        DOUBLE PRECISION,
    created_at          TIMESTAMP DEFAULT now(),
    PRIMARY KEY (snapshot_date, sector, market_cap_tier)
);