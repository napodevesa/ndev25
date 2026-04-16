-- ============================================================
--  CAPA SECTOR — TABLA 1: sector_etfs
--  Catálogo completo de ETFs del universo de análisis.
--  Se llena una sola vez. Nunca se borra.
--
--  Estructura:
--  - 9 ETFs sectoriales SPDR (benchmark de sector)
--  - ~56 ETFs de industria (iShares, SPDR, temáticos)
--  - 2 ETFs de refugio (GLD, TLT)
--  - 1 benchmark de mercado (SPY)
-- ============================================================

CREATE TABLE IF NOT EXISTS sector.sector_etfs (

    ticker          VARCHAR(10)     PRIMARY KEY,
    nombre          VARCHAR(100)    NOT NULL,

    -- Clasificación
    tipo            VARCHAR(20)     NOT NULL,
    -- 'benchmark'   → SPY
    -- 'sector'      → XLK, XLF, XLV... (SPDR sectoriales)
    -- 'industria'   → ETFs de industria específica
    -- 'refugio'     → GLD, TLT

    sector_gics     VARCHAR(50),    -- sector GICS al que pertenece
    sector_etf      VARCHAR(10),    -- ETF sectorial padre (ej: SOXX → XLK)
    industria       VARCHAR(100),   -- descripción de la industria

    -- Metadata
    emisor          VARCHAR(20),    -- 'SPDR', 'iShares', 'Invesco', etc.
    activo          BOOLEAN         DEFAULT TRUE,
    creado_en       TIMESTAMPTZ     DEFAULT NOW()
);

-- ── Poblar catálogo ────────────────────────────────────────────────────────────

-- BENCHMARK
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('SPY',  'SPDR S&P 500 ETF',                    'benchmark', NULL, NULL, 'S&P 500 — mercado total', 'SPDR');

-- SECTORIALES SPDR (benchmarks de sector)
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('XLK',  'Technology Select Sector SPDR',        'sector', 'Tecnología',              'XLK',  'Tecnología broad',          'SPDR'),
    ('XLV',  'Health Care Select Sector SPDR',       'sector', 'Salud',                   'XLV',  'Salud broad',               'SPDR'),
    ('XLF',  'Financial Select Sector SPDR',         'sector', 'Financiero',              'XLF',  'Financiero broad',          'SPDR'),
    ('XLE',  'Energy Select Sector SPDR',            'sector', 'Energía',                 'XLE',  'Energía broad',             'SPDR'),
    ('XLY',  'Consumer Discret. Select Sector SPDR', 'sector', 'Consumo discrecional',    'XLY',  'Consumo discrecional broad','SPDR'),
    ('XLP',  'Consumer Staples Select Sector SPDR',  'sector', 'Consumo básico',          'XLP',  'Consumo básico broad',      'SPDR'),
    ('XLU',  'Utilities Select Sector SPDR',         'sector', 'Utilities',               'XLU',  'Utilities broad',           'SPDR'),
    ('XLI',  'Industrial Select Sector SPDR',        'sector', 'Industrial',              'XLI',  'Industrial broad',          'SPDR'),
    ('XLB',  'Materials Select Sector SPDR',         'sector', 'Materiales',              'XLB',  'Materiales broad',          'SPDR'),
    ('XLRE', 'Real Estate Select Sector SPDR',       'sector', 'Real estate',             'XLRE', 'Real estate broad',         'SPDR'),
    ('XLC',  'Communication Services SPDR',          'sector', 'Comunicaciones',          'XLC',  'Comunicaciones broad',      'SPDR');

-- TECNOLOGÍA — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('SOXX', 'iShares Semiconductor ETF',            'industria', 'Tecnología', 'XLK', 'Semiconductores',           'iShares'),
    ('IGV',  'iShares Expanded Tech-Software ETF',   'industria', 'Tecnología', 'XLK', 'Software',                  'iShares'),
    ('SKYY', 'First Trust Cloud Computing ETF',      'industria', 'Tecnología', 'XLK', 'Cloud computing',           'First Trust'),
    ('HACK', 'ETFMG Prime Cyber Security ETF',       'industria', 'Tecnología', 'XLK', 'Ciberseguridad',            'ETFMG'),
    ('AIQ',  'Global X AI & Technology ETF',         'industria', 'Tecnología', 'XLK', 'Inteligencia artificial',   'Global X'),
    ('FTEC', 'Fidelity MSCI IT Index ETF',           'industria', 'Tecnología', 'XLK', 'Tecnología broad Fidelity', 'Fidelity');

-- SALUD — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('IBB',  'iShares Biotechnology ETF',            'industria', 'Salud', 'XLV', 'Biotecnología',            'iShares'),
    ('IHI',  'iShares US Medical Devices ETF',       'industria', 'Salud', 'XLV', 'Dispositivos médicos',     'iShares'),
    ('XPH',  'SPDR S&P Pharmaceuticals ETF',         'industria', 'Salud', 'XLV', 'Farmacéuticas',            'SPDR'),
    ('IHF',  'iShares US Healthcare Providers ETF',  'industria', 'Salud', 'XLV', 'Servicios de salud',       'iShares'),
    ('ARKG', 'ARK Genomic Revolution ETF',           'industria', 'Salud', 'XLV', 'Genómica / biotech disruptivo', 'ARK');

-- FINANCIERO — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('KBE',  'SPDR S&P Bank ETF',                   'industria', 'Financiero', 'XLF', 'Bancos',                'SPDR'),
    ('KRE',  'SPDR S&P Regional Banking ETF',        'industria', 'Financiero', 'XLF', 'Bancos regionales',     'SPDR'),
    ('IAI',  'iShares US Broker-Dealers ETF',        'industria', 'Financiero', 'XLF', 'Brokers / asset mgmt',  'iShares'),
    ('KIE',  'SPDR S&P Insurance ETF',               'industria', 'Financiero', 'XLF', 'Seguros',               'SPDR'),
    ('IPAY', 'ETFMG Prime Mobile Payments ETF',      'industria', 'Financiero', 'XLF', 'Pagos digitales',       'ETFMG');

-- ENERGÍA — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('OIH',  'VanEck Oil Services ETF',              'industria', 'Energía', 'XLE', 'Servicios petroleros',  'VanEck'),
    ('XOP',  'SPDR S&P Oil & Gas E&P ETF',           'industria', 'Energía', 'XLE', 'Exploración y producción', 'SPDR'),
    ('AMLP', 'Alerian MLP ETF',                      'industria', 'Energía', 'XLE', 'Midstream / pipelines', 'Alerian'),
    ('ICLN', 'iShares Global Clean Energy ETF',      'industria', 'Energía', 'XLE', 'Energía limpia',        'iShares'),
    ('TAN',  'Invesco Solar ETF',                    'industria', 'Energía', 'XLE', 'Solar',                 'Invesco'),
    ('FAN',  'First Trust Global Wind Energy ETF',   'industria', 'Energía', 'XLE', 'Eólica',               'First Trust');

-- CONSUMO DISCRECIONAL — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('XRT',  'SPDR S&P Retail ETF',                  'industria', 'Consumo discrecional', 'XLY', 'Retail',            'SPDR'),
    ('JETS', 'US Global Jets ETF',                   'industria', 'Consumo discrecional', 'XLY', 'Aerolíneas',        'US Global'),
    ('AWAY', 'ETFMG Travel Tech ETF',                'industria', 'Consumo discrecional', 'XLY', 'Turismo / hotelería','ETFMG'),
    ('CARZ', 'First Trust Nasdaq Global Auto ETF',   'industria', 'Consumo discrecional', 'XLY', 'Automotriz',        'First Trust'),
    ('ONLN', 'ProShares Online Retail ETF',          'industria', 'Consumo discrecional', 'XLY', 'E-commerce',        'ProShares');

-- CONSUMO BÁSICO — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('PBJ',  'Invesco Food & Beverage ETF',          'industria', 'Consumo básico', 'XLP', 'Alimentos y bebidas',   'Invesco'),
    ('FTXG', 'First Trust Nasdaq Food & Beverage',   'industria', 'Consumo básico', 'XLP', 'Food & staples retail', 'First Trust'),
    ('JHMS', 'John Hancock Multifactor CS ETF',      'industria', 'Consumo básico', 'XLP', 'Household products',    'John Hancock');

-- INDUSTRIAL — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('ITA',  'iShares US Aerospace & Defense ETF',   'industria', 'Industrial', 'XLI', 'Defensa y aeroespacial',    'iShares'),
    ('PAVE', 'Global X US Infrastructure Dev. ETF',  'industria', 'Industrial', 'XLI', 'Infraestructura',           'Global X'),
    ('XTN',  'SPDR S&P Transportation ETF',          'industria', 'Industrial', 'XLI', 'Transporte',                'SPDR'),
    ('ROBO', 'ROBO Global Robotics & Auto. ETF',     'industria', 'Industrial', 'XLI', 'Robótica y automatización', 'ROBO Global'),
    ('WOOD', 'iShares Global Timber & Forestry ETF', 'industria', 'Industrial', 'XLI', 'Forestal / madera',         'iShares');

-- UTILITIES — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('UTG',  'Reaves Utility Income Fund',           'industria', 'Utilities', 'XLU', 'Utilities diversificado', 'Reaves'),
    ('PUI',  'Invesco DWA Utilities Momentum ETF',   'industria', 'Utilities', 'XLU', 'Utilities momentum',      'Invesco'),
    ('PHO',  'Invesco Water Resources ETF',          'industria', 'Utilities', 'XLU', 'Agua',                    'Invesco');

-- REAL ESTATE — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('REZ',  'iShares Residential & Multi REIT ETF', 'industria', 'Real estate', 'XLRE', 'REITs residenciales',   'iShares'),
    ('SRVR', 'Pacer Benchmark Data & Infra REIT',    'industria', 'Real estate', 'XLRE', 'Data centers / torres', 'Pacer'),
    ('INDS', 'Pacer Benchmark Industrial REIT ETF',  'industria', 'Real estate', 'XLRE', 'REITs industriales',    'Pacer'),
    ('KBWY', 'Invesco KBW Premium Yield REIT ETF',   'industria', 'Real estate', 'XLRE', 'REITs small cap yield', 'Invesco');

-- MATERIALES — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('GDX',  'VanEck Gold Miners ETF',               'industria', 'Materiales', 'XLB', 'Mineras de oro',    'VanEck'),
    ('COPX', 'Global X Copper Miners ETF',           'industria', 'Materiales', 'XLB', 'Mineras de cobre',  'Global X'),
    ('LIT',  'Global X Lithium & Battery Tech ETF',  'industria', 'Materiales', 'XLB', 'Litio y baterías',  'Global X'),
    ('REMX', 'VanEck Rare Earth/Strat Metals ETF',   'industria', 'Materiales', 'XLB', 'Tierras raras',     'VanEck');

-- COMUNICACIONES — industrias
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('SOCL', 'Global X Social Media ETF',            'industria', 'Comunicaciones', 'XLC', 'Redes sociales',        'Global X'),
    ('NERD', 'Roundhill BITKRAFT Esports ETF',       'industria', 'Comunicaciones', 'XLC', 'Gaming / esports',      'Roundhill'),
    ('FIVG', 'Defiance Next Gen Connectivity ETF',   'industria', 'Comunicaciones', 'XLC', '5G',                    'Defiance');

-- REFUGIO — activos defensivos para CONTRACTION
INSERT INTO sector.sector_etfs (ticker, nombre, tipo, sector_gics, sector_etf, industria, emisor) VALUES
    ('GLD',  'SPDR Gold Shares',                     'refugio', NULL, NULL, 'Oro — refugio inflación / crisis', 'SPDR'),
    ('TLT',  'iShares 20+ Year Treasury Bond ETF',   'refugio', NULL, NULL, 'Bonos largo plazo — refugio recesión', 'iShares')
ON CONFLICT (ticker) DO NOTHING;

-- ── Índices ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_etfs_tipo
    ON sector.sector_etfs (tipo);
CREATE INDEX IF NOT EXISTS idx_etfs_sector
    ON sector.sector_etfs (sector_gics);
CREATE INDEX IF NOT EXISTS idx_etfs_padre
    ON sector.sector_etfs (sector_etf);

-- ── Verificación rápida ────────────────────────────────────────────────────────
-- SELECT tipo, COUNT(*) FROM sector.sector_etfs GROUP BY tipo ORDER BY tipo;
-- SELECT sector_gics, COUNT(*) FROM sector.sector_etfs
--   WHERE tipo = 'industria' GROUP BY sector_gics ORDER BY sector_gics;