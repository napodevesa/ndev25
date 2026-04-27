INSERT INTO etf.catalog (ticker, nombre, categoria, subcategoria, descripcion) VALUES

-- Metales preciosos
('GLD',  'SPDR Gold Shares',           'commodity_metal', 'precioso',    'Oro físico'),
('SLV',  'iShares Silver Trust',       'commodity_metal', 'precioso',    'Plata física'),
('PPLT', 'Aberdeen Platinum ETF',      'commodity_metal', 'precioso',    'Platino'),

-- Energía
('USO',  'US Oil Fund',                'commodity_energia','petroleo',   'Petróleo crudo WTI'),
('UNG',  'US Natural Gas Fund',        'commodity_energia','gas',        'Gas natural'),
('URA',  'Global X Uranium ETF',       'commodity_energia','uranio',     'Mineras uranio'),

-- Metales industriales
('COPX', 'Global X Copper Miners',     'commodity_metal', 'industrial',  'Mineras cobre'),
('REMX', 'VanEck Rare Earth ETF',      'commodity_metal', 'industrial',  'Tierras raras'),
('PICK', 'iShares MSCI Global Metals', 'commodity_metal', 'industrial',  'Mineras diversificadas'),

-- Agrícolas
('DBA',  'Invesco DB Agriculture',     'commodity_agricola','canasta',   'Canasta agrícola'),
('WEAT', 'Teucrium Wheat Fund',        'commodity_agricola','grano',     'Trigo'),
('CORN', 'Teucrium Corn Fund',         'commodity_agricola','grano',     'Maíz'),

-- Internacionales
('EEM',  'iShares MSCI Emerging',      'internacional',   'emergentes',  'Mercados emergentes'),
('EFA',  'iShares MSCI EAFE',          'internacional',   'desarrollado','Desarrollados ex-USA'),
('FXI',  'iShares China Large-Cap',    'internacional',   'asia',        'China large caps'),
('EWZ',  'iShares MSCI Brazil',        'internacional',   'latam',       'Brasil'),
('INDA', 'iShares MSCI India',         'internacional',   'asia',        'India'),

-- Renta fija
('TLT',  'iShares 20+ Year Treasury',  'renta_fija',     'largo_plazo', 'Bonos USA largo plazo'),
('HYG',  'iShares High Yield Corp',    'renta_fija',     'high_yield',  'Bonos high yield');