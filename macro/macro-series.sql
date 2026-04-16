-- ============================================================
--  SCHEMA: Sistema Macro USA → Agentes de Trading
--  Compatible: PostgreSQL 14+ / SQLite 3.35+
--  Notas SQLite: reemplazar SERIAL por INTEGER PRIMARY KEY,
--                TIMESTAMPTZ por TEXT, NUMERIC por REAL
-- ============================================================
 
 
-- ────────────────────────────────────────────────────────────
--  TABLA 1: macro_series
--  Catálogo de series. Una fila por indicador conocido.
--  Se llena una sola vez y se actualiza si cambia algún ID.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS macro.macro_series (
    serie_id        VARCHAR(30)  PRIMARY KEY,          -- "UNRATE", "T10Y2Y", etc.
    nombre          VARCHAR(100) NOT NULL,              -- "Tasa de Desempleo"
    unidad          VARCHAR(20)  NOT NULL,              -- "%", "pp", "índice", "miles"
    frecuencia      VARCHAR(10)  NOT NULL,              -- "diaria", "mensual", "trimestral"
    descripcion     VARCHAR(200),
    fuente          VARCHAR(20)  DEFAULT 'FRED',
    activo          BOOLEAN      DEFAULT TRUE,
    creado_en       TIMESTAMPTZ  DEFAULT NOW()
);
 
-- Poblar catálogo con las series del script Python
INSERT INTO macro.macro_series (serie_id, nombre, unidad, frecuencia, descripcion) VALUES
    ('UNRATE',          'Tasa de Desempleo',              '%',      'mensual',     'Mercado laboral'),
    ('DFEDTARU',        'Tasa Fed Funds (objetivo sup)',   '%',      'diaria',      'Política monetaria'),
    ('CPIAUCSL',        'IPC (nivel índice)',              'índice', 'mensual',     'Base cálculo inflación'),
    ('CPILFESL',        'IPC Subyacente (core)',           'índice', 'mensual',     'Sin alimentos ni energía'),
    ('T10Y2Y',          'Curva Rendimiento 10Y-2Y',        'pp',     'diaria',      'Spread de tasas'),
    ('DGS2',            'Bono 2Y',                         '%',      'diaria',      'Parte corta curva'),
    ('DGS10',           'Bono 10Y',                        '%',      'diaria',      'Parte larga curva'),
    ('A191RL1Q225SBEA', 'PIB Real (var. trimestral)',      '%',      'trimestral',  'Crecimiento económico'),
    ('PAYEMS',          'Nóminas no agrícolas (NFP)',       'miles',  'mensual',     'Empleo total'),
    ('RSAFS',           'Ventas minoristas',               'M USD',  'mensual',     'Consumo'),
    ('T5YIE',           'Expectativas inflación 5Y',       '%',      'diaria',      'Credibilidad Fed'),
    ('NFCI',            'Cond. financieras (NFCI)',        'índice', 'semanal',     'Crédito+liquidez+riesgo'),
    ('VIXCLS',          'VIX (rezago 1 día)',              'puntos', 'diaria',      'Volatilidad implícita'),
    ('PERMIT',          'Permisos de construcción',        'miles',  'mensual',     'Indicador líder'),
    ('TOTALSL',         'Crédito al consumo',              'M USD',  'mensual',     'Estrés financiero'),
    -- Series derivadas calculadas por el script (no vienen directo de FRED)
    ('IPC_ANUAL',       'IPC Anual (%)',                   '%',      'mensual',     'Calculada: var. a/a de CPIAUCSL'),
    ('CORE_ANUAL',      'IPC Subyacente Anual (%)',        '%',      'mensual',     'Calculada: var. a/a de CPILFESL')
ON CONFLICT (serie_id) DO NOTHING;
 
 