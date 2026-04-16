-- ============================================================
--  TABLA 2: macro_raw
--  Historial de observaciones descargadas desde FRED.
--  El script Python inserta aquí después de cada corrida.
--
--  Regla de oro: append-only, nunca se borra nada.
--  Si el dato ya existe (misma serie + misma fecha) → skip.
-- ============================================================
 
CREATE TABLE IF NOT EXISTS macro.macro_raw (
 
    -- Identificación
    id              SERIAL          PRIMARY KEY,
    serie_id        VARCHAR(30)     NOT NULL
                        REFERENCES macro.macro_series(serie_id),
    fecha_dato      DATE            NOT NULL,   -- fecha del dato según FRED
 
    -- Valor
    valor           NUMERIC(18,6),              -- valor numérico limpio
    valor_texto     VARCHAR(20),                -- string original de FRED (por si viene ".")
 
    -- Semáforo individual (calculado por el script Python)
    -- 'verde' | 'amarillo' | 'rojo' | null
    semaforo        VARCHAR(10),
    nota_semaforo   VARCHAR(200),               -- "Pleno empleo", "Curva invertida", etc.
 
    -- Metadata de la corrida
    descargado_en   TIMESTAMPTZ     DEFAULT NOW(),
    run_id          VARCHAR(40),                -- timestamp de la corrida, ej: "20260331_0900"
                                                -- sirve para agrupar todos los datos de una misma ejecución
 
    -- Constraint: no duplicar el mismo dato
    UNIQUE (serie_id, fecha_dato)
);
 
-- ── Índices ────────────────────────────────────────────────
-- El más usado: "dame el último valor de esta serie"
CREATE INDEX IF NOT EXISTS idx_raw_serie_fecha
    ON macro.macro_raw (serie_id, fecha_dato DESC);
 
-- Para auditoría: "qué se descargó en esta corrida"
CREATE INDEX IF NOT EXISTS idx_raw_run_id
    ON macro.macro_raw (run_id);
 
-- Para queries temporales: "qué se cargó entre estas fechas"
CREATE INDEX IF NOT EXISTS idx_raw_descargado
    ON macro.macro_raw (descargado_en DESC);