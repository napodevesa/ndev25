-- ============================================================
--  TABLA 4: macro_notas_ai
--  Una fila por corrida con la nota generada por Claude.
--  No bloquea el diagnóstico — se ejecuta después del INSERT
--  en macro_diagnostico, con el mismo run_id.
--
--  El agente de trading lee esta tabla para contexto cualitativo.
--  No la genera en tiempo real — la lee de acá (velocidad + costo).
-- ============================================================

CREATE TABLE IF NOT EXISTS macro.macro_notas_ai (

    id              SERIAL          PRIMARY KEY,
    run_id          VARCHAR(40)     NOT NULL,       -- mismo run_id del pipeline
    diagnostico_id  INTEGER         NOT NULL
                        REFERENCES macro.macro_diagnostico(id),
    generado_en     TIMESTAMPTZ     DEFAULT NOW(),
    modelo_ai       VARCHAR(50)     DEFAULT 'claude-sonnet-4-20250514',

    -- Estado macro que recibió la AI (para referencia rápida)
    estado_macro    VARCHAR(20)     NOT NULL,       -- EXPANSION|SLOWDOWN|CONTRACTION|RECOVERY

    -- ── Nota estructurada ──────────────────────────────────
    -- Claude devuelve un JSON con estos campos.
    -- Los guardamos separados para que el agente pueda leer
    -- cada parte sin parsear texto.
    resumen         TEXT,           -- 2-3 oraciones: qué está pasando y por qué
    riesgos         TEXT,           -- principales riesgos identificados
    outlook         TEXT,           -- visión próximos 3-6 meses
    nota_completa   TEXT,           -- respuesta completa (backup)

    -- ── Scores numéricos ───────────────────────────────────
    -- La AI devuelve estos números en el JSON.
    -- Complementan el score_riesgo determinístico del SQL.
    score_sentimiento   SMALLINT,   -- 0-100: qué tan positivo es el panorama
    score_recesion      SMALLINT,   -- 0-100: probabilidad de recesión estimada
    score_inflacion     SMALLINT,   -- 0-100: riesgo inflacionario

    -- ── Metadata ───────────────────────────────────────────
    tokens_usados   INTEGER,
    prompt_version  VARCHAR(10)     DEFAULT 'v1'
);

CREATE INDEX IF NOT EXISTS idx_notas_run_id
    ON macro.macro_notas_ai (run_id);
CREATE INDEX IF NOT EXISTS idx_notas_diagnostico
    ON macro.macro_notas_ai (diagnostico_id);
CREATE INDEX IF NOT EXISTS idx_notas_generado
    ON macro.macro_notas_ai (generado_en DESC);