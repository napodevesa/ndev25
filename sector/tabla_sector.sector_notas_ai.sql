 
-- ============================================================
--  TABLA 4: sector_notas_ai
--  Una nota AI por corrida — resume el panorama sectorial.
--  Mismo patrón que macro_notas_ai.
-- ============================================================
 
CREATE TABLE IF NOT EXISTS sector.sector_notas_ai (
 
    id              SERIAL          PRIMARY KEY,
    run_id          VARCHAR(40)     NOT NULL,
    snapshot_id     INTEGER,                        -- ref al primer id del run
    generado_en     TIMESTAMPTZ     DEFAULT NOW(),
    modelo_ai       VARCHAR(50)     DEFAULT 'claude-sonnet-4-20250514',
 
    -- Estado macro vigente cuando se generó la nota
    estado_macro    VARCHAR(20),
 
    -- Top sectores que recibió la AI
    top_tickers     TEXT,                           -- "FAN, XOP, OIH, AMLP, SRVR"
 
    -- Nota estructurada
    resumen         TEXT,                           -- qué está rotando y por qué
    oportunidades   TEXT,                           -- industrias con mejor setup
    riesgos         TEXT,                           -- sectores a evitar
    nota_completa   TEXT,                           -- respuesta completa
 
    -- Scores
    score_rotacion  SMALLINT,                       -- 0-100: qué tan clara es la rotación
    score_riesgo    SMALLINT,                       -- 0-100: nivel de riesgo sectorial
 
    tokens_usados   INTEGER,
    prompt_version  VARCHAR(10)     DEFAULT 'v1'
);
 
CREATE INDEX IF NOT EXISTS idx_snap_notas_run
    ON sector.sector_notas_ai (run_id);
CREATE INDEX IF NOT EXISTS idx_snap_notas_generado
    ON sector.sector_notas_ai (generado_en DESC);
 
 
-- ============================================================
--  VERIFICACIÓN rápida
-- ============================================================
 
-- Ver el último snapshot completo ordenado por score:
-- SELECT ticker, industria, estado, alineacion_macro,
--        score_total, rank_total, ret_3m, rsi_rs_semanal
-- FROM sector.sector_snapshot
-- WHERE run_id = (SELECT MAX(run_id) FROM sector.sector_snapshot)
-- ORDER BY rank_total;
 
-- Ver top industrias alineadas con el macro:
-- SELECT ticker, industria, estado_macro, alineacion_macro, score_total
-- FROM sector.sector_snapshot
-- WHERE run_id = (SELECT MAX(run_id) FROM sector.sector_snapshot)
--   AND alineacion_macro = 'ALIGNED'
--   AND estado = 'LEADING_STRONG'
-- ORDER BY rank_total;