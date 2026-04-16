CREATE TABLE IF NOT EXISTS sector.sector_diagnostico_tecnico (

    id                  SERIAL          PRIMARY KEY,
    fecha               DATE            NOT NULL,
    run_id              VARCHAR(40)     NOT NULL,

    -- Diagnóstico por grupo
    score_defensivos    NUMERIC(6,2),   -- RSI_RS promedio de XLV,XLP,XLU,GLD,TLT
    score_ciclicos      NUMERIC(6,2),   -- RSI_RS promedio de XLK,XLY,XLF,XLI,XLE
    score_mixtos        NUMERIC(6,2),   -- XLB, XLRE, XLC

    -- Líderes y rezagados
    top_3_lideres       TEXT,           -- "XLV, GLD, XLP"
    top_3_rezagados     TEXT,           -- "XLK, XLY, XLF"

    -- Señal de volumen
    vol_defensivos      NUMERIC(6,2),   -- volume_ratio promedio defensivos
    vol_ciclicos        NUMERIC(6,2),   -- volume_ratio promedio cíclicos

    -- Diagnóstico sectorial
    diagnostico_sector  VARCHAR(30),
    -- CONFIRMA_SLOWDOWN | CONTRADICE_SLOWDOWN
    -- CONFIRMA_EXPANSION | CONFIRMA_CONTRACTION | SEÑAL_MIXTA

    -- Coherencia con macro
    estado_macro        VARCHAR(20),    -- copiado de macro_diagnostico
    coherencia          VARCHAR(20),    -- ALTA | MEDIA | BAJA | CONTRADICE

    -- Nota explicativa
    nota                TEXT,

    UNIQUE (fecha)
);