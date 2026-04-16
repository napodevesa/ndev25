-- ============================================================
--  VISTAS + TABLA DE DIAGNÓSTICO
--  Flujo: macro_raw → v_pivot → v_semaforo → v_diagnostico
--                                          → macro_diagnostico (tabla)
--
--  4 estados: EXPANSION | SLOWDOWN | CONTRACTION | RECOVERY
-- ============================================================


-- ────────────────────────────────────────────────────────────
--  VISTA 1: v_macro_pivot
--  Una fila con el último valor conocido de cada serie.
--  Es la base que leen todas las vistas siguientes.
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW macro.v_macro_pivot AS
SELECT
    MAX(CASE WHEN serie_id = 'UNRATE'          THEN valor END) AS desempleo,
    MAX(CASE WHEN serie_id = 'DFEDTARU'        THEN valor END) AS fed_funds,
    MAX(CASE WHEN serie_id = 'IPC_ANUAL'       THEN valor END) AS ipc_anual,
    MAX(CASE WHEN serie_id = 'CORE_ANUAL'      THEN valor END) AS core_anual,
    MAX(CASE WHEN serie_id = 'T10Y2Y'          THEN valor END) AS curva_10y2y,
    MAX(CASE WHEN serie_id = 'DGS2'            THEN valor END) AS bono_2y,
    MAX(CASE WHEN serie_id = 'DGS10'           THEN valor END) AS bono_10y,
    MAX(CASE WHEN serie_id = 'A191RL1Q225SBEA' THEN valor END) AS pib_trim,
    MAX(CASE WHEN serie_id = 'PAYEMS'          THEN valor END) AS nfp_miles,
    MAX(CASE WHEN serie_id = 'RSAFS'           THEN valor END) AS ventas_minoristas,
    MAX(CASE WHEN serie_id = 'T5YIE'           THEN valor END) AS expectativas_inf,
    MAX(CASE WHEN serie_id = 'NFCI'            THEN valor END) AS nfci,
    MAX(CASE WHEN serie_id = 'VIXCLS'          THEN valor END) AS vix,
    MAX(CASE WHEN serie_id = 'PERMIT'          THEN valor END) AS permisos_constr,
    MAX(CASE WHEN serie_id = 'TOTALSL'         THEN valor END) AS credito_consumo,
    MAX(descargado_en)                                         AS ultima_actualizacion
FROM macro.macro_raw
WHERE (serie_id, fecha_dato) IN (
    -- Para cada serie, toma solo la fila con la fecha más reciente
    SELECT serie_id, MAX(fecha_dato)
    FROM macro.macro_raw
    GROUP BY serie_id
);


-- ────────────────────────────────────────────────────────────
--  VISTA 2: v_semaforo
--  Aplica las reglas CASE WHEN indicador por indicador.
--  Cada uno devuelve: 'verde' | 'amarillo' | 'rojo'
--
--  Para cambiar un umbral: editás solo esta vista.
--  El diagnóstico final se actualiza solo.
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW macro.v_semaforo AS
SELECT
    -- Valores numéricos (pasan sin tocar para usarlos en v_diagnostico)
    desempleo, fed_funds, ipc_anual, core_anual,
    curva_10y2y, bono_2y, bono_10y, pib_trim,
    nfp_miles, ventas_minoristas, expectativas_inf,
    nfci, vix, permisos_constr, credito_consumo,
    ultima_actualizacion,

    -- ── Semáforos individuales ──────────────────────────────

    CASE
        WHEN desempleo    <= 4.0 THEN 'verde'
        WHEN desempleo    <= 5.5 THEN 'amarillo'
        ELSE                          'rojo'
    END AS s_desempleo,

    CASE
        WHEN fed_funds    <= 2.5 THEN 'verde'
        WHEN fed_funds    <= 4.5 THEN 'amarillo'
        ELSE                          'rojo'
    END AS s_fed,

    CASE
        WHEN ipc_anual    <= 2.5 THEN 'verde'
        WHEN ipc_anual    <= 4.0 THEN 'amarillo'
        ELSE                          'rojo'
    END AS s_ipc,

    CASE
        WHEN core_anual   <= 2.5 THEN 'verde'
        WHEN core_anual   <= 4.0 THEN 'amarillo'
        ELSE                          'rojo'
    END AS s_core,

    -- Curva: negativa es roja (señal recesión clásica)
    CASE
        WHEN curva_10y2y  <  -0.01 THEN 'rojo'
        WHEN curva_10y2y  <   0.5  THEN 'amarillo'
        ELSE                            'verde'
    END AS s_curva,

    CASE
        WHEN pib_trim     <= 0.0 THEN 'rojo'
        WHEN pib_trim     <= 1.5 THEN 'amarillo'
        ELSE                          'verde'
    END AS s_pib,

    CASE
        WHEN vix          <= 20.0 THEN 'verde'
        WHEN vix          <= 30.0 THEN 'amarillo'
        ELSE                           'rojo'
    END AS s_vix,

    CASE
        WHEN expectativas_inf <= 2.5 THEN 'verde'
        WHEN expectativas_inf <= 3.0 THEN 'amarillo'
        ELSE                               'rojo'
    END AS s_expectativas,

    CASE
        WHEN nfci         <= 0.0 THEN 'verde'
        WHEN nfci         <= 0.5 THEN 'amarillo'
        ELSE                          'rojo'
    END AS s_nfci,

    CASE
        WHEN ventas_minoristas >= 700000 THEN 'verde'
        WHEN ventas_minoristas >= 650000 THEN 'amarillo'
        ELSE                                  'rojo'
    END AS s_ventas,

    CASE
        WHEN permisos_constr >= 1400 THEN 'verde'
        WHEN permisos_constr >= 1000 THEN 'amarillo'
        ELSE                              'rojo'
    END AS s_permisos,

    CASE
        WHEN credito_consumo <= 4800000 THEN 'verde'
        WHEN credito_consumo <= 5200000 THEN 'amarillo'
        ELSE                                 'rojo'
    END AS s_credito

FROM macro.v_macro_pivot;


-- ────────────────────────────────────────────────────────────
--  VISTA 3: v_diagnostico
--  Cuenta señales, calcula score, y asigna el estado macro.
--
--  ORDEN DE PRIORIDAD (importante — SQL evalúa de arriba a abajo):
--    1. CONTRACTION  — lo más grave, tiene prioridad absoluta
--    2. SLOWDOWN     — deterioro claro pero no recesión
--    3. RECOVERY     — rebote confirmado
--    4. EXPANSION    — estado base / default
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW macro.v_diagnostico AS

-- Paso intermedio: conteo de semáforos y score
WITH conteo AS (
    SELECT
        *,

        -- Cuántos indicadores están en cada color
        (CASE WHEN s_desempleo    = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_fed          = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_ipc          = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_core         = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_curva        = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_pib          = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_vix          = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_expectativas = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_nfci         = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_ventas       = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_permisos     = 'verde' THEN 1 ELSE 0 END +
         CASE WHEN s_credito      = 'verde' THEN 1 ELSE 0 END
        ) AS n_verdes,

        (CASE WHEN s_desempleo    = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_fed          = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_ipc          = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_core         = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_curva        = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_pib          = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_vix          = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_expectativas = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_nfci         = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_ventas       = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_permisos     = 'amarillo' THEN 1 ELSE 0 END +
         CASE WHEN s_credito      = 'amarillo' THEN 1 ELSE 0 END
        ) AS n_amarillos,

        (CASE WHEN s_desempleo    = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_fed          = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_ipc          = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_core         = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_curva        = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_pib          = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_vix          = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_expectativas = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_nfci         = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_ventas       = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_permisos     = 'rojo' THEN 1 ELSE 0 END +
         CASE WHEN s_credito      = 'rojo' THEN 1 ELSE 0 END
        ) AS n_rojos

    FROM macro.v_semaforo
)

SELECT
    -- Datos y semáforos individuales
    desempleo, fed_funds, ipc_anual, core_anual,
    curva_10y2y, pib_trim, vix, expectativas_inf, nfci,
    s_desempleo, s_fed, s_ipc, s_core,
    s_curva, s_pib, s_vix, s_expectativas,
    s_nfci, s_ventas, s_permisos, s_credito,

    -- Conteos
    n_verdes, n_amarillos, n_rojos,

    -- Score de riesgo 0-100 (rojos pesan doble)
    -- Fórmula: puntos_obtenidos / puntos_maximos * 100
    -- Máximo: 12 indicadores × 2 puntos = 24
    LEAST(100, ROUND(
        (n_rojos * 2 + n_amarillos)::NUMERIC / 24.0 * 100
    , 0))::SMALLINT AS score_riesgo,

    -- ── ESTADO MACRO (4 estados, prioridad explícita) ───────
    CASE

        -- 1. CONTRACTION
        --    PIB negativo + desempleo subiendo, o colapso generalizado
        WHEN (s_pib = 'rojo' AND s_desempleo IN ('amarillo','rojo'))
          OR n_rojos >= 4
        THEN 'CONTRACTION'

        -- 2. SLOWDOWN
        --    Crecimiento enfriando, señales mixtas deteriorando
        WHEN s_pib = 'amarillo'
          OR (s_desempleo = 'amarillo' AND s_curva IN ('amarillo','rojo'))
          OR n_amarillos >= 4
          OR (n_rojos >= 2 AND s_pib != 'verde')
        THEN 'SLOWDOWN'

        -- 3. RECOVERY
        --    PIB volviendo a verde, inflación controlada, Fed laxa
        WHEN s_pib  = 'verde'
         AND s_ipc  IN ('verde','amarillo')
         AND s_fed  = 'verde'
         AND n_rojos <= 1
        THEN 'RECOVERY'

        -- 4. EXPANSION (default)
        --    Todo lo que no encaja arriba: economía sana
        ELSE 'EXPANSION'

    END AS estado_macro,

    -- ── CONFIANZA del diagnóstico ────────────────────────────
    -- Alta: señales muy alineadas en una dirección
    -- Baja: señales contradictorias
    CASE
        WHEN n_rojos >= 4 OR n_verdes >= 9  THEN 'alta'
        WHEN n_amarillos >= 5               THEN 'baja'
        ELSE                                     'media'
    END AS confianza,

    -- ── REGLA que disparó el estado (para auditoría) ─────────
    CASE
        WHEN (s_pib = 'rojo' AND s_desempleo IN ('amarillo','rojo'))
          OR n_rojos >= 4
        THEN 'PIB rojo + desempleo deteriorado, o 4+ rojos'

        WHEN s_pib = 'amarillo'
          OR (s_desempleo = 'amarillo' AND s_curva IN ('amarillo','rojo'))
          OR n_amarillos >= 4
          OR (n_rojos >= 2 AND s_pib != 'verde')
        THEN 'PIB amarillo o múltiples señales de deterioro'

        WHEN s_pib  = 'verde'
         AND s_ipc  IN ('verde','amarillo')
         AND s_fed  = 'verde'
         AND n_rojos <= 1
        THEN 'PIB verde + inflación ok + Fed laxa'

        ELSE 'Estado base — pocas señales negativas'
    END AS regla_disparada,

    ultima_actualizacion

FROM conteo;


-- ────────────────────────────────────────────────────────────
--  TABLA 3: macro_diagnostico
--  Snapshot histórico de cada corrida del motor SQL.
--  Las vistas calculan en tiempo real; esta tabla persiste.
--
--  Se llena desde Python al final de cada pipeline.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS macro.macro_diagnostico (
    id              SERIAL          PRIMARY KEY,
    run_id          VARCHAR(40)     NOT NULL,       -- mismo run_id que macro_raw
    calculado_en    TIMESTAMPTZ     DEFAULT NOW(),

    -- Estado final
    estado_macro    VARCHAR(20)     NOT NULL,       -- EXPANSION|SLOWDOWN|CONTRACTION|RECOVERY
    confianza       VARCHAR(10),                    -- alta|media|baja
    score_riesgo    SMALLINT,                       -- 0-100
    n_verdes        SMALLINT,
    n_amarillos     SMALLINT,
    n_rojos         SMALLINT,
    regla_disparada VARCHAR(200),

    -- Semáforos individuales (snapshot)
    s_desempleo     VARCHAR(10),
    s_fed           VARCHAR(10),
    s_ipc           VARCHAR(10),
    s_core          VARCHAR(10),
    s_curva         VARCHAR(10),
    s_pib           VARCHAR(10),
    s_vix           VARCHAR(10),
    s_expectativas  VARCHAR(10),
    s_nfci          VARCHAR(10),

    -- Valores numéricos clave (snapshot)
    desempleo       NUMERIC(6,2),
    fed_funds       NUMERIC(6,2),
    ipc_anual       NUMERIC(6,2),
    core_anual      NUMERIC(6,2),
    curva_10y2y     NUMERIC(8,4),
    pib_trim        NUMERIC(6,2),
    vix             NUMERIC(6,2)
);

CREATE INDEX IF NOT EXISTS idx_diag_run_id
    ON macro.macro_diagnostico (run_id);
CREATE INDEX IF NOT EXISTS idx_diag_estado
    ON macro.macro_diagnostico (estado_macro, calculado_en DESC);
CREATE INDEX IF NOT EXISTS idx_diag_calculado
    ON macro.macro_diagnostico (calculado_en DESC);


-- ────────────────────────────────────────────────────────────
--  VERIFICACIÓN rápida — correr después de crear las vistas
-- ────────────────────────────────────────────────────────────

-- Ver el diagnóstico actual completo:
-- SELECT estado_macro, confianza, score_riesgo,
--        n_verdes, n_amarillos, n_rojos,
--        regla_disparada
-- FROM v_diagnostico;

-- Ver semáforos individuales:
-- SELECT s_desempleo, s_fed, s_ipc, s_curva, s_pib, s_vix
-- FROM v_diagnostico;

-- Ver los valores numéricos:
-- SELECT desempleo, fed_funds, ipc_anual, curva_10y2y, pib_trim, vix
-- FROM v_diagnostico;