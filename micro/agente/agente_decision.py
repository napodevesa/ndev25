#!/usr/bin/env python3
"""
agente_decision.py

Lee seleccion.enriquecimiento y produce agente.decision con el motor
determinístico completo (CTEs encadenados en SQL, v5.0).

Flujo:
  1. Ejecuta INSERT … WITH … SELECT sobre agente.decision
  2. Inserta las top 25 en agente.top (JOIN con enriquecimiento)
  3. Muestra resumen en consola
  4. Registra en infraestructura.update_logs

Nota: seleccion.enriquecimiento solo guarda roic_signo/deuda_signo.
El campo fcf_signo no existe — la condición se omite en el CTE tendencia.

Sin API calls — SQL puro.

Ejecución: python micro/agente/agente_decision.py
"""

import os
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, date
from dotenv import load_dotenv

# ── ENV ────────────────────────────────────────────────────────────────────────
load_dotenv("/Users/ndev/Desktop/ndev25/.env")

POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5433))

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID        = datetime.now().strftime("%Y%m%d_%H%M")
SNAPSHOT_DATE = date(date.today().year, date.today().month, 1)

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"agente_decision_{date.today().isoformat()}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── DB ─────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )

# ── SQL MACRO (para display) ───────────────────────────────────────────────────
SQL_MACRO = """
    SELECT estado_macro, score_riesgo
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC LIMIT 1
"""

# ── MOTOR DETERMINÍSTICO — INSERT CON CTEs ─────────────────────────────────────
# Nota: fcf_signo no existe en seleccion.enriquecimiento — condición omitida.
# tendencia_fundamental se usa en CTEs intermedios pero no se almacena
# en agente.decision (columna no existe en la tabla).
INSERT_DECISION = """
INSERT INTO agente.decision (
    ticker, snapshot_date,
    sector, industry, market_cap_tier,
    multifactor_score, multifactor_rank,
    contexto, timing,
    estado_macro, macro_factor, sector_alineado,
    exposicion, instrumento, flag_timing,
    target_position_size,
    score_conviccion, rank_conviccion,
    riesgo_principal, notas,
    trade_status, agent_version
)

WITH

-- Leer macro
estado_macro AS (
    SELECT estado_macro, score_riesgo
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC LIMIT 1
),

-- Base con todos los datos
base AS (
    SELECT e.*, m.estado_macro AS estado_macro_actual, m.score_riesgo
    FROM seleccion.enriquecimiento e
    CROSS JOIN estado_macro m
    WHERE e.snapshot_date = (SELECT MAX(snapshot_date)
                              FROM seleccion.enriquecimiento)
),

-- 1. TENDENCIA FUNDAMENTAL (regresiones)
tendencia AS (
    SELECT *,
        CASE
            WHEN roic_confiable AND roic_signo = 1
             AND deuda_confiable AND deuda_signo = -1  THEN 'mejora_estructural'
            WHEN roic_signo = 1
             AND deuda_signo = -1                      THEN 'mejora_parcial'
            WHEN roic_signo = -1
             AND roic_confiable                        THEN 'deterioro'
            ELSE                                            'sin_tendencia'
        END AS tendencia_fundamental
    FROM base
),

-- 2. CONTEXTO ESTRUCTURAL
contexto AS (
    SELECT *,
        CASE
            WHEN altman_z_score < 1.81                THEN 'structural_risk'
            WHEN piotroski_score <= 3                 THEN 'structural_risk'
            WHEN roic_signo = -1
             AND roic_confiable                       THEN 'structural_risk'
            WHEN quality_score >= 70
             AND value_score >= 40
             AND altman_z_score >= 2.99
             AND piotroski_score >= 6
             AND roic_signo = 1                       THEN 'structural_quality'
            WHEN quality_score >= 70
             AND altman_z_score >= 2.99               THEN 'solid_but_expensive'
            WHEN quality_score >= 50
             AND roic_signo = 1
             AND deuda_signo = -1                     THEN 'improving'
            ELSE                                           'structural_neutral'
        END AS contexto
    FROM tendencia
),

-- 3. TIMING TÉCNICO
timing AS (
    SELECT *,
        CASE
            WHEN rsi_14_semanal > 75                  THEN 'overbought'
            WHEN rsi_14_semanal BETWEEN 40 AND 65
             AND precio_vs_ma200 > 0
             AND volume_ratio_20d >= 0.8              THEN 'good_entry'
            WHEN rsi_14_semanal < 40
             AND precio_vs_ma200 > 0                  THEN 'pullback_in_uptrend'
            WHEN precio_vs_ma200 < -5                 THEN 'below_ma200'
            ELSE                                           'neutral'
        END AS timing
    FROM contexto
),

-- 4. AJUSTE MACRO
macro_ajuste AS (
    SELECT *,
        CASE estado_macro_actual
            WHEN 'EXPANSION'   THEN 1.10
            WHEN 'RECOVERY'    THEN 1.05
            WHEN 'SLOWDOWN'    THEN 0.85
            WHEN 'CONTRACTION' THEN 0.70
            ELSE                    1.00
        END AS macro_factor,
        CASE
            WHEN estado_macro_actual IN ('CONTRACTION', 'SLOWDOWN') THEN TRUE
            ELSE FALSE
        END AS preferir_income
    FROM timing
),

-- 5. INSTRUMENTACIÓN
instrumentacion AS (
    SELECT *,
        CASE
            WHEN contexto = 'structural_risk'         THEN 'none'
            WHEN timing = 'below_ma200'               THEN 'none'

            WHEN preferir_income
             AND contexto IN ('structural_quality',
                              'solid_but_expensive',
                              'improving')            THEN 'cash_secured_put'

            WHEN contexto = 'structural_quality'
             AND timing IN ('good_entry',
                            'pullback_in_uptrend')
             AND obv_slope > 0                        THEN 'stock'

            WHEN contexto = 'structural_quality'
             AND timing = 'overbought'                THEN 'cash_secured_put'

            WHEN contexto IN ('solid_but_expensive',
                              'improving')            THEN 'cash_secured_put'

            ELSE                                           'none'
        END AS instrumento
    FROM macro_ajuste
),

-- 6. SIZING
sizing AS (
    SELECT *,
        CASE
            WHEN instrumento = 'none' THEN 0.0
            ELSE ROUND(LEAST(1.0,
                (quality_score / 100.0 * 0.60
               + value_score   / 100.0 * 0.40)

                * CASE
                    WHEN piotroski_score >= 7 THEN 1.10
                    WHEN piotroski_score >= 5 THEN 1.00
                    ELSE                           0.85
                  END

                * CASE tendencia_fundamental
                    WHEN 'mejora_estructural' THEN 1.10
                    WHEN 'mejora_parcial'     THEN 1.05
                    WHEN 'deterioro'          THEN 0.85
                    ELSE                           1.00
                  END

                * CASE timing
                    WHEN 'good_entry'          THEN 1.00
                    WHEN 'pullback_in_uptrend' THEN 0.90
                    WHEN 'neutral'             THEN 0.80
                    WHEN 'overbought'          THEN 0.70
                    ELSE                            0.60
                  END

                * macro_factor
            ), 2)
        END AS target_position_size
    FROM instrumentacion
),

-- 7. SCORE DE CONVICCIÓN
conviccion AS (
    SELECT *,
        ROUND(
            (quality_score * 0.35 + value_score * 0.15)
            + CASE timing
                WHEN 'good_entry'          THEN 30
                WHEN 'pullback_in_uptrend' THEN 25
                WHEN 'macro_defensivo'     THEN 15
                WHEN 'overbought'          THEN 10
                ELSE                           12
              END
            + CASE
                WHEN piotroski_score >= 7 THEN 15
                WHEN piotroski_score >= 5 THEN 10
                ELSE                           5
              END
            + CASE tendencia_fundamental
                WHEN 'mejora_estructural' THEN 10
                WHEN 'mejora_parcial'     THEN 5
                ELSE                          0
              END
            + CASE WHEN sector_alineado = 'ALIGNED' THEN 5 ELSE 0 END
        , 1) AS score_conviccion
    FROM sizing
),

-- 8. RANKING Y FLAG TIMING
final AS (
    SELECT *,
        RANK() OVER (ORDER BY score_conviccion DESC) AS rank_conviccion,
        CASE
            WHEN timing = 'good_entry'
             AND obv_slope > 0                    THEN 'tecnico_confirmado'
            WHEN timing = 'pullback_in_uptrend'   THEN 'pullback_comprable'
            WHEN timing = 'overbought'            THEN 'esperar_pullback'
            WHEN preferir_income                  THEN 'macro_defensivo'
            ELSE                                       'fundamental_only'
        END AS flag_timing,
        CASE
            WHEN instrumento = 'none' THEN 'no_trade'
            ELSE                           'active'
        END AS trade_status
    FROM conviccion
    WHERE instrumento != 'none'
)

SELECT
    ticker, snapshot_date,
    sector, industry, market_cap_tier,
    multifactor_score, multifactor_rank,
    contexto, timing,
    estado_macro_actual AS estado_macro, macro_factor, sector_alineado,
    CASE
        WHEN instrumento = 'stock'            THEN 'long_core'
        WHEN instrumento = 'cash_secured_put' THEN 'income_core'
        ELSE                                       'none'
    END AS exposicion,
    instrumento, flag_timing,
    target_position_size,
    score_conviccion,
    rank_conviccion::SMALLINT,
    CASE contexto
        WHEN 'structural_quality'  THEN 'Riesgo mercado general'
        WHEN 'solid_but_expensive' THEN 'Riesgo corrección por valuación'
        WHEN 'improving'           THEN 'Riesgo reversión de tendencia'
        ELSE                            'Riesgo debilidad estructural'
    END AS riesgo_principal,
    'Macro:' || estado_macro_actual
        || ' | Q:' || quality_score
        || ' | V:' || value_score
        || ' | Altman:' || COALESCE(altman_z_score::TEXT, 'N/A')
        || ' | Piotroski:' || COALESCE(piotroski_score::TEXT, 'N/A')
        || ' | RSI:' || COALESCE(ROUND(rsi_14_semanal, 1)::TEXT, 'N/A')
        || ' | MA200:' || COALESCE(ROUND(precio_vs_ma200, 1)::TEXT, 'N/A')
        || ' | Tend:' || tendencia_fundamental
        || ' | Timing:' || timing
        AS notas,
    trade_status,
    'v5.0' AS agent_version

FROM final

ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
    contexto             = EXCLUDED.contexto,
    timing               = EXCLUDED.timing,
    instrumento          = EXCLUDED.instrumento,
    flag_timing          = EXCLUDED.flag_timing,
    score_conviccion     = EXCLUDED.score_conviccion,
    rank_conviccion      = EXCLUDED.rank_conviccion,
    target_position_size = EXCLUDED.target_position_size,
    macro_factor         = EXCLUDED.macro_factor,
    exposicion           = EXCLUDED.exposicion,
    riesgo_principal     = EXCLUDED.riesgo_principal,
    notas                = EXCLUDED.notas,
    trade_status         = EXCLUDED.trade_status,
    agent_version        = EXCLUDED.agent_version,
    actualizado_en       = NOW()
"""

# ── TOP 25 ────────────────────────────────────────────────────────────────────
# Parametrizado con %(snapshot_date)s y %(run_id)s para evitar depender
# de MAX(snapshot_date) después del commit — usa el valor conocido.
INSERT_TOP = """
INSERT INTO agente.top (
    ticker, snapshot_date,
    sector, industry, market_cap_tier,
    quality_score, value_score, multifactor_score,
    score_conviccion, rank_conviccion,
    contexto, instrumento, flag_timing,
    sector_alineado, target_position_size,
    rsi_14_semanal, precio_vs_ma200, volume_ratio_20d,
    altman_z_score, piotroski_score,
    roic_signo, roic_confiable
)
SELECT
    d.ticker, d.snapshot_date,
    e.sector, e.industry, e.market_cap_tier,
    e.quality_score, e.value_score, e.multifactor_score,
    d.score_conviccion, d.rank_conviccion,
    d.contexto, d.instrumento, d.flag_timing,
    d.sector_alineado, d.target_position_size,
    e.rsi_14_semanal, e.precio_vs_ma200, e.volume_ratio_20d,
    e.altman_z_score, e.piotroski_score,
    e.roic_signo, e.roic_confiable
FROM agente.decision d
JOIN seleccion.enriquecimiento e
    ON  e.ticker        = d.ticker
    AND e.snapshot_date = d.snapshot_date
WHERE d.snapshot_date = %(snapshot_date)s
  AND d.trade_status   = 'active'
ORDER BY d.rank_conviccion
LIMIT 25
ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
    score_conviccion     = EXCLUDED.score_conviccion,
    rank_conviccion      = EXCLUDED.rank_conviccion,
    instrumento          = EXCLUDED.instrumento,
    flag_timing          = EXCLUDED.flag_timing,
    target_position_size = EXCLUDED.target_position_size,
    actualizado_en       = NOW()
"""

# ── QUERIES DE RESUMEN ────────────────────────────────────────────────────────
SQL_RESUMEN = """
    SELECT
        instrumento,
        COUNT(*) AS n
    FROM agente.decision
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
      AND trade_status = 'active'
    GROUP BY instrumento
    ORDER BY instrumento
"""

SQL_CONTEXTOS = """
    SELECT contexto, COUNT(*) AS n
    FROM agente.decision
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
      AND trade_status = 'active'
    GROUP BY contexto
    ORDER BY n DESC
"""

SQL_FLAGS = """
    SELECT flag_timing, COUNT(*) AS n
    FROM agente.decision
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
      AND trade_status = 'active'
    GROUP BY flag_timing
    ORDER BY n DESC
"""

SQL_TOP10 = """
    SELECT
        d.ticker, d.sector, d.instrumento,
        d.contexto, d.flag_timing, d.sector_alineado,
        d.score_conviccion, d.rank_conviccion, d.target_position_size,
        e.rsi_14_semanal, e.altman_z_score, e.piotroski_score
    FROM agente.decision d
    JOIN seleccion.enriquecimiento e
        ON  e.ticker        = d.ticker
        AND e.snapshot_date = d.snapshot_date
    WHERE d.snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
      AND d.trade_status = 'active'
    ORDER BY d.rank_conviccion
    LIMIT 10
"""

LOG_SQL = """
    INSERT INTO infraestructura.update_logs
        (schema_name, table_name, ticker, status, message)
    VALUES (%s, %s, %s, %s, %s)
"""


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*65}")
    print(f"  AGENTE DECISION — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id        : {RUN_ID}")
    print(f"  snapshot_date : {SNAPSHOT_DATE}  (primer día del mes)")
    print(f"{'='*65}\n")

    conn = get_conn()

    # ── Paso 1: leer macro para display
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_MACRO)
        macro = cur.fetchone()

    estado_macro  = macro["estado_macro"]  if macro else "—"
    score_riesgo  = macro["score_riesgo"]  if macro else "—"
    print(f"  Estado macro  : {estado_macro}  (score_riesgo={score_riesgo})\n")

    # ── Paso 2: ejecutar motor determinístico
    print("  [1/4] Ejecutando motor determinístico (CTEs v5.0)...")
    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_DECISION)
            n_decision = cur.rowcount
        conn.commit()
        log.info(f"agente.decision: {n_decision} filas insertadas/actualizadas")
        print(f"         {n_decision} señales generadas\n")
    except Exception as e:
        log.error(f"Error en INSERT agente.decision: {e}")
        conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute(LOG_SQL, ("agente", "decision", "BULK", "fail", str(e)))
            conn.commit()
        except Exception:
            pass
        conn.close()
        return

    # ── Paso 3: insertar top 25 (DESPUÉS del commit de agente.decision)
    print("  [2/4] Insertando top 25 en agente.top...")
    n_top = 0
    try:
        params_top = {"snapshot_date": SNAPSHOT_DATE}
        with conn.cursor() as cur:
            cur.execute(INSERT_TOP, params_top)
            n_top = cur.rowcount
        conn.commit()
        log.info(f"agente.top: {n_top} filas insertadas/actualizadas — snapshot={SNAPSHOT_DATE}")
        print(f"         {n_top} empresas insertadas en agente.top\n")
    except Exception as e:
        log.error(f"Error en INSERT agente.top: {e}")
        conn.rollback()
        try:
            with conn.cursor() as cur:
                cur.execute(LOG_SQL, ("agente", "top", "BULK", "fail", str(e)))
            conn.commit()
        except Exception:
            pass

    # ── Paso 4: resumen
    print("  [3/4] Generando resumen...\n")

    with conn.cursor() as cur:
        cur.execute(SQL_RESUMEN)
        por_instrumento = cur.fetchall()
        cur.execute(SQL_CONTEXTOS)
        por_contexto = cur.fetchall()
        cur.execute(SQL_FLAGS)
        por_flag = cur.fetchall()
        cur.execute(SQL_TOP10)
        top10 = cur.fetchall()

    n_total  = sum(r[1] for r in por_instrumento)
    n_stock  = next((r[1] for r in por_instrumento if r[0] == "stock"), 0)
    n_csp    = next((r[1] for r in por_instrumento if r[0] == "cash_secured_put"), 0)

    print(f"  {'─'*65}")
    print(f"  SEÑALES ACTIVAS   : {n_total}")
    print(f"    stock           : {n_stock}")
    print(f"    cash_secured_put: {n_csp}")
    print()

    print(f"  Por contexto:")
    for row in por_contexto:
        print(f"    {row[0]:<25} {row[1]:>4}")
    print()

    print(f"  Por flag_timing:")
    for row in por_flag:
        print(f"    {row[0]:<25} {row[1]:>4}")
    print()

    print(f"  Top 10 por score_conviccion:")
    print(f"  {'─'*65}")
    print(f"  {'RK':>3}  {'TICKER':<7}  {'INSTR':<17}  "
          f"{'CONV':>5}  {'RSI_W':>5}  {'ALTMAN':>7}  {'PIOT':>4}  FLAG")
    print(f"  {'─'*65}")
    for r in top10:
        ticker, sector, instr, ctx, flag, alin, conv, rk, size, rsi, altman, piot = r
        print(
            f"  {rk:>3}  {ticker:<7}  {(instr or '—'):<17}  "
            f"{float(conv or 0):>5.1f}  "
            f"{float(rsi or 0):>5.1f}  "
            f"{float(altman or 0):>7.2f}  "
            f"{piot or '—':>4}  "
            f"{flag or '—'}"
        )
    print(f"  {'─'*65}")

    # ── Paso 4: logging
    print("\n  [4/4] Registrando en infraestructura.update_logs...")
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_SQL, (
                "agente", "decision", "BULK", "success",
                f"{n_total} señales activas — stock={n_stock} csp={n_csp} — {estado_macro}"
            ))
            cur.execute(LOG_SQL, (
                "agente", "top", "BULK", "success",
                f"top {n_top} por score_conviccion — snapshot {SNAPSHOT_DATE}"
            ))
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log: {e}")
        conn.rollback()

    conn.close()

    print(f"\n{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Log: {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(
        f"Fin — señales={n_total} stock={n_stock} csp={n_csp} "
        f"macro={estado_macro} snapshot={SNAPSHOT_DATE}"
    )


if __name__ == "__main__":
    main()
