#!/usr/bin/env python3
"""
motor_opciones.py

Lee agente.decision y agente_opciones.contratos_raw y produce
agente_opciones.trade_decision_opciones.

Flujo:
  1. Lee estado_macro, score_riesgo y VIX desde macro.macro_diagnostico
  2. Calcula regimen_vix en Python
  3. Ejecuta INSERT con CTEs encadenados — todo SQL, sin lógica en Python
  4. Muestra resumen en consola
  5. Registra en infraestructura.update_logs

Sin API calls — SQL puro.

Ejecución: python agente_opciones/motor_opciones.py
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
LOG_FILE = os.path.join(LOG_DIR, f"motor_opciones_{date.today().isoformat()}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
SCHEMA = "agente_opciones"
TABLE  = "trade_decision_opciones"

# ── SQL ────────────────────────────────────────────────────────────────────────
SQL_MACRO = """
    SELECT estado_macro, score_riesgo, vix
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC LIMIT 1
"""

INSERT_DECISION = """
INSERT INTO agente_opciones.trade_decision_opciones (
    ticker, snapshot_date,
    direccion, contexto, tendencia_fundamental,
    estado_macro, regimen_vix, vix,
    nivel_iv, iv_promedio, term_structure, liquidez,
    estrategia, delta_objetivo,
    put_strike, put_delta, put_theta, put_iv, put_dte,
    call_strike, call_delta, call_theta, call_iv,
    sizing, trade_status, notas, agent_version
)

WITH

base AS (
    SELECT d.ticker, d.snapshot_date,
           d.contexto, d.instrumento,
           d.flag_timing, d.sector_alineado,
           d.target_position_size AS sizing_base,
           d.estado_macro,
           d.tendencia_fundamental,
           e.rsi_14_semanal, e.precio_vs_ma200,
           e.vol_realizada_30d
    FROM agente.decision d
    JOIN seleccion.enriquecimiento e
        ON  e.ticker        = d.ticker
        AND e.snapshot_date = d.snapshot_date
    WHERE d.trade_status = 'active'
      AND d.snapshot_date = (SELECT MAX(snapshot_date)
                              FROM agente.decision)
),

iv_por_ticker AS (
    SELECT ticker,
           ROUND(AVG(iv)::NUMERIC, 4) AS iv_promedio,
           CASE
               WHEN AVG(iv) < 0.20 THEN 'baja'
               WHEN AVG(iv) < 0.40 THEN 'media'
               ELSE                     'alta'
           END AS nivel_iv
    FROM agente_opciones.contratos_raw
    WHERE fecha = CURRENT_DATE
      AND iv IS NOT NULL
    GROUP BY ticker
),

term_structure AS (
    SELECT ticker,
           CASE
               WHEN AVG(CASE WHEN dte <= 35 THEN iv END)
                  > AVG(CASE WHEN dte >  35 THEN iv END)
                   THEN 'backwardation'
               WHEN AVG(CASE WHEN dte <= 35 THEN iv END)
                  < AVG(CASE WHEN dte >  35 THEN iv END)
                   THEN 'contango'
               ELSE 'flat'
           END AS term_structure
    FROM agente_opciones.contratos_raw
    WHERE fecha = CURRENT_DATE
    GROUP BY ticker
),

liquidez AS (
    SELECT ticker,
           CASE
               WHEN MAX(oi) FILTER (WHERE contract_type = 'put') >= 50
                   THEN 'liquido'
               WHEN MAX(oi) FILTER (WHERE contract_type = 'put') >= 10
                   THEN 'semi_liquido'
               ELSE 'iliquido'
           END AS liquidez
    FROM agente_opciones.contratos_raw
    WHERE fecha = CURRENT_DATE
    GROUP BY ticker
),

mejor_put AS (
    SELECT DISTINCT ON (ticker)
        ticker,
        strike AS put_strike,
        delta  AS put_delta,
        theta  AS put_theta,
        iv     AS put_iv,
        dte    AS put_dte
    FROM agente_opciones.contratos_raw
    WHERE contract_type = 'put'
      AND fecha = CURRENT_DATE
      AND oi >= 5
    ORDER BY ticker, ABS(theta) DESC NULLS LAST
),

mejor_call AS (
    SELECT DISTINCT ON (ticker)
        ticker,
        strike AS call_strike,
        delta  AS call_delta,
        theta  AS call_theta,
        iv     AS call_iv
    FROM agente_opciones.contratos_raw
    WHERE contract_type = 'call'
      AND fecha = CURRENT_DATE
      AND oi >= 5
    ORDER BY ticker, ABS(theta) DESC NULLS LAST
),

joined AS (
    SELECT
        b.*,
        iv.nivel_iv,
        iv.iv_promedio,
        ts.term_structure,
        lq.liquidez,
        mp.put_strike, mp.put_delta, mp.put_theta,
        mp.put_iv,     mp.put_dte,
        mc.call_strike, mc.call_delta,
        mc.call_theta,  mc.call_iv
    FROM base b
    LEFT JOIN iv_por_ticker  iv ON iv.ticker = b.ticker
    LEFT JOIN term_structure ts ON ts.ticker = b.ticker
    LEFT JOIN liquidez       lq ON lq.ticker = b.ticker
    LEFT JOIN mejor_put      mp ON mp.ticker = b.ticker
    LEFT JOIN mejor_call     mc ON mc.ticker = b.ticker
),

estrategia AS (
    SELECT *,
        CASE
            WHEN liquidez = 'iliquido'       THEN 'no_trade'
            WHEN nivel_iv = 'baja'           THEN 'no_trade'
            WHEN put_strike IS NULL          THEN 'no_trade'

            WHEN estado_macro = 'CONTRACTION'
                THEN 'cash_secured_put'

            WHEN estado_macro = 'SLOWDOWN'
             AND nivel_iv IN ('media', 'alta')
             AND contexto IN ('structural_quality',
                              'solid_but_expensive')
                THEN 'bull_put_spread'

            WHEN estado_macro = 'SLOWDOWN'
                THEN 'cash_secured_put'

            WHEN estado_macro IN ('EXPANSION', 'RECOVERY')
             AND nivel_iv = 'alta'
             AND call_strike IS NOT NULL
                THEN 'jade_lizard'

            WHEN estado_macro IN ('EXPANSION', 'RECOVERY')
             AND nivel_iv IN ('media', 'alta')
                THEN 'bull_put_spread'

            WHEN term_structure = 'backwardation'
             AND nivel_iv IN ('media', 'alta')
                THEN 'calendar_spread'

            ELSE 'cash_secured_put'
        END AS estrategia
    FROM joined
),

delta_obj AS (
    SELECT *,
        CASE estrategia
            WHEN 'cash_secured_put' THEN 0.30
            WHEN 'bull_put_spread'  THEN 0.25
            WHEN 'iron_condor'      THEN 0.20
            WHEN 'jade_lizard'      THEN 0.35
            WHEN 'calendar_spread'  THEN 0.25
            ELSE NULL
        END AS delta_objetivo,

        CASE
            WHEN estrategia = 'no_trade'                       THEN NULL
            WHEN flag_timing IN ('tecnico_confirmado',
                                 'pullback_comprable')         THEN 30
            WHEN flag_timing = 'fundamental_only'              THEN 45
            WHEN flag_timing = 'macro_defensivo'               THEN 38
            ELSE                                                    40
        END AS dte_objetivo

    FROM estrategia
),

sizing_final AS (
    SELECT *,
        CASE
            WHEN estrategia = 'no_trade' THEN 0.0
            ELSE ROUND(LEAST(1.0,
                sizing_base

                * CASE %(regimen_vix)s
                    WHEN 'panico'       THEN 1.20
                    WHEN 'elevado'      THEN 1.10
                    WHEN 'normal'       THEN 1.00
                    WHEN 'complacencia' THEN 0.80
                    ELSE                     1.00
                  END

                * CASE liquidez
                    WHEN 'semi_liquido' THEN 0.50
                    ELSE                     1.00
                  END

                * CASE tendencia_fundamental
                    WHEN 'mejora_estructural' THEN 1.10
                    WHEN 'deterioro'          THEN 0.85
                    ELSE                           1.00
                  END
            )::NUMERIC, 2)
        END AS sizing_ajustado
    FROM delta_obj
),

direccion_cte AS (
    SELECT *,
        CASE contexto
            WHEN 'structural_quality'  THEN 'alcista'
            WHEN 'solid_but_expensive' THEN 'neutral_bajista'
            WHEN 'improving'           THEN 'alcista'
            ELSE                            'neutral'
        END AS direccion_final
    FROM sizing_final
),

final AS (
    SELECT *,
        'Macro:' || estado_macro
            || ' | VIX:'     || %(vix)s
            || ' | Régimen:' || %(regimen_vix)s
            || ' | IV:'      || COALESCE(nivel_iv, 'N/A')
            || ' | Liquidez:'|| COALESCE(liquidez, 'N/A')
            || ' | Tend:'    || COALESCE(tendencia_fundamental, 'N/A')
            AS notas
    FROM direccion_cte
    WHERE estrategia != 'no_trade'
)

SELECT
    ticker, snapshot_date,
    direccion_final, contexto, tendencia_fundamental,
    estado_macro,
    %(regimen_vix)s AS regimen_vix,
    %(vix)s         AS vix,
    nivel_iv, iv_promedio, term_structure, liquidez,
    estrategia, delta_objetivo,
    put_strike, put_delta, put_theta, put_iv, put_dte,
    call_strike, call_delta, call_theta, call_iv,
    sizing_ajustado, 'active', notas, 'v3.0'
FROM final

ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
    direccion             = EXCLUDED.direccion,
    contexto              = EXCLUDED.contexto,
    tendencia_fundamental = EXCLUDED.tendencia_fundamental,
    estado_macro          = EXCLUDED.estado_macro,
    regimen_vix           = EXCLUDED.regimen_vix,
    vix                   = EXCLUDED.vix,
    nivel_iv              = EXCLUDED.nivel_iv,
    iv_promedio           = EXCLUDED.iv_promedio,
    term_structure        = EXCLUDED.term_structure,
    liquidez              = EXCLUDED.liquidez,
    estrategia            = EXCLUDED.estrategia,
    delta_objetivo        = EXCLUDED.delta_objetivo,
    put_strike            = EXCLUDED.put_strike,
    put_delta             = EXCLUDED.put_delta,
    put_theta             = EXCLUDED.put_theta,
    put_iv                = EXCLUDED.put_iv,
    put_dte               = EXCLUDED.put_dte,
    call_strike           = EXCLUDED.call_strike,
    call_delta            = EXCLUDED.call_delta,
    call_theta            = EXCLUDED.call_theta,
    call_iv               = EXCLUDED.call_iv,
    sizing                = EXCLUDED.sizing,
    notas                 = EXCLUDED.notas,
    agent_version         = EXCLUDED.agent_version,
    actualizado_en        = NOW()
"""

SQL_RESUMEN_ESTRATEGIA = """
    SELECT estrategia, COUNT(*) AS n
    FROM agente_opciones.trade_decision_opciones
    WHERE snapshot_date = (SELECT MAX(snapshot_date)
                           FROM agente_opciones.trade_decision_opciones)
    GROUP BY estrategia
    ORDER BY n DESC
"""

SQL_RESUMEN_IV = """
    SELECT nivel_iv, COUNT(*) AS n
    FROM agente_opciones.trade_decision_opciones
    WHERE snapshot_date = (SELECT MAX(snapshot_date)
                           FROM agente_opciones.trade_decision_opciones)
    GROUP BY nivel_iv
    ORDER BY n DESC
"""

SQL_TOP10 = """
    SELECT ticker, estrategia, nivel_iv, liquidez,
           put_strike, put_delta, put_theta, put_dte,
           sizing, notas
    FROM agente_opciones.trade_decision_opciones
    WHERE snapshot_date = (SELECT MAX(snapshot_date)
                           FROM agente_opciones.trade_decision_opciones)
      AND trade_status = 'active'
    ORDER BY sizing DESC NULLS LAST
    LIMIT 10
"""

LOG_SQL = """
    INSERT INTO infraestructura.update_logs
        (schema_name, table_name, ticker, status, message)
    VALUES (%s, %s, %s, %s, %s)
"""


# ── DB ─────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def registrar_log(conn, status: str, message: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_SQL, (SCHEMA, TABLE, "BULK", status, message))
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log: {e}")
        conn.rollback()


# ── Régimen VIX ────────────────────────────────────────────────────────────────
def calcular_regimen_vix(vix: float | None) -> str:
    if vix is None:
        return "normal"
    if vix <= 15:
        return "complacencia"
    if vix <= 25:
        return "normal"
    if vix <= 35:
        return "elevado"
    return "panico"


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*65}")
    print(f"  MOTOR OPCIONES — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id        : {RUN_ID}")
    print(f"  snapshot_date : {SNAPSHOT_DATE}")
    print(f"  destino       : {SCHEMA}.{TABLE}")
    print(f"{'='*65}\n")

    conn = get_conn()

    # ── Paso 1: leer macro y VIX
    print("  [1/4] Leyendo estado macro y VIX...")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_MACRO)
        macro = cur.fetchone()

    if not macro:
        log.error("Sin datos en macro.macro_diagnostico. Ejecutar macro_fred.py primero.")
        conn.close()
        return

    estado_macro = macro["estado_macro"]
    score_riesgo = macro["score_riesgo"]
    vix          = float(macro["vix"]) if macro["vix"] is not None else None
    regimen_vix  = calcular_regimen_vix(vix)

    print(f"  → Estado macro : {estado_macro}  (score_riesgo={score_riesgo})")
    print(f"  → VIX          : {vix}")
    print(f"  → Régimen VIX  : {regimen_vix}\n")

    log.info(f"Macro: {estado_macro} | VIX: {vix} | Régimen: {regimen_vix}")

    # ── Paso 2: ejecutar motor (INSERT con CTEs)
    print("  [2/4] Ejecutando motor de opciones (CTEs v3.0)...")

    params = {
        "regimen_vix": regimen_vix,
        "vix":         vix if vix is not None else 0,
    }

    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_DECISION, params)
            n_total = cur.rowcount
        conn.commit()
        log.info(f"trade_decision_opciones: {n_total} filas insertadas/actualizadas")
        print(f"  → {n_total} estrategias generadas\n")
    except Exception as e:
        log.error(f"Error en INSERT trade_decision_opciones: {e}")
        conn.rollback()
        registrar_log(conn, "fail", str(e))
        conn.close()
        raise

    # ── Paso 3: resumen
    print("  [3/4] Generando resumen...\n")

    with conn.cursor() as cur:
        cur.execute(SQL_RESUMEN_ESTRATEGIA)
        por_estrategia = cur.fetchall()

        cur.execute(SQL_RESUMEN_IV)
        por_iv = cur.fetchall()

        cur.execute(SQL_TOP10)
        top10 = cur.fetchall()

    print("  Por estrategia:")
    for row in por_estrategia:
        print(f"    {row[0]:<22} {row[1]:>4}")
    print()

    print("  Por nivel_iv:")
    for row in por_iv:
        print(f"    {str(row[0]):<10} {row[1]:>4}")
    print()

    print("  Top 10 por sizing:")
    print(f"  {'─'*75}")
    print(f"  {'TICKER':<7}  {'ESTRATEGIA':<18}  {'IV':<6}  {'LIQUIDEZ':<11}  "
          f"{'STRIKE':>7}  {'DELTA':>6}  {'THETA':>7}  {'DTE':>4}  {'SIZE':>5}")
    print(f"  {'─'*75}")
    for r in top10:
        ticker, estrategia, nivel_iv, liquidez, \
            put_strike, put_delta, put_theta, put_dte, \
            sizing, notas = r
        print(
            f"  {str(ticker):<7}  "
            f"{str(estrategia or '—'):<18}  "
            f"{str(nivel_iv or '—'):<6}  "
            f"{str(liquidez or '—'):<11}  "
            f"{float(put_strike or 0):>7.2f}  "
            f"{float(put_delta or 0):>6.3f}  "
            f"{float(put_theta or 0):>7.4f}  "
            f"{int(put_dte or 0):>4}  "
            f"{float(sizing or 0):>5.2f}"
        )
    print(f"  {'─'*75}\n")

    # ── Paso 4: logging
    print("  [4/4] Registrando en infraestructura.update_logs...")
    n_por_estrategia = {r[0]: r[1] for r in por_estrategia}
    msg = (
        f"{n_total} estrategias — "
        f"macro={estado_macro} vix={vix} regimen={regimen_vix} — "
        + " ".join(f"{k}={v}" for k, v in n_por_estrategia.items())
    )
    registrar_log(conn, "success", msg)

    conn.close()

    print(f"\n{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Log : {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — total={n_total} macro={estado_macro} vix={vix} regimen={regimen_vix}")


if __name__ == "__main__":
    main()
