#!/usr/bin/env python3
"""
etf_signal.py

Lee sector.sector_snapshot y genera señales por ETF.
Motor determinístico en Python — CERO llamadas a API.

Output: etf.signal (ON CONFLICT DO UPDATE por ticker + snapshot_date)

Ejecución: python etf/etf_signal.py
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
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5433")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M")

today = date.today()
SNAPSHOT_DATE = date(today.year, today.month, 1)

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output_ingest")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"etf_signal_{today.isoformat()}.log")

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
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT,
    )

# ── PASO 1: Leer estado macro ──────────────────────────────────────────────────
def leer_estado_macro(conn) -> tuple[str, float | None]:
    sql = """
        SELECT estado_macro, score_riesgo
        FROM macro.macro_diagnostico
        ORDER BY calculado_en DESC LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if row:
            return str(row[0]), row[1]
        return "DESCONOCIDO", None

# ── PASO 2: Leer snapshot de ETFs ─────────────────────────────────────────────
def leer_snapshot(conn) -> list[dict]:
    sql = """
        SELECT r.ticker, r.industria, e.tipo, e.nombre,
               r.estado, r.alineacion_macro,
               r.rsi_rs_semanal, r.rs_vs_spy, r.rs_percentil,
               r.ret_1m, r.ret_3m, r.ret_6m,
               r.score_total
        FROM sector.v_sector_ranking r
        JOIN sector.sector_etfs e ON e.ticker = r.ticker
        WHERE e.activo = TRUE
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]

# ── PASO 3: Motor determinístico ───────────────────────────────────────────────

BONUS_MACRO = {
    "SLOWDOWN": {
        "refugio":        15,
        "renta_fija":     10,
        "sector_gics":     5,
        "commodity":       5,
        "industria":      -5,
        "internacional": -10,
    },
    "CONTRACTION": {
        "refugio":        20,
        "renta_fija":     15,
        "sector_gics":    -5,
        "commodity":      -5,
        "industria":     -10,
        "internacional": -15,
    },
    "EXPANSION": {
        "refugio":       -10,
        "renta_fija":     -5,
        "sector_gics":     0,
        "commodity":       5,
        "industria":       5,
        "internacional":  10,
    },
    "RECOVERY": {
        "refugio":       -10,
        "renta_fija":     -5,
        "sector_gics":     0,
        "commodity":      10,
        "industria":       5,
        "internacional":  15,
    },
}


def calcular_score(row: dict, estado_macro: str) -> tuple[float, float]:
    """Devuelve (score_final, score_tecnico_base)."""
    rs_pct = float(row.get("rs_percentil") or 0)
    rsi    = float(row.get("rsi_rs_semanal") or 50)
    ret3m  = float(row.get("ret_3m") or 0)
    alin   = str(row.get("alineacion_macro") or "")

    score_base = (
        rs_pct * 0.40
        + (rsi / 100) * 30
        + (min(max(ret3m, -30), 30) / 30 * 50 + 50) / 100 * 20
        + (10 if alin == "ALIGNED" else 0)
    )

    tipo  = str(row.get("tipo") or "industria")
    bonus = BONUS_MACRO.get(estado_macro, {}).get(tipo, 0)

    score_final = round(min(max(score_base + bonus, 0), 100), 2)
    score_base  = round(min(max(score_base, 0), 100), 2)
    return score_final, score_base


def calcular_señal(row: dict, estado_macro: str) -> str:
    rsi    = float(row.get("rsi_rs_semanal") or 50)
    estado = str(row.get("estado") or "NEUTRAL")
    score  = float(row.get("score_final") or 0)
    alin   = str(row.get("alineacion_macro") or "")

    # Eliminatorios hard
    if rsi > 75:
        return "ESPERAR_PULLBACK"
    if estado == "LAGGING" and rsi < 35:
        return "EVITAR"

    # Señales positivas basadas en score_final
    if score >= 75 and estado == "LEADING_STRONG":
        return "COMPRAR"
    if score >= 75 and estado == "LEADING_WEAK":
        return "INTERESANTE"
    if score >= 60 and estado in ("LEADING_STRONG", "LEADING_WEAK"):
        return "INTERESANTE"
    if score >= 45 and estado in ("LEADING_STRONG", "LEADING_WEAK"):
        return "MONITOREAR"
    if score >= 45 and alin == "ALIGNED":
        return "MONITOREAR"

    # Negativas
    if estado == "LAGGING":
        return "EVITAR"

    return "NEUTRAL"


def calcular_razon(row: dict, señal: str, estado_macro: str) -> str:
    rsi   = float(row.get("rsi_rs_semanal") or 0)
    ret3m = float(row.get("ret_3m") or 0)
    tipo  = str(row.get("tipo") or "")
    bonus = BONUS_MACRO.get(estado_macro, {}).get(tipo, 0)
    score = float(row.get("score_final") or 0)

    if bonus > 0:
        contexto_macro = f" Favorecido por contexto {estado_macro}."
    elif bonus < 0:
        contexto_macro = f" Penalizado por contexto {estado_macro}."
    else:
        contexto_macro = ""

    razones = {
        "COMPRAR":          f"Liderando el mercado con RSI {rsi:.0f} y retorno 3M de {ret3m:.1f}%.{contexto_macro}",
        "INTERESANTE":      f"Buen momentum (RSI {rsi:.0f}) con score {score:.0f}/100.{contexto_macro}",
        "ESPERAR_PULLBACK": f"Sobrecomprado (RSI {rsi:.0f}). Esperar corrección antes de entrar.",
        "MONITOREAR":       f"Momentum moderado. Score {score:.0f}/100.{contexto_macro}",
        "EVITAR":           f"Rezagado vs mercado. RSI {rsi:.0f}, retorno 3M {ret3m:.1f}%.{contexto_macro}",
        "NEUTRAL":          f"Sin señal clara. Score {score:.0f}/100.{contexto_macro}",
    }
    return razones.get(señal, "")

# ── PASO 4: INSERT ─────────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO etf.signal (
    ticker, snapshot_date, run_id,
    señal, score, score_tecnico, estado_macro, razon
)
VALUES (
    %(ticker)s, %(snapshot_date)s, %(run_id)s,
    %(señal)s, %(score)s, %(score_tecnico)s, %(estado_macro)s, %(razon)s
)
ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
    señal         = EXCLUDED.señal,
    score         = EXCLUDED.score,
    score_tecnico = EXCLUDED.score_tecnico,
    estado_macro  = EXCLUDED.estado_macro,
    razon         = EXCLUDED.razon,
    run_id        = EXCLUDED.run_id
"""

# ── Log infraestructura ────────────────────────────────────────────────────────
def registrar_log(conn, status: str, message: str):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO infraestructura.update_logs "
                "(schema_name, table_name, ticker, status, message) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("etf", "signal", "BULK", status, message),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log: {e}")
        conn.rollback()

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*65}")
    print(f"  ETF SIGNAL — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id:        {RUN_ID}")
    print(f"  snapshot_date: {SNAPSHOT_DATE}")
    print(f"{'='*65}\n")

    conn = get_conn()

    # Paso 1
    estado_macro, score_riesgo = leer_estado_macro(conn)
    log.info(f"Estado macro: {estado_macro} | Score riesgo: {score_riesgo}")
    print(f"  Estado macro:  {estado_macro}")
    print(f"  Score riesgo:  {score_riesgo}\n")

    # Paso 2
    etfs = leer_snapshot(conn)
    log.info(f"ETFs en snapshot: {len(etfs)}")

    if not etfs:
        print("  ✗ Sin datos en sector.sector_snapshot. Corré sector_precios.py primero.")
        registrar_log(conn, "fail", "Sin datos en sector_snapshot")
        conn.close()
        return

    print(f"  ETFs leídos:   {len(etfs)}\n")

    # Paso 3 — Motor
    resultados = []
    for row in etfs:
        score_final, score_base = calcular_score(row, estado_macro)
        row["score_final"] = score_final
        señal = calcular_señal(row, estado_macro)
        razon = calcular_razon(row, señal, estado_macro)
        resultados.append({
            "ticker":        row["ticker"],
            "snapshot_date": SNAPSHOT_DATE,
            "run_id":        RUN_ID,
            "señal":         señal,
            "score":         score_final,
            "score_tecnico": score_base,
            "estado_macro":  estado_macro,
            "razon":         razon,
        })

    # Paso 4 — INSERT
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, INSERT_SQL, resultados)
        conn.commit()
        log.info(f"{len(resultados)} filas insertadas/actualizadas en etf.signal")
    except Exception as e:
        log.error(f"Error en INSERT: {e}")
        conn.rollback()
        registrar_log(conn, "fail", str(e))
        conn.close()
        raise

    # Paso 5 — Resumen
    ORDEN_SEÑAL = ["COMPRAR", "INTERESANTE", "MONITOREAR",
                   "NEUTRAL", "ESPERAR_PULLBACK", "EVITAR"]

    dist: dict[str, int] = {}
    for r in resultados:
        dist[r["señal"]] = dist.get(r["señal"], 0) + 1

    print(f"  {'─'*55}")
    print(f"  Total ETFs procesados: {len(resultados)}")
    print(f"\n  Distribución por señal:")
    for señal in ORDEN_SEÑAL:
        n = dist.get(señal, 0)
        bar = "█" * n
        print(f"    {señal:<20} {n:>3}  {bar}")

    top5 = sorted(resultados, key=lambda x: x["score"], reverse=True)[:5]
    print(f"\n  Top 5 por score:")
    print(f"    {'Ticker':<8} {'Tipo':<12} {'Señal':<20} {'Score':>6}")
    print(f"    {'─'*50}")
    for r in top5:
        etf_row = next((e for e in etfs if e["ticker"] == r["ticker"]), {})
        tipo    = str(etf_row.get("tipo") or "—")[:11]
        print(f"    {r['ticker']:<8} {tipo:<12} {r['señal']:<20} {r['score']:>6.1f}")

    bot3 = sorted(resultados, key=lambda x: x["score"])[:3]
    print(f"\n  Bottom 3 por score:")
    print(f"    {'─'*50}")
    for r in bot3:
        print(f"    {r['ticker']:<8} {r['señal']:<20} {r['score']:>6.1f}")

    print(f"  {'─'*55}\n")

    registrar_log(conn, "success",
                  f"{len(resultados)} ETFs — estado_macro={estado_macro} run_id={RUN_ID}")
    conn.close()

    print(f"{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
