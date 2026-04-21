#!/usr/bin/env python3
"""
sector_diagnostico_tecnico.py

Calcula un diagnóstico sectorial técnico comparando defensivos vs cíclicos
usando RSI y volumen de sector.sector_snapshot.
Sin API calls — solo SQL.

Output: sector.sector_diagnostico_tecnico (una fila por fecha, ON CONFLICT DO UPDATE)

Ejecución: python sector/sector_diagnostico_tecnico.py
"""

import os
import logging
import subprocess
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

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output_ingest")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"sector_diagnostico_{date.today().isoformat()}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Clasificación de sectores ──────────────────────────────────────────────────
DEFENSIVOS = ["XLV", "XLP", "XLU", "GLD", "TLT"]
CICLICOS   = ["XLK", "XLY", "XLF", "XLI", "XLE"]
MIXTOS     = ["XLB", "XLRE", "XLC"]

# ── DB ─────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT,
    )

# ── Leer snapshot sectorial ────────────────────────────────────────────────────
def leer_snapshot_sectorial(conn) -> dict:
    tickers      = DEFENSIVOS + CICLICOS + MIXTOS
    placeholders = ",".join(["%s"] * len(tickers))
    sql = f"""
        SELECT
            ticker,
            rsi_rs_semanal,
            rsi_rs_diario,
            ret_3m,
            ret_1m,
            vol_ratio AS volume_ratio_20d,
            obv_slope,
            score_total,
            estado,
            alineacion_macro
        FROM sector.sector_snapshot
        WHERE ticker IN ({placeholders})
          AND run_id = (SELECT MAX(run_id) FROM sector.sector_snapshot)
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tickers)
        rows = cur.fetchall()
    return {r["ticker"]: dict(r) for r in rows}

# ── Leer estado macro ──────────────────────────────────────────────────────────
def leer_estado_macro(conn) -> str | None:
    sql = "SELECT estado_macro FROM macro.macro_diagnostico ORDER BY calculado_en DESC LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None

# ── Score por grupo ────────────────────────────────────────────────────────────
def calcular_score_grupo(snapshot: dict, tickers: list) -> dict:
    rsi_values = []
    vol_values = []
    ret_values = []
    obv_values = []

    for t in tickers:
        if t not in snapshot:
            continue
        d = snapshot[t]
        if d.get("rsi_rs_semanal") is not None:
            rsi_values.append(float(d["rsi_rs_semanal"]))
        if d.get("volume_ratio_20d") is not None:
            vol_values.append(float(d["volume_ratio_20d"]))
        if d.get("ret_3m") is not None:
            ret_values.append(float(d["ret_3m"]))
        if d.get("obv_slope") is not None:
            obv_values.append(float(d["obv_slope"]))

    def avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else None

    return {
        "rsi_promedio": avg(rsi_values),
        "vol_promedio": avg(vol_values),
        "ret_promedio": avg(ret_values),
        "obv_promedio": avg(obv_values),
        "n_tickers":    len(rsi_values),
    }

# ── Top líderes y rezagados ────────────────────────────────────────────────────
def calcular_top_bottom(snapshot: dict, tickers: list) -> tuple[str, str]:
    datos = [
        (t, float(snapshot[t]["rsi_rs_semanal"]))
        for t in tickers
        if t in snapshot and snapshot[t].get("rsi_rs_semanal") is not None
    ]
    datos.sort(key=lambda x: x[1], reverse=True)
    lideres   = ", ".join([t for t, _ in datos[:3]])
    rezagados = ", ".join([t for t, _ in datos[-3:]])
    return lideres, rezagados

# ── Diagnóstico ────────────────────────────────────────────────────────────────
def calcular_diagnostico(def_score: dict, cic_score: dict,
                          estado_macro: str) -> tuple[str, str, str]:
    rsi_def = def_score.get("rsi_promedio") or 50
    rsi_cic = cic_score.get("rsi_promedio") or 50
    vol_def = def_score.get("vol_promedio") or 1.0
    vol_cic = cic_score.get("vol_promedio") or 1.0

    diff_rsi = rsi_def - rsi_cic
    diff_vol = vol_def - vol_cic

    if diff_rsi >= 15 and diff_vol >= 0.1:
        diagnostico = "CONFIRMA_SLOWDOWN"
        nota = (f"Defensivos lideran con RSI {rsi_def:.0f} vs cíclicos {rsi_cic:.0f}. "
                f"Volumen confirma la rotación hacia sectores defensivos.")
    elif diff_rsi >= 10:
        diagnostico = "CONFIRMA_SLOWDOWN"
        nota = (f"Defensivos lideran con RSI {rsi_def:.0f} vs cíclicos {rsi_cic:.0f}. "
                f"Volumen no confirma con fuerza.")
    elif diff_rsi <= -15 and diff_vol <= -0.1:
        diagnostico = "CONFIRMA_EXPANSION"
        nota = (f"Cíclicos lideran con RSI {rsi_cic:.0f} vs defensivos {rsi_def:.0f}. "
                f"Volumen confirma entrada de dinero en sectores de riesgo.")
    elif diff_rsi <= -10:
        diagnostico = "CONFIRMA_EXPANSION"
        nota = (f"Cíclicos lideran con RSI {rsi_cic:.0f} vs defensivos {rsi_def:.0f}. "
                f"Volumen no confirma con fuerza.")
    elif rsi_def >= 70 and rsi_cic >= 70:
        diagnostico = "CONFIRMA_CONTRACTION"
        nota = (f"Todos los sectores en RSI alto. "
                f"Defensivos RSI {rsi_def:.0f}, Cíclicos {rsi_cic:.0f}.")
    else:
        diagnostico = "SEÑAL_MIXTA"
        nota = (f"Sin dirección clara. Defensivos RSI {rsi_def:.0f}, "
                f"Cíclicos RSI {rsi_cic:.0f}. Diferencia insuficiente para señal.")

    coherencia_map = {
        ("SLOWDOWN",    "CONFIRMA_SLOWDOWN"):    "ALTA",
        ("SLOWDOWN",    "SEÑAL_MIXTA"):          "MEDIA",
        ("SLOWDOWN",    "CONFIRMA_EXPANSION"):   "CONTRADICE",
        ("EXPANSION",   "CONFIRMA_EXPANSION"):   "ALTA",
        ("EXPANSION",   "SEÑAL_MIXTA"):          "MEDIA",
        ("EXPANSION",   "CONFIRMA_SLOWDOWN"):    "CONTRADICE",
        ("CONTRACTION", "CONFIRMA_CONTRACTION"): "ALTA",
        ("CONTRACTION", "CONFIRMA_SLOWDOWN"):    "MEDIA",
        ("CONTRACTION", "CONFIRMA_EXPANSION"):   "CONTRADICE",
        ("RECOVERY",    "CONFIRMA_EXPANSION"):   "ALTA",
        ("RECOVERY",    "SEÑAL_MIXTA"):          "MEDIA",
        ("RECOVERY",    "CONFIRMA_SLOWDOWN"):    "CONTRADICE",
    }
    coherencia = coherencia_map.get((estado_macro, diagnostico), "MEDIA")
    return diagnostico, coherencia, nota

# ── INSERT ─────────────────────────────────────────────────────────────────────
INSERT_SQL = """
INSERT INTO sector.sector_diagnostico_tecnico (
    fecha, run_id,
    score_defensivos, score_ciclicos, score_mixtos,
    top_3_lideres, top_3_rezagados,
    vol_defensivos, vol_ciclicos,
    diagnostico_sector, estado_macro, coherencia, nota
)
VALUES (
    %(fecha)s, %(run_id)s,
    %(score_defensivos)s, %(score_ciclicos)s, %(score_mixtos)s,
    %(top_3_lideres)s, %(top_3_rezagados)s,
    %(vol_defensivos)s, %(vol_ciclicos)s,
    %(diagnostico_sector)s, %(estado_macro)s, %(coherencia)s, %(nota)s
)
ON CONFLICT (fecha) DO UPDATE SET
    run_id             = EXCLUDED.run_id,
    score_defensivos   = EXCLUDED.score_defensivos,
    score_ciclicos     = EXCLUDED.score_ciclicos,
    score_mixtos       = EXCLUDED.score_mixtos,
    top_3_lideres      = EXCLUDED.top_3_lideres,
    top_3_rezagados    = EXCLUDED.top_3_rezagados,
    vol_defensivos     = EXCLUDED.vol_defensivos,
    vol_ciclicos       = EXCLUDED.vol_ciclicos,
    diagnostico_sector = EXCLUDED.diagnostico_sector,
    estado_macro       = EXCLUDED.estado_macro,
    coherencia         = EXCLUDED.coherencia,
    nota               = EXCLUDED.nota
"""

# ── Log infraestructura ────────────────────────────────────────────────────────
def registrar_log(conn, status: str, message: str):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO infraestructura.update_logs "
                "(schema_name, table_name, ticker, status, message) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("sector", "sector_diagnostico_tecnico", "BULK", status, message),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log: {e}")
        conn.rollback()

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    # Sin caffeinate — no hay API calls
    print(f"\n{'='*65}")
    print(f"  SECTOR DIAGNÓSTICO TÉCNICO — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id: {RUN_ID}")
    print(f"{'='*65}\n")
    print("  Leyendo desde sector.sector_snapshot — sin API calls\n")

    conn = get_conn()

    snapshot     = leer_snapshot_sectorial(conn)
    estado_macro = leer_estado_macro(conn)

    log.info(f"Tickers en snapshot: {list(snapshot.keys())}")
    log.info(f"Estado macro vigente: {estado_macro}")

    if not snapshot:
        print("  ✗ Sin datos en sector_snapshot. Corré sector_precios.py primero.")
        registrar_log(conn, "fail", "Sin datos en sector_snapshot")
        conn.close()
        return

    def_score = calcular_score_grupo(snapshot, DEFENSIVOS)
    cic_score = calcular_score_grupo(snapshot, CICLICOS)
    mix_score = calcular_score_grupo(snapshot, MIXTOS)

    todos = DEFENSIVOS + CICLICOS + MIXTOS
    top_lideres, top_rezagados = calcular_top_bottom(snapshot, todos)

    diagnostico, coherencia, nota = calcular_diagnostico(def_score, cic_score, estado_macro)

    print(f"  Estado macro:      {estado_macro}")
    print(f"  RSI defensivos:    {def_score['rsi_promedio']} "
          f"(vol: {def_score['vol_promedio']})")
    print(f"  RSI cíclicos:      {cic_score['rsi_promedio']} "
          f"(vol: {cic_score['vol_promedio']})")
    print(f"  RSI mixtos:        {mix_score['rsi_promedio']}")
    print(f"  Top líderes:       {top_lideres}")
    print(f"  Top rezagados:     {top_rezagados}")
    print(f"\n  {'─'*55}")
    print(f"  Diagnóstico:       {diagnostico}")
    print(f"  Coherencia macro:  {coherencia}")
    print(f"  Nota:              {nota}")
    print(f"  {'─'*55}\n")

    payload = {
        "fecha":             date.today(),
        "run_id":            RUN_ID,
        "score_defensivos":  def_score["rsi_promedio"],
        "score_ciclicos":    cic_score["rsi_promedio"],
        "score_mixtos":      mix_score["rsi_promedio"],
        "top_3_lideres":     top_lideres,
        "top_3_rezagados":   top_rezagados,
        "vol_defensivos":    def_score["vol_promedio"],
        "vol_ciclicos":      cic_score["vol_promedio"],
        "diagnostico_sector":diagnostico,
        "estado_macro":      estado_macro,
        "coherencia":        coherencia,
        "nota":              nota,
    }

    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, payload)
        conn.commit()
        print(f"  ✓ Diagnóstico guardado en sector.sector_diagnostico_tecnico")
        registrar_log(conn, "success",
                      f"diagnostico={diagnostico} coherencia={coherencia} run_id={RUN_ID}")
    except Exception as e:
        log.error(f"Error en INSERT: {e}")
        conn.rollback()
        registrar_log(conn, "fail", str(e))
        conn.close()
        raise

    conn.close()

    print(f"\n{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
