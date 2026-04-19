#!/usr/bin/env python3
"""
enriquecer.py

Lee las empresas de seleccion.universo y las enriquece con:
  - Técnicos (RSI, MA200, volatilidad, OBV, momentum)    ← ingest.precios
  - Salud financiera (Altman Z, Piotroski F)              ← ingest.scores_salud
  - Regresiones anuales (ROIC, deuda, IQ, FCF, EV)       ← ingest.keymetrics_hist
  - Conexión macro/sector                                 ← macro + sector schemas

Sin API calls — cálculo 100% local.

Ejecución: python micro/seleccion/enriquecer.py
"""

import os
import logging
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, date
from math import sqrt
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

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M")

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"enriquecer_{date.today().isoformat()}.log")

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
SCHEMA       = "seleccion"
TABLE        = "enriquecimiento"
MIN_DIAS     = 210   # mínimo de días de precios para calcular técnicos
LOG_CADA     = 100

# Mapa sector FMP → ETF SPDR
SECTOR_ETF = {
    "Technology":             "XLK",
    "Healthcare":             "XLV",
    "Consumer Defensive":     "XLP",
    "Utilities":              "XLU",
    "Energy":                 "XLE",
    "Financial Services":     "XLF",
    "Industrials":            "XLI",
    "Basic Materials":        "XLB",
    "Real Estate":            "XLRE",
    "Communication Services": "XLC",
    "Consumer Cyclical":      "XLY",
}

# ── SQL ────────────────────────────────────────────────────────────────────────
SQL_UNIVERSO = """
    SELECT u.ticker, u.snapshot_date, u.sector, u.industry,
           u.market_cap_tier, u.quality_score, u.value_score,
           u.multifactor_score, u.multifactor_rank, u.multifactor_percentile
    FROM seleccion.universo u
    WHERE u.snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.universo)
    ORDER BY u.multifactor_rank
"""

SQL_PRECIOS = """
    SELECT fecha, close_adj, volume
    FROM ingest.precios
    WHERE ticker = %s
      AND close_adj IS NOT NULL
    ORDER BY fecha ASC
"""

SQL_SALUD = """
    SELECT altman_z_score, altman_zona, piotroski_score, piotroski_categoria
    FROM ingest.scores_salud
    WHERE ticker = %s
      AND fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.scores_salud)
"""

SQL_HIST = """
    SELECT fecha_reporte, roic, net_debt_to_ebitda,
           income_quality, fcf_yield, ev_to_ebitda
    FROM ingest.keymetrics_hist
    WHERE ticker = %s
    ORDER BY fecha_reporte ASC
"""

SQL_MACRO = """
    SELECT estado_macro
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC
    LIMIT 1
"""

SQL_ALINEACION = """
    SELECT ticker, alineacion_macro
    FROM sector.v_sector_ranking
    WHERE tipo = 'sector'
"""

INSERT_SQL = """
    INSERT INTO seleccion.enriquecimiento (
        ticker, snapshot_date,
        sector, industry, market_cap_tier,
        quality_score, value_score, multifactor_score,
        multifactor_rank, multifactor_percentile,

        rsi_14_diario, rsi_14_semanal,
        precio_vs_ma200, dist_max_52w,
        vol_realizada_30d, vol_realizada_90d,
        volume_ratio_20d, obv_slope,
        momentum_3m, momentum_6m, momentum_12m,

        altman_z_score, altman_zona,
        piotroski_score, piotroski_categoria,

        roic_tendencia, roic_signo, roic_r2, roic_confiable,
        deuda_tendencia, deuda_signo, deuda_r2, deuda_confiable,

        estado_macro, sector_etf, sector_alineado,
        run_id
    )
    VALUES (
        %(ticker)s, %(snapshot_date)s,
        %(sector)s, %(industry)s, %(market_cap_tier)s,
        %(quality_score)s, %(value_score)s, %(multifactor_score)s,
        %(multifactor_rank)s, %(multifactor_percentile)s,

        %(rsi_14_diario)s, %(rsi_14_semanal)s,
        %(precio_vs_ma200)s, %(dist_max_52w)s,
        %(vol_realizada_30d)s, %(vol_realizada_90d)s,
        %(volume_ratio_20d)s, %(obv_slope)s,
        %(momentum_3m)s, %(momentum_6m)s, %(momentum_12m)s,

        %(altman_z_score)s, %(altman_zona)s,
        %(piotroski_score)s, %(piotroski_categoria)s,

        %(roic_tendencia)s, %(roic_signo)s, %(roic_r2)s, %(roic_confiable)s,
        %(deuda_tendencia)s, %(deuda_signo)s, %(deuda_r2)s, %(deuda_confiable)s,

        %(estado_macro)s, %(sector_etf)s, %(sector_alineado)s,
        %(run_id)s
    )
    ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
        sector              = EXCLUDED.sector,
        industry            = EXCLUDED.industry,
        market_cap_tier     = EXCLUDED.market_cap_tier,
        quality_score       = EXCLUDED.quality_score,
        value_score         = EXCLUDED.value_score,
        multifactor_score   = EXCLUDED.multifactor_score,
        multifactor_rank    = EXCLUDED.multifactor_rank,
        multifactor_percentile = EXCLUDED.multifactor_percentile,
        rsi_14_diario       = EXCLUDED.rsi_14_diario,
        rsi_14_semanal      = EXCLUDED.rsi_14_semanal,
        precio_vs_ma200     = EXCLUDED.precio_vs_ma200,
        dist_max_52w        = EXCLUDED.dist_max_52w,
        vol_realizada_30d   = EXCLUDED.vol_realizada_30d,
        vol_realizada_90d   = EXCLUDED.vol_realizada_90d,
        volume_ratio_20d    = EXCLUDED.volume_ratio_20d,
        obv_slope           = EXCLUDED.obv_slope,
        momentum_3m         = EXCLUDED.momentum_3m,
        momentum_6m         = EXCLUDED.momentum_6m,
        momentum_12m        = EXCLUDED.momentum_12m,
        altman_z_score      = EXCLUDED.altman_z_score,
        altman_zona         = EXCLUDED.altman_zona,
        piotroski_score     = EXCLUDED.piotroski_score,
        piotroski_categoria = EXCLUDED.piotroski_categoria,
        roic_tendencia      = EXCLUDED.roic_tendencia,
        roic_signo          = EXCLUDED.roic_signo,
        roic_r2             = EXCLUDED.roic_r2,
        roic_confiable      = EXCLUDED.roic_confiable,
        deuda_tendencia     = EXCLUDED.deuda_tendencia,
        deuda_signo         = EXCLUDED.deuda_signo,
        deuda_r2            = EXCLUDED.deuda_r2,
        deuda_confiable     = EXCLUDED.deuda_confiable,
        estado_macro        = EXCLUDED.estado_macro,
        sector_etf          = EXCLUDED.sector_etf,
        sector_alineado     = EXCLUDED.sector_alineado,
        actualizado_en      = NOW(),
        run_id              = EXCLUDED.run_id
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


def registrar_log(conn, ticker: str, status: str, message: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_SQL, (SCHEMA, TABLE, ticker, status, message))
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log para {ticker}: {e}")
        conn.rollback()


# ── TÉCNICOS ───────────────────────────────────────────────────────────────────
def calcular_rsi(series: pd.Series, window: int = 14) -> float | None:
    if len(series) < window + 1:
        return None
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(window).mean()
    loss  = (-delta.clip(upper=0)).rolling(window).mean()
    rs    = gain / loss.replace(0, np.nan)
    val   = (100 - 100 / (1 + rs)).iloc[-1]
    return float(val) if pd.notna(val) else None


def calcular_obv_slope(close: pd.Series, volume: pd.Series) -> float | None:
    if len(close) < 20:
        return None
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    y   = obv.iloc[-20:].values.astype(float)
    x   = np.arange(20)
    slope = np.polyfit(x, y, 1)[0]
    mean  = np.mean(y)
    return float(slope / mean) if mean != 0 else None


def calcular_tecnicos(conn, ticker: str) -> dict:
    """
    Lee ingest.precios y calcula todos los indicadores técnicos.
    Devuelve dict con NULLs si no hay suficientes datos (< MIN_DIAS).
    """
    nulos = {
        "rsi_14_diario": None, "rsi_14_semanal": None,
        "precio_vs_ma200": None, "dist_max_52w": None,
        "vol_realizada_30d": None, "vol_realizada_90d": None,
        "volume_ratio_20d": None, "obv_slope": None,
        "momentum_3m": None, "momentum_6m": None, "momentum_12m": None,
    }

    with conn.cursor() as cur:
        cur.execute(SQL_PRECIOS, (ticker,))
        rows = cur.fetchall()

    if not rows or len(rows) < MIN_DIAS:
        return nulos

    df             = pd.DataFrame(rows, columns=["fecha", "close", "volume"])
    df["fecha"]    = pd.to_datetime(df["fecha"])
    df["close"]    = df["close"].astype(float)
    df["volume"]   = df["volume"].astype(float)
    df             = df.set_index("fecha").sort_index()

    close  = df["close"]
    volume = df["volume"]

    # RSI diario
    rsi_d = calcular_rsi(close)

    # RSI semanal
    close_w = close.resample("W-FRI").last().dropna()
    rsi_w   = calcular_rsi(close_w)

    # MA200
    precio_vs_ma200 = None
    if len(close) >= 200:
        ma200 = close.iloc[-200:].mean()
        precio_vs_ma200 = round((close.iloc[-1] / ma200 - 1) * 100, 4)

    # Distancia al máximo 52W
    dist_max_52w = None
    if len(close) >= 252:
        max52 = close.iloc[-252:].max()
        dist_max_52w = round((close.iloc[-1] / max52 - 1) * 100, 4)

    # Volatilidad realizada
    returns = close.pct_change().dropna()
    vol_30  = None
    vol_90  = None
    if len(returns) >= 30:
        vol_30 = round(returns.iloc[-30:].std() * sqrt(252) * 100, 4)
    if len(returns) >= 90:
        vol_90 = round(returns.iloc[-90:].std() * sqrt(252) * 100, 4)

    # Volume ratio 20d
    vol_ratio = None
    if len(volume) >= 20:
        vol_ratio = round(float(volume.iloc[-1]) / float(volume.iloc[-20:].mean()), 4)

    # OBV slope normalizado
    obv_slope = calcular_obv_slope(close, volume)
    if obv_slope is not None:
        obv_slope = round(obv_slope, 6)

    # Momentum
    def mom(dias):
        if len(close) > dias:
            return round((float(close.iloc[-1]) / float(close.iloc[-dias]) - 1) * 100, 4)
        return None

    return {
        "rsi_14_diario":    round(rsi_d, 2) if rsi_d is not None else None,
        "rsi_14_semanal":   round(rsi_w, 2) if rsi_w is not None else None,
        "precio_vs_ma200":  precio_vs_ma200,
        "dist_max_52w":     dist_max_52w,
        "vol_realizada_30d":vol_30,
        "vol_realizada_90d":vol_90,
        "volume_ratio_20d": vol_ratio,
        "obv_slope":        obv_slope,
        "momentum_3m":      mom(63),
        "momentum_6m":      mom(126),
        "momentum_12m":     mom(252),
    }


# ── SALUD ──────────────────────────────────────────────────────────────────────
def leer_salud(conn, ticker: str) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_SALUD, (ticker,))
        row = cur.fetchone()
    if not row:
        return {
            "altman_z_score": None, "altman_zona": None,
            "piotroski_score": None, "piotroski_categoria": None,
        }
    return dict(row)


# ── REGRESIONES ───────────────────────────────────────────────────────────────
def calcular_regresion(valores: list) -> tuple:
    """
    Regresión lineal simple sobre valores anuales.
    Retorna (pendiente, signo, r2, confiable).
    confiable = n >= 3 AND r2 >= 0.50
    """
    serie = pd.Series(valores, dtype=float).dropna()
    n     = len(serie)
    if n < 2:
        return None, None, None, False
    x = np.arange(n, dtype=float)
    slope, _, r, _, _ = stats.linregress(x, serie.values)
    r2        = round(float(r ** 2), 4)
    signo     = 1 if slope > 0 else -1
    confiable = bool(n >= 3 and r2 >= 0.50)
    return round(float(slope), 6), signo, r2, confiable


def calcular_regresiones(conn, ticker: str) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_HIST, (ticker,))
        rows = cur.fetchall()

    nulos = {
        "roic_tendencia": None, "roic_signo": None,
        "roic_r2": None,        "roic_confiable": False,
        "deuda_tendencia": None, "deuda_signo": None,
        "deuda_r2": None,        "deuda_confiable": False,
    }

    if not rows:
        return nulos

    roic_vals  = [r["roic"]             for r in rows]
    deuda_vals = [r["net_debt_to_ebitda"] for r in rows]

    roic_t,  roic_s,  roic_r2,  roic_c  = calcular_regresion(roic_vals)
    deuda_t, deuda_s, deuda_r2, deuda_c = calcular_regresion(deuda_vals)

    return {
        "roic_tendencia":  roic_t,
        "roic_signo":      roic_s,
        "roic_r2":         roic_r2,
        "roic_confiable":  roic_c,
        "deuda_tendencia": deuda_t,
        "deuda_signo":     deuda_s,
        "deuda_r2":        deuda_r2,
        "deuda_confiable": deuda_c,
    }


# ── MACRO / SECTOR ─────────────────────────────────────────────────────────────
def leer_estado_macro(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute(SQL_MACRO)
        row = cur.fetchone()
    return row[0] if row else None


def leer_alineaciones(conn) -> dict[str, str]:
    """
    Devuelve dict {etf_ticker: alineacion_macro} para todos los ETFs sectoriales.
    Se lee UNA VEZ antes del loop principal.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_ALINEACION)
        rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}


# ── Sanitización de tipos numpy → Python nativo ───────────────────────────────
def safe(val):
    if val is None:
        return None
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        if np.isnan(val) or np.isinf(val):
            return None
        return float(val)
    return val


# ── INSERT ─────────────────────────────────────────────────────────────────────
def insertar(conn, payload: dict) -> None:
    payload_safe = {k: safe(v) for k, v in payload.items()}
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, payload_safe)
    conn.commit()


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*65}")
    print(f"  ENRIQUECER UNIVERSO — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id  : {RUN_ID}")
    print(f"  destino : {SCHEMA}.{TABLE}")
    print(f"{'='*65}\n")

    conn = get_conn()

    # ── Leer contexto global (una sola vez)
    estado_macro = leer_estado_macro(conn)
    alineaciones = leer_alineaciones(conn)
    log.info(f"Estado macro: {estado_macro}")
    log.info(f"Alineaciones sectoriales leídas: {len(alineaciones)} ETFs")

    # ── Leer universo
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_UNIVERSO)
        universo = [dict(r) for r in cur.fetchall()]

    total = len(universo)
    log.info(f"Empresas a enriquecer: {total}")
    print(f"  Estado macro    : {estado_macro}")
    print(f"  Empresas        : {total}\n")

    if total == 0:
        log.error("seleccion.universo vacío. Ejecutar aplicar_filtro.py primero.")
        conn.close()
        return

    n_ok   = 0
    n_fail = 0

    # Contadores para resumen
    altman_dist     = {"safe": 0, "grey": 0, "distress": 0, "—": 0}
    piotroski_dist  = {"fuerte": 0, "neutral": 0, "debil": 0, "—": 0}
    n_aligned       = 0
    n_roic_mejora   = 0
    n_deuda_baja    = 0
    rsi_sem_vals    = []
    top5_rows       = []

    for i, empresa in enumerate(universo, start=1):
        ticker = empresa["ticker"]

        try:
            # ── Técnicos
            tecnicos = calcular_tecnicos(conn, ticker)

            # ── Salud financiera
            salud = leer_salud(conn, ticker)

            # ── Regresiones
            regresiones = calcular_regresiones(conn, ticker)

            # ── Sector y alineación macro
            sector_etf    = SECTOR_ETF.get(empresa["sector"] or "", None)
            sector_alin   = alineaciones.get(sector_etf, "NEUTRAL") if sector_etf else "NEUTRAL"

            # ── Construir payload
            payload = {
                "ticker":               ticker,
                "snapshot_date":        empresa["snapshot_date"],
                "sector":               empresa["sector"],
                "industry":             empresa["industry"],
                "market_cap_tier":      empresa["market_cap_tier"],
                "quality_score":        empresa["quality_score"],
                "value_score":          empresa["value_score"],
                "multifactor_score":    empresa["multifactor_score"],
                "multifactor_rank":     empresa["multifactor_rank"],
                "multifactor_percentile": empresa["multifactor_percentile"],
                **tecnicos,
                **salud,
                **regresiones,
                "estado_macro":         estado_macro,
                "sector_etf":           sector_etf,
                "sector_alineado":      sector_alin,
                "run_id":               RUN_ID,
            }

            insertar(conn, payload)
            registrar_log(conn, ticker, "success", "OK")
            n_ok += 1

            # ── Acumular para resumen
            zona = salud.get("altman_zona") or "—"
            altman_dist[zona] = altman_dist.get(zona, 0) + 1

            cat = salud.get("piotroski_categoria") or "—"
            piotroski_dist[cat] = piotroski_dist.get(cat, 0) + 1

            if sector_alin == "ALIGNED":
                n_aligned += 1
            if regresiones.get("roic_signo") == 1:
                n_roic_mejora += 1
            if regresiones.get("deuda_signo") == -1:
                n_deuda_baja += 1
            if tecnicos.get("rsi_14_semanal") is not None:
                rsi_sem_vals.append(tecnicos["rsi_14_semanal"])
            if empresa["multifactor_rank"] <= 5:
                top5_rows.append((empresa, tecnicos, salud))

        except Exception as e:
            n_fail += 1
            log.error(f"[{ticker}] error: {e}")
            registrar_log(conn, ticker, "fail", str(e))
            conn.rollback()

        if i % LOG_CADA == 0:
            log.info(f"Progreso: {i}/{total} — OK={n_ok} FAIL={n_fail}")

    conn.close()

    # ── Resumen final
    avg_rsi = round(sum(rsi_sem_vals) / len(rsi_sem_vals), 1) if rsi_sem_vals else None

    print(f"\n{'='*65}")
    print(f"  RESUMEN FINAL — {datetime.now().strftime('%H:%M:%S')}")
    print(f"  {'─'*61}")
    print(f"  Total enriquecidas : {n_ok}  /  FAIL : {n_fail}")
    print()
    print(f"  Altman Z-Score:")
    print(f"    safe     : {altman_dist.get('safe', 0):>4}")
    print(f"    grey     : {altman_dist.get('grey', 0):>4}")
    print(f"    distress : {altman_dist.get('distress', 0):>4}")
    print(f"    sin dato : {altman_dist.get('—', 0):>4}")
    print()
    print(f"  Piotroski F-Score:")
    print(f"    fuerte   : {piotroski_dist.get('fuerte', 0):>4}")
    print(f"    neutral  : {piotroski_dist.get('neutral', 0):>4}")
    print(f"    debil    : {piotroski_dist.get('debil', 0):>4}")
    print(f"    sin dato : {piotroski_dist.get('—', 0):>4}")
    print()
    print(f"  Sector ALIGNED     : {n_aligned}")
    print(f"  ROIC mejorando     : {n_roic_mejora}")
    print(f"  Deuda bajando      : {n_deuda_baja}")
    print(f"  RSI semanal avg    : {avg_rsi}")
    print()

    if top5_rows:
        print(f"  Top 5 por multifactor_rank:")
        print(f"  {'─'*61}")
        print(f"  {'RK':>4}  {'TICKER':<8}  {'MULTI':>6}  "
              f"{'RSI_W':>6}  {'ALTMAN':>8}  {'PIOT':>5}  SECTOR")
        print(f"  {'─'*61}")
        for emp, tec, sal in sorted(top5_rows, key=lambda x: x[0]["multifactor_rank"]):
            print(
                f"  {int(emp['multifactor_rank']):>4}  "
                f"{emp['ticker']:<8}  "
                f"{float(emp['multifactor_score'] or 0):>6.1f}  "
                f"{tec.get('rsi_14_semanal') or 0:>6.1f}  "
                f"{float(sal.get('altman_z_score') or 0):>8.2f}  "
                f"{sal.get('piotroski_score') or '—':>5}  "
                f"{(emp['sector'] or '—')[:20]}"
            )
        print(f"  {'─'*61}")

    print(f"\n  Log: {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — OK={n_ok} FAIL={n_fail} ALIGNED={n_aligned} "
             f"ROIC_UP={n_roic_mejora} DEUDA_DOWN={n_deuda_baja}")


if __name__ == "__main__":
    main()
