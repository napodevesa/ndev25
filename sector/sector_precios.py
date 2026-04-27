#!/usr/bin/env python3
"""
sector_precios.py

Descarga precios diarios de 63 ETFs sectoriales desde FMP,
calcula indicadores técnicos de fuerza relativa vs SPY
y guarda el snapshot en sector.sector_snapshot.

Ejecución: python sector/sector_precios.py
"""

import os
import time
import subprocess
import requests
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ── ENV ────────────────────────────────────────────────────────────────────────
load_dotenv("/Users/ndev/Desktop/ndev25/.env")

POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5433")
FMP_API_KEY = os.getenv("FMP_API_KEY")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, FMP_API_KEY]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID    = datetime.now().strftime("%Y%m%d_%H%M")
BENCHMARK = "SPY"
DIAS_HIST = 730
DIAS_MIN  = 252

SLEEP_ENTRE_REQUESTS = 5
MAX_REINTENTOS       = 3
SLEEP_EN_429         = 60

# ── Conexión ───────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT,
    )

# ── Leer tickers activos ───────────────────────────────────────────────────────
def leer_tickers(conn) -> list[dict]:
    sql = """
        SELECT ticker, nombre, tipo, sector_gics, sector_etf, industria
        FROM sector.sector_etfs
        WHERE activo = TRUE
        ORDER BY tipo, sector_gics, ticker
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]

# ── Última fecha en DB ─────────────────────────────────────────────────────────
def ultima_fecha_en_db(conn, ticker: str):
    sql = "SELECT MAX(fecha) FROM sector.sector_raw WHERE ticker = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (ticker,))
        return cur.fetchone()[0]

# ── Descarga FMP ───────────────────────────────────────────────────────────────
def fetch_precios_fmp(ticker, desde, hasta, api_key):
    url = (
        f"https://financialmodelingprep.com/stable/"
        f"historical-price-eod/dividend-adjusted"
        f"?symbol={ticker}&from={desde}&to={hasta}"
        f"&apikey={api_key}"
    )
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 429:
            return None, "rate_limit"
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        data = r.json()
        if not data or isinstance(data, dict):
            return [], "sin_datos"

        filas = []
        for d in data:
            fecha = d.get("date")
            close = d.get("adjClose") or d.get("close")
            volume = d.get("volume", 0)
            if fecha and close:
                filas.append({
                    "fecha":  fecha,
                    "close":  float(close),
                    "volume": int(volume) if volume else 0
                })
        return filas, "ok"
    except Exception as e:
        return None, str(e)


def descargar_precios(ticker: str, desde: str, hasta: str) -> pd.DataFrame | None:
    for intento in range(1, MAX_REINTENTOS + 1):
        filas, estado = fetch_precios_fmp(ticker, desde, hasta, FMP_API_KEY)
        if estado == "rate_limit":
            print(f"\n  ⏳ Rate limit en {ticker} (intento {intento}/{MAX_REINTENTOS}) "
                  f"— esperando {SLEEP_EN_429}s...")
            time.sleep(SLEEP_EN_429)
            continue
        if estado == "sin_datos" or filas == []:
            print(f"  ⚠ Sin datos: {ticker}")
            return None
        if filas is None:
            if intento < MAX_REINTENTOS:
                print(f"\n  ⚠ Error {ticker} (intento {intento}): {estado} — reintentando...")
                time.sleep(SLEEP_EN_429)
            else:
                print(f"  ✗ Error descargando {ticker}: {estado}")
                return None
            continue
        df = pd.DataFrame(filas)
        df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
        df = df.sort_values("fecha")
        time.sleep(SLEEP_ENTRE_REQUESTS)
        return df[["fecha", "close", "volume"]].copy()
    return None

# ── INSERT sector_raw ──────────────────────────────────────────────────────────
def insertar_precios(conn, ticker: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    sql = """
        INSERT INTO sector.sector_raw
            (ticker, fecha, open, high, low, close, volume, vwap, run_id)
        VALUES
            (%(ticker)s, %(fecha)s, %(open)s, %(high)s, %(low)s,
             %(close)s, %(volume)s, %(vwap)s, %(run_id)s)
        ON CONFLICT (ticker, fecha) DO NOTHING
    """
    filas = []
    for _, row in df.iterrows():
        filas.append({
            "ticker":  ticker,
            "fecha":   row["fecha"],
            "open":    float(row["open"])   if "open"   in row else None,
            "high":    float(row["high"])   if "high"   in row else None,
            "low":     float(row["low"])    if "low"    in row else None,
            "close":   float(row["close"]),
            "volume":  int(row["volume"])   if "volume" in row else None,
            "vwap":    float(row["vwap"])   if "vwap"   in row else None,
            "run_id":  RUN_ID,
        })
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, filas)
    conn.commit()
    return len(filas)

# ── Cargar histórico ───────────────────────────────────────────────────────────
def cargar_serie(conn, ticker: str) -> pd.DataFrame:
    sql = """
        SELECT fecha, close, volume
        FROM sector.sector_raw
        WHERE ticker = %s
        ORDER BY fecha ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticker,))
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["fecha","close","volume"])
    df["fecha"]  = pd.to_datetime(df["fecha"])
    df["close"]  = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    return df.set_index("fecha")

# ── Indicadores ────────────────────────────────────────────────────────────────
def calcular_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(window).mean()
    loss  = -delta.clip(upper=0).rolling(window).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calcular_slope(series: pd.Series, window: int = 5) -> float:
    s = series.dropna()
    if len(s) < window:
        return np.nan
    y = s.values[-window:]
    x = np.arange(window)
    return float(np.polyfit(x, y, 1)[0])


def calcular_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    return (np.sign(close.diff()) * volume).fillna(0).cumsum()


def calcular_retorno(series: pd.Series, dias: int) -> float:
    s = series.dropna()
    if len(s) < dias + 1:
        return np.nan
    return float((s.iloc[-1] / s.iloc[-dias - 1]) - 1) * 100


def calcular_percentil_52w(series: pd.Series) -> float:
    s = series.dropna()
    if len(s) < 252:
        return np.nan
    ventana = s.iloc[-252:]
    return float(pd.Series(ventana).rank(pct=True).iloc[-1] * 100)


def calcular_dist_maximo_52w(series: pd.Series) -> float:
    s = series.dropna()
    if len(s) < 252:
        return np.nan
    return float((s.iloc[-1] / s.iloc[-252:].max() - 1) * 100)


def calcular_indicadores(df_ticker: pd.DataFrame, df_spy: pd.DataFrame) -> dict | None:
    if df_ticker.empty or df_spy.empty:
        return None
    df = df_ticker.join(df_spy[["close"]].rename(columns={"close":"close_spy"}), how="inner")
    if len(df) < DIAS_MIN:
        return None

    close     = df["close"]
    volume    = df["volume"]
    close_spy = df["close_spy"]
    rs        = close / close_spy
    rs_weekly = rs.resample("W-FRI").last()

    def safe(v):
        return float(v) if not (v is None or (isinstance(v, float) and np.isnan(v))) else None

    return {
        "rs_vs_spy":      safe(float(rs.iloc[-1])),
        "rsi_rs_diario":  safe(calcular_rsi(rs).iloc[-1]),
        "rsi_rs_semanal": safe(calcular_rsi(rs_weekly).iloc[-1]),
        "slope_rs":       safe(calcular_slope(rs_weekly)),
        "rs_percentil":   safe(calcular_percentil_52w(rs)),
        "ret_1m":         safe(calcular_retorno(close, 21)),
        "ret_3m":         safe(calcular_retorno(close, 63)),
        "ret_6m":         safe(calcular_retorno(close, 126)),
        "ret_1a":         safe(calcular_retorno(close, 252)),
        "dist_max_52w":   safe(calcular_dist_maximo_52w(close)),
        "vol_ratio":      safe(float(volume.iloc[-1] / volume.rolling(20).mean().iloc[-1])
                               if len(volume) >= 20 else np.nan),
        "obv_slope":      safe(calcular_slope(calcular_obv(close, volume))),
        "rsi_precio":     safe(calcular_rsi(close).iloc[-1]),
        "close":          float(close.iloc[-1]),
        "fecha_ultimo":   df.index[-1].date(),
    }

# ── Guardar snapshot ───────────────────────────────────────────────────────────
def guardar_snapshot(conn, resultados: list[dict]):
    sql_macro = "SELECT estado_macro FROM macro.macro_diagnostico ORDER BY calculado_en DESC LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql_macro)
        row = cur.fetchone()
        estado_macro = row[0] if row else None

    ALINEACION = {
        "EXPANSION":   ["XLK","XLY","XLF","XLI","SOXX","IGV","SKYY","XRT","IAI","ITA"],
        "SLOWDOWN":    ["XLV","XLP","XLU","GLD","TLT","IBB","PBJ","PHO","UTG","PUI"],
        "CONTRACTION": ["XLP","XLU","XLV","GLD","TLT","PBJ","PHO","UTG","IBB"],
        "RECOVERY":    ["XLI","XLB","XLF","XLK","PAVE","COPX","KBE","KRE","ITA"],
    }
    favorecidos = ALINEACION.get(estado_macro, [])

    sql_insert = """
        INSERT INTO sector.sector_snapshot (
            run_id, fecha_ultimo, ticker, tipo, sector_gics, sector_etf, industria,
            close, rs_vs_spy, rsi_rs_diario, rsi_rs_semanal, slope_rs, rs_percentil,
            ret_1m, ret_3m, ret_6m, ret_1a, dist_max_52w,
            vol_ratio, obv_slope, rsi_precio,
            estado_macro, alineacion_macro
        ) VALUES (
            %(run_id)s, %(fecha_ultimo)s, %(ticker)s, %(tipo)s,
            %(sector_gics)s, %(sector_etf)s, %(industria)s,
            %(close)s, %(rs_vs_spy)s, %(rsi_rs_diario)s, %(rsi_rs_semanal)s,
            %(slope_rs)s, %(rs_percentil)s,
            %(ret_1m)s, %(ret_3m)s, %(ret_6m)s, %(ret_1a)s, %(dist_max_52w)s,
            %(vol_ratio)s, %(obv_slope)s, %(rsi_precio)s,
            %(estado_macro)s, %(alineacion_macro)s
        )
    """
    filas = []
    for r in resultados:
        filas.append({
            **r,
            "estado_macro":     estado_macro,
            "alineacion_macro": "ALIGNED" if r["ticker"] in favorecidos else "NEUTRAL",
            "fecha_ultimo":     r.get("fecha_ultimo"),
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql_insert, filas)
    conn.commit()
    print(f"  ✓ {len(filas)} filas guardadas en sector_snapshot (run_id: {RUN_ID})")
    print(f"  ✓ Estado macro vigente: {estado_macro}")
    aligned = sum(1 for f in filas if f["alineacion_macro"] == "ALIGNED")
    print(f"  ✓ ETFs alineados: {aligned}/{len(filas)}")

# ── Log infraestructura ────────────────────────────────────────────────────────
def registrar_log(conn, status: str, message: str):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO infraestructura.update_logs "
                "(schema_name, table_name, ticker, status, message) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("sector", "sector_snapshot", "BULK", status, message),
            )
        conn.commit()
    except Exception as e:
        print(f"  ⚠ No se pudo registrar log: {e}")
        conn.rollback()

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    caffeinate = subprocess.Popen(["caffeinate"])

    print(f"\n{'='*65}")
    print(f"  SECTOR PRECIOS — {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  run: {RUN_ID}")
    print(f"{'='*65}\n")

    conn    = get_conn()
    tickers = leer_tickers(conn)
    hoy     = datetime.today().date()

    print(f"  Tickers en catálogo: {len(tickers)}\n")

    # Paso 1: descargar precios
    print("  [1/3] Descargando precios desde FMP...")
    for t in tickers:
        ticker = t["ticker"]
        ultima = ultima_fecha_en_db(conn, ticker)

        if ultima is None:
            desde = (hoy - timedelta(days=DIAS_HIST)).strftime("%Y-%m-%d")
            modo  = "histórico completo"
        else:
            desde = (ultima + timedelta(days=1)).strftime("%Y-%m-%d")
            modo  = f"incremental desde {desde}"

        hasta = hoy.strftime("%Y-%m-%d")

        if desde > hasta:
            print(f"  ✓ {ticker:6} — ya actualizado")
            continue

        print(f"  → {ticker:6} ({modo})", end=" ")
        df = descargar_precios(ticker, desde, hasta)
        n  = insertar_precios(conn, ticker, df)
        print(f"→ {n} filas insertadas")

    # Paso 2: cargar SPY
    print("\n  [2/3] Cargando benchmark SPY...")
    df_spy = cargar_serie(conn, BENCHMARK)
    if df_spy.empty:
        print("  ✗ SPY sin datos — abortando cálculo de indicadores")
        registrar_log(conn, "fail", "SPY sin datos")
        conn.close()
        caffeinate.terminate()
        return
    print(f"  ✓ SPY: {len(df_spy)} días de historia")

    # Paso 3: calcular indicadores
    print("\n  [3/3] Calculando indicadores...")
    resultados = []

    for t in tickers:
        ticker = t["ticker"]
        if ticker == BENCHMARK:
            continue
        df_ticker   = cargar_serie(conn, ticker)
        indicadores = calcular_indicadores(df_ticker, df_spy)
        if indicadores is None:
            print(f"  ⚠ {ticker:6} — datos insuficientes")
            continue
        fila = {**t, **indicadores, "run_id": RUN_ID}
        resultados.append(fila)
        print(f"  ✓ {ticker:6} | RS={indicadores['rs_vs_spy']:.3f} "
              f"| RSI_sem={indicadores['rsi_rs_semanal']:.1f} "
              f"| ret_3m={indicadores['ret_3m']:.1f}%")

    print(f"\n{'─'*65}")
    print(f"  Tickers procesados: {len(resultados)}")

    if resultados:
        df_res = pd.DataFrame(resultados)
        top5 = df_res[df_res["tipo"] == "industria"].nlargest(5, "rsi_rs_semanal")[
            ["ticker","industria","rs_vs_spy","rsi_rs_semanal","ret_3m"]
        ]
        print(f"\n  Top 5 por RSI semanal (industrias):")
        print(top5.to_string(index=False))

        print("\n  Guardando snapshot en DB...")
        try:
            guardar_snapshot(conn, resultados)
            registrar_log(conn, "success",
                          f"{len(resultados)} ETFs — run_id={RUN_ID}")
        except Exception as e:
            print(f"  ✗ Error guardando snapshot: {e}")
            registrar_log(conn, "fail", str(e))
            conn.close()
            caffeinate.terminate()
            raise

    conn.close()
    caffeinate.terminate()

    print(f"\n{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
