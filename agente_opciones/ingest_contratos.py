#!/usr/bin/env python3
"""
ingest_contratos.py

Lee las empresas con instrumento = 'cash_secured_put' en agente.decision
y descarga los contratos de opciones PUT y CALL desde Polygon.

Filtros por contrato:
  DTE: 20–90 días
  Put  delta: −0.35 a −0.25
  Call delta:  0.20 a  0.35

Inserta en agente_opciones.contratos_raw con
ON CONFLICT (opcion, fecha) DO UPDATE.

Ejecución: python agente_opciones/ingest_contratos.py
"""

import os
import time
import logging
import subprocess
import requests
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
POLYGON_API_KEY   = os.getenv("POLYGON_API_KEY")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POLYGON_API_KEY]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID        = datetime.now().strftime("%Y%m%d_%H%M")
SNAPSHOT_DATE = date(date.today().year, date.today().month, 1)

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"ingest_contratos_{date.today().isoformat()}.log")

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
SCHEMA         = "agente_opciones"
TABLE          = "contratos_raw"
BASE_URL       = "https://api.polygon.io/v3/snapshot/options"
MIN_DTE          = 20
MAX_DTE          = 90
CONTRACT_TYPES   = ["put", "call"]
DELTA_RANGO      = {
    "put":  (-0.35, -0.25),
    "call": ( 0.20,  0.35),
}
SLEEP_ENTRE    = 0.25
SLEEP_429      = 60
MAX_REINTENTOS = 3
LOG_CADA       = 50

# ── SQL ────────────────────────────────────────────────────────────────────────
SQL_TICKERS = """
    SELECT DISTINCT ticker
    FROM agente.decision
    WHERE trade_status = 'active'
      AND instrumento = 'cash_secured_put'
      AND snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
    ORDER BY ticker
"""

INSERT_SQL = """
    INSERT INTO agente_opciones.contratos_raw (
        ticker, opcion, snapshot_date, fecha,
        contract_type, strike, vto, dte,
        delta, gamma, theta, vega,
        iv, oi, volume, vwap, close_price,
        run_id
    ) VALUES (
        %(ticker)s, %(opcion)s, %(snapshot_date)s, %(fecha)s,
        %(contract_type)s, %(strike)s, %(vto)s, %(dte)s,
        %(delta)s, %(gamma)s, %(theta)s, %(vega)s,
        %(iv)s, %(oi)s, %(volume)s, %(vwap)s, %(close_price)s,
        %(run_id)s
    )
    ON CONFLICT (opcion, fecha) DO UPDATE SET
        delta       = EXCLUDED.delta,
        gamma       = EXCLUDED.gamma,
        theta       = EXCLUDED.theta,
        vega        = EXCLUDED.vega,
        iv          = EXCLUDED.iv,
        oi          = EXCLUDED.oi,
        volume      = EXCLUDED.volume,
        vwap        = EXCLUDED.vwap,
        close_price = EXCLUDED.close_price,
        run_id      = EXCLUDED.run_id
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


def obtener_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(SQL_TICKERS)
        return [row[0] for row in cur.fetchall()]


def registrar_log(conn, ticker: str, status: str, message: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_SQL, (SCHEMA, TABLE, ticker, status, message))
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log para {ticker}: {e}")
        conn.rollback()


# ── API ────────────────────────────────────────────────────────────────────────
def to_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def to_int(val, default=0):
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _paginar_tipo(ticker: str, contract_type: str) -> list[dict] | None:
    """
    Descarga contratos de un tipo (put o call) para el ticker.
    Paginado con next_url. Corta cuando dte > MAX_DTE.
    Reintenta hasta MAX_REINTENTOS si recibe 429.
    Devuelve lista de filas o None si hay error HTTP/red.
    """
    hoy      = datetime.now()
    hoy_date = date.today()
    min_d, max_d = DELTA_RANGO[contract_type]

    url      = f"{BASE_URL}/{ticker}?limit=250&apiKey={POLYGON_API_KEY}"
    resultados = []
    buscando = True
    intento  = 1

    while url and buscando:
        try:
            resp = requests.get(url, timeout=15)

            if resp.status_code == 429:
                if intento <= MAX_REINTENTOS:
                    log.warning(
                        f"[{ticker}/{contract_type}] 429 rate limit "
                        f"(intento {intento}/{MAX_REINTENTOS}) — esperando {SLEEP_429}s..."
                    )
                    time.sleep(SLEEP_429)
                    intento += 1
                    continue
                else:
                    log.error(f"[{ticker}/{contract_type}] 429 persistente — abandonando")
                    return None

            if resp.status_code != 200:
                log.error(f"[{ticker}/{contract_type}] HTTP {resp.status_code}")
                return None

            intento   = 1  # reset tras éxito
            data      = resp.json()
            contratos = data.get("results", [])

            for c in contratos:
                details = c.get("details", {})
                greeks  = c.get("greeks", {})
                day     = c.get("day", {})

                if details.get("contract_type") != contract_type:
                    continue

                expiry = details.get("expiration_date")
                if not expiry:
                    continue

                fecha_vto = datetime.strptime(expiry, "%Y-%m-%d")
                dte       = (fecha_vto - hoy).days

                # Polygon ordena por vencimiento — cortar al superar MAX_DTE
                if dte > MAX_DTE:
                    buscando = False
                    break

                if dte < MIN_DTE:
                    continue

                delta = greeks.get("delta")
                if delta is None:
                    continue
                try:
                    delta = float(delta)
                except (TypeError, ValueError):
                    continue

                if not (min_d <= delta <= max_d):
                    continue

                resultados.append({
                    "ticker":        ticker,
                    "opcion":        details.get("ticker"),
                    "snapshot_date": SNAPSHOT_DATE,
                    "fecha":         hoy_date,
                    "contract_type": contract_type,
                    "strike":        to_float(details.get("strike_price")),
                    "vto":           expiry,
                    "dte":           dte,
                    "delta":         round(delta, 4),
                    "gamma":         to_float(greeks.get("gamma")),
                    "theta":         to_float(greeks.get("theta")),
                    "vega":          to_float(greeks.get("vega")),
                    "iv":            to_float(c.get("implied_volatility")),
                    "oi":            to_int(c.get("open_interest")),
                    "volume":        to_int(day.get("volume")),
                    "vwap":          to_float(day.get("vwap")),
                    "close_price":   to_float(day.get("close")),
                    "run_id":        RUN_ID,
                })

            next_url = data.get("next_url")
            url = f"{next_url}&apiKey={POLYGON_API_KEY}" if (next_url and buscando) else None

        except requests.exceptions.RequestException as e:
            log.error(f"[{ticker}/{contract_type}] error de red (intento {intento}): {e}")
            if intento < MAX_REINTENTOS:
                time.sleep(SLEEP_429)
                intento += 1
            else:
                return None

    return resultados


def fetch_contratos(ticker: str) -> tuple[list[dict], list[dict]]:
    """
    Descarga puts y calls para el ticker.
    Devuelve (filas_put, filas_call) — cada una puede ser lista vacía si no hay datos.
    Si _paginar_tipo devuelve None (error), se registra como fallo y se devuelve [].
    """
    filas_put  = _paginar_tipo(ticker, "put")  or []
    filas_call = _paginar_tipo(ticker, "call") or []
    return filas_put, filas_call


# ── INSERT ─────────────────────────────────────────────────────────────────────
def insertar(conn, filas: list[dict]) -> int:
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_SQL, filas)
    conn.commit()
    return len(filas)


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    caffeinate = subprocess.Popen(["caffeinate"])

    print(f"\n{'='*65}")
    print(f"  INGEST CONTRATOS OPCIONES — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id        : {RUN_ID}")
    print(f"  snapshot_date : {SNAPSHOT_DATE}")
    print(f"  destino       : {SCHEMA}.{TABLE}")
    print(f"  PUT  delta    : {DELTA_RANGO['put'][0]} a {DELTA_RANGO['put'][1]}")
    print(f"  CALL delta    :  {DELTA_RANGO['call'][0]} a  {DELTA_RANGO['call'][1]}")
    print(f"  DTE           : {MIN_DTE}–{MAX_DTE} días")
    print(f"{'='*65}\n")

    conn    = get_conn()
    tickers = obtener_tickers(conn)
    total   = len(tickers)

    if total == 0:
        log.error("Sin tickers con instrumento=cash_secured_put en agente.decision. "
                  "Ejecutar agente_decision.py primero.")
        caffeinate.terminate()
        conn.close()
        return

    log.info(f"Tickers a procesar: {total}")
    print(f"  Tickers activos (CSP): {total}\n")

    n_ok        = 0
    n_fail      = 0
    n_sin_datos = 0
    n_puts      = 0
    n_calls     = 0

    for i, ticker in enumerate(tickers, start=1):
        try:
            filas_put, filas_call = fetch_contratos(ticker)
            filas_total = filas_put + filas_call

            if not filas_total:
                n_sin_datos += 1
                registrar_log(conn, ticker, "skip", "Sin contratos en rango DTE/delta")
            else:
                n_insertadas = insertar(conn, filas_total)
                n_ok        += 1
                n_puts      += len(filas_put)
                n_calls     += len(filas_call)
                registrar_log(
                    conn, ticker, "success",
                    f"put={len(filas_put)} call={len(filas_call)} "
                    f"total={n_insertadas} ({MIN_DTE}–{MAX_DTE}d)"
                )

        except Exception as e:
            n_fail += 1
            log.error(f"[{ticker}] excepción inesperada: {e}")
            registrar_log(conn, ticker, "fail", str(e))
            conn.rollback()

        if i % LOG_CADA == 0:
            log.info(
                f"Progreso: {i}/{total} — "
                f"OK={n_ok} SIN_DATOS={n_sin_datos} FAIL={n_fail} "
                f"PUTS={n_puts} CALLS={n_calls}"
            )

        time.sleep(SLEEP_ENTRE)

    # ── Log global
    n_total = n_puts + n_calls
    try:
        registrar_log(
            conn, "BULK", "success",
            f"total={total} ok={n_ok} sin_datos={n_sin_datos} fail={n_fail} "
            f"puts={n_puts} calls={n_calls} snapshot={SNAPSHOT_DATE}"
        )
    except Exception:
        pass

    conn.close()
    caffeinate.terminate()

    promedio = n_total / n_ok if n_ok > 0 else 0

    print(f"\n{'='*65}")
    print(f"  RESUMEN FINAL")
    print(f"  Tickers procesados : {total}")
    print(f"  OK (con contratos) : {n_ok}")
    print(f"  Sin datos Polygon  : {n_sin_datos}")
    print(f"  FAIL               : {n_fail}")
    print(f"  Puts insertados    : {n_puts}")
    print(f"  Calls insertados   : {n_calls}")
    print(f"  Total contratos    : {n_total}")
    print(f"  Prom. por ticker   : {promedio:.1f}")
    print(f"  Log                : {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(
        f"Fin — total={total} ok={n_ok} sin_datos={n_sin_datos} fail={n_fail} "
        f"puts={n_puts} calls={n_calls} prom={promedio:.1f}"
    )


if __name__ == "__main__":
    main()
