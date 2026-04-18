#!/usr/bin/env python3
"""
ingest_precios.py

Descarga precios EOD ajustados por dividendos desde FMP para todos los
tickers de universos.stock_opciones_2026 e inserta en ingest.precios.

Lógica incremental:
  - Si no hay datos para el ticker  → bajar 730 días de historia
  - Si ya hay datos                 → bajar solo desde último día + 1
  - Si ya está al día               → skip sin llamar a la API

Endpoint:
  https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted
  ?symbol={ticker}&from={desde}&to={hasta}&apikey={API_KEY}

Ejecución: python micro/ingest/ingest_precios.py
"""

import os
import time
import logging
import subprocess
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

# ── ENV ────────────────────────────────────────────────────────────────────────
load_dotenv("/Users/ndev/Desktop/ndev25/.env")

POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5433))
FMP_API_KEY       = os.getenv("FMP_API_KEY")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, FMP_API_KEY]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M")

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output_ingest")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"ingest_precios_{date.today().isoformat()}.log")

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
SCHEMA         = "ingest"
TABLE          = "precios"
BASE_URL       = "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted"
DIAS_HIST      = 730     # historia inicial si no hay datos previos
SLEEP_ENTRE    = 0.25    # segundos entre requests normales
SLEEP_429      = 60      # segundos de espera si recibimos 429
MAX_REINTENTOS = 3       # intentos por ticker antes de marcar FAIL
LOG_CADA       = 200     # loguear progreso cada N tickers

# ── SQL ────────────────────────────────────────────────────────────────────────
SQL_TICKERS = """
    SELECT ticker
    FROM universos.stock_opciones_2026
    ORDER BY ticker
"""

SQL_ULTIMA_FECHA = """
    SELECT MAX(fecha)
    FROM ingest.precios
    WHERE ticker = %s
"""

INSERT_SQL = """
    INSERT INTO ingest.precios (ticker, fecha, close_adj, volume, run_id)
    VALUES (%(ticker)s, %(fecha)s, %(close_adj)s, %(volume)s, %(run_id)s)
    ON CONFLICT (ticker, fecha) DO NOTHING
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


def ultima_fecha_en_db(conn, ticker: str):
    """
    Devuelve la última fecha en ingest.precios para el ticker,
    o None si no hay datos previos.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_ULTIMA_FECHA, (ticker,))
        resultado = cur.fetchone()[0]
        return resultado


# ── API ────────────────────────────────────────────────────────────────────────
def fetch_precios(ticker: str, desde: str, hasta: str) -> list[dict] | None:
    """
    Llama al endpoint FMP dividend-adjusted con rango de fechas.
    Reintenta hasta MAX_REINTENTOS veces si recibe 429.
    Devuelve lista de filas mapeadas a columnas DB, o None si falla.
    """
    url = (
        f"{BASE_URL}"
        f"?symbol={ticker}"
        f"&from={desde}"
        f"&to={hasta}"
        f"&apikey={FMP_API_KEY}"
    )

    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            resp = requests.get(url, timeout=15)

            if resp.status_code == 429:
                log.warning(
                    f"[{ticker}] 429 rate limit (intento {intento}/{MAX_REINTENTOS}) "
                    f"— esperando {SLEEP_429}s..."
                )
                time.sleep(SLEEP_429)
                continue

            if resp.status_code != 200:
                log.error(f"[{ticker}] HTTP {resp.status_code}")
                return None

            data = resp.json()

            if not isinstance(data, list) or not data:
                log.warning(f"[{ticker}] respuesta vacía o formato inesperado")
                return None

            filas = []
            for item in data:
                fecha_raw = item.get("date")
                close_adj = item.get("adjClose")
                volume    = item.get("volume")

                if not fecha_raw or close_adj is None:
                    continue

                try:
                    close_adj = float(close_adj)
                except (TypeError, ValueError):
                    close_adj = None

                try:
                    volume = int(volume) if volume is not None else None
                except (TypeError, ValueError):
                    volume = None

                filas.append({
                    "ticker":    ticker,
                    "fecha":     fecha_raw,   # string "YYYY-MM-DD", psycopg2 lo castea a DATE
                    "close_adj": close_adj,
                    "volume":    volume,
                    "run_id":    RUN_ID,
                })

            return filas if filas else None

        except requests.exceptions.RequestException as e:
            log.error(f"[{ticker}] error de red (intento {intento}): {e}")
            if intento < MAX_REINTENTOS:
                time.sleep(SLEEP_429)

    return None


# ── INSERT + LOG ───────────────────────────────────────────────────────────────
def insertar(conn, filas: list[dict]) -> int:
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_SQL, filas)
    conn.commit()
    return len(filas)


def registrar_log(conn, ticker: str, status: str, message: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_SQL, (SCHEMA, TABLE, ticker, status, message))
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log para {ticker}: {e}")
        conn.rollback()


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    caffeinate = subprocess.Popen(["caffeinate"])

    hoy   = date.today()
    hasta = hoy.strftime("%Y-%m-%d")

    print(f"\n{'='*65}")
    print(f"  INGEST PRECIOS EOD — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id  : {RUN_ID}")
    print(f"  destino : {SCHEMA}.{TABLE}")
    print(f"  hasta   : {hasta}  (dinámica)")
    print(f"{'='*65}\n")

    conn    = get_conn()
    tickers = obtener_tickers(conn)
    total   = len(tickers)
    log.info(f"Tickers a procesar: {total}")

    n_ok   = 0
    n_skip = 0
    n_fail = 0

    for i, ticker in enumerate(tickers, start=1):
        try:
            ultima = ultima_fecha_en_db(conn, ticker)

            if ultima is None:
                # Primera carga — bajar histórico completo
                desde = (hoy - timedelta(days=DIAS_HIST)).strftime("%Y-%m-%d")
                modo  = f"histórico ({DIAS_HIST}d)"
            else:
                desde = (ultima + timedelta(days=1)).strftime("%Y-%m-%d")
                modo  = f"incremental desde {desde}"

            # Ya al día — no llamar a la API
            if desde > hasta:
                n_skip += 1
                if i % LOG_CADA == 0:
                    log.info(f"Progreso: {i}/{total} — OK={n_ok} SKIP={n_skip} FAIL={n_fail}")
                time.sleep(SLEEP_ENTRE)
                continue

            filas = fetch_precios(ticker, desde, hasta)

            if filas is None:
                n_fail += 1
                registrar_log(conn, ticker, "fail", f"Sin datos ({modo})")
            else:
                n_insertadas = insertar(conn, filas)
                n_ok += 1
                registrar_log(conn, ticker, "success", f"{n_insertadas} filas — {modo}")

        except Exception as e:
            n_fail += 1
            log.error(f"[{ticker}] excepción inesperada: {e}")
            registrar_log(conn, ticker, "fail", str(e))
            conn.rollback()

        if i % LOG_CADA == 0:
            log.info(f"Progreso: {i}/{total} — OK={n_ok} SKIP={n_skip} FAIL={n_fail}")

        time.sleep(SLEEP_ENTRE)

    conn.close()
    caffeinate.terminate()

    print(f"\n{'='*65}")
    print(f"  RESUMEN FINAL")
    print(f"  TOTAL : {total}")
    print(f"  OK    : {n_ok}")
    print(f"  SKIP  : {n_skip}  (ya al día)")
    print(f"  FAIL  : {n_fail}")
    print(f"  Log   : {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — TOTAL={total} OK={n_ok} SKIP={n_skip} FAIL={n_fail}")


if __name__ == "__main__":
    main()
