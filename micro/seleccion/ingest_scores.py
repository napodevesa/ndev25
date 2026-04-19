#!/usr/bin/env python3
"""
ingest_scores.py

Lee los tickers de seleccion.universo y descarga Altman Z-score y
Piotroski F-score desde FMP para cada empresa.

Endpoint:
  https://financialmodelingprep.com/stable/financial-scores?symbol={ticker}&apikey={API_KEY}

Crea ingest.scores_salud si no existe, luego inserta con
ON CONFLICT DO UPDATE.

Ejecución: python micro/seleccion/ingest_scores.py
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
FMP_API_KEY       = os.getenv("FMP_API_KEY")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, FMP_API_KEY]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M")

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"ingest_scores_{date.today().isoformat()}.log")

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
TABLE          = "scores_salud"
BASE_URL       = "https://financialmodelingprep.com/stable/financial-scores"
SLEEP_ENTRE    = 0.25
SLEEP_429      = 60
MAX_REINTENTOS = 3
LOG_CADA       = 100

# ── SQL ────────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ingest.scores_salud (
    ticker              VARCHAR(10)     NOT NULL,
    fecha_consulta      DATE            NOT NULL,
    altman_z_score      NUMERIC(16,4),
    altman_zona         VARCHAR(20),
    piotroski_score     SMALLINT,
    piotroski_categoria VARCHAR(20),
    working_capital     NUMERIC(20,2),
    total_assets        NUMERIC(20,2),
    retained_earnings   NUMERIC(20,2),
    ebit                NUMERIC(20,2),
    market_cap          NUMERIC(20,2),
    total_liabilities   NUMERIC(20,2),
    revenue             NUMERIC(20,2),
    run_id              VARCHAR(40),
    creado_en           TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (ticker, fecha_consulta)
)
"""

SQL_TICKERS = """
    SELECT ticker
    FROM seleccion.universo
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.universo)
    ORDER BY ticker
"""

INSERT_SQL = """
    INSERT INTO ingest.scores_salud (
        ticker, fecha_consulta,
        altman_z_score, altman_zona,
        piotroski_score, piotroski_categoria,
        working_capital, total_assets, retained_earnings,
        ebit, market_cap, total_liabilities, revenue,
        run_id
    )
    VALUES (
        %(ticker)s, %(fecha_consulta)s,
        %(altman_z_score)s, %(altman_zona)s,
        %(piotroski_score)s, %(piotroski_categoria)s,
        %(working_capital)s, %(total_assets)s, %(retained_earnings)s,
        %(ebit)s, %(market_cap)s, %(total_liabilities)s, %(revenue)s,
        %(run_id)s
    )
    ON CONFLICT (ticker, fecha_consulta) DO UPDATE SET
        altman_z_score      = EXCLUDED.altman_z_score,
        altman_zona         = EXCLUDED.altman_zona,
        piotroski_score     = EXCLUDED.piotroski_score,
        piotroski_categoria = EXCLUDED.piotroski_categoria,
        working_capital     = EXCLUDED.working_capital,
        total_assets        = EXCLUDED.total_assets,
        retained_earnings   = EXCLUDED.retained_earnings,
        ebit                = EXCLUDED.ebit,
        market_cap          = EXCLUDED.market_cap,
        total_liabilities   = EXCLUDED.total_liabilities,
        revenue             = EXCLUDED.revenue,
        run_id              = EXCLUDED.run_id
"""

LOG_SQL = """
    INSERT INTO infraestructura.update_logs
        (schema_name, table_name, ticker, status, message)
    VALUES (%s, %s, %s, %s, %s)
"""


# ── Clasificaciones ────────────────────────────────────────────────────────────
def altman_zona(z: float | None) -> str | None:
    if z is None:
        return None
    if z > 2.99:
        return "safe"
    if z >= 1.81:
        return "grey"
    return "distress"


def piotroski_categoria(f: int | None) -> str | None:
    if f is None:
        return None
    if f >= 7:
        return "fuerte"
    if f >= 4:
        return "neutral"
    return "debil"


# ── DB ─────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def crear_tabla(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def obtener_tickers(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(SQL_TICKERS)
        return [row[0] for row in cur.fetchall()]


# ── API ────────────────────────────────────────────────────────────────────────
def fetch_scores(ticker: str) -> dict | None:
    """
    Llama al endpoint FMP financial-scores.
    Reintenta hasta MAX_REINTENTOS veces si recibe 429.
    Devuelve el dict listo para insertar, o None si falla.
    """
    url = f"{BASE_URL}?symbol={ticker}&apikey={FMP_API_KEY}"

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

            if isinstance(data, list):
                if not data:
                    log.warning(f"[{ticker}] respuesta vacía")
                    return None
                raw = data[0]
            elif isinstance(data, dict):
                raw = data
            else:
                log.warning(f"[{ticker}] formato inesperado: {type(data)}")
                return None

            def to_float(val):
                if val is None:
                    return None
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return None

            def to_int(val):
                if val is None:
                    return None
                try:
                    return int(val)
                except (TypeError, ValueError):
                    return None

            z = to_float(raw.get("altmanZScore"))
            f = to_int(raw.get("piotroskiScore"))

            return {
                "ticker":               ticker,
                "fecha_consulta":       date.today(),
                "altman_z_score":       z,
                "altman_zona":          altman_zona(z),
                "piotroski_score":      f,
                "piotroski_categoria":  piotroski_categoria(f),
                "working_capital":      to_float(raw.get("workingCapital")),
                "total_assets":         to_float(raw.get("totalAssets")),
                "retained_earnings":    to_float(raw.get("retainedEarnings")),
                "ebit":                 to_float(raw.get("ebit")),
                "market_cap":           to_float(raw.get("marketCap")),
                "total_liabilities":    to_float(raw.get("totalLiabilities")),
                "revenue":              to_float(raw.get("revenue")),
                "run_id":               RUN_ID,
            }

        except requests.exceptions.RequestException as e:
            log.error(f"[{ticker}] error de red (intento {intento}): {e}")
            if intento < MAX_REINTENTOS:
                time.sleep(SLEEP_429)

    return None


# ── INSERT + LOG ───────────────────────────────────────────────────────────────
def insertar(conn, row: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, row)
    conn.commit()


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

    print(f"\n{'='*65}")
    print(f"  INGEST SCORES SALUD — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id  : {RUN_ID}")
    print(f"  destino : {SCHEMA}.{TABLE}")
    print(f"  fecha   : {date.today().isoformat()}  (dinámica)")
    print(f"{'='*65}\n")

    conn = get_conn()

    # Crear tabla si no existe
    crear_tabla(conn)
    log.info("Tabla ingest.scores_salud verificada")

    tickers = obtener_tickers(conn)
    total   = len(tickers)
    log.info(f"Tickers a procesar: {total}")

    if total == 0:
        log.error("Sin tickers en seleccion.universo. Ejecutar aplicar_filtro.py primero.")
        caffeinate.terminate()
        conn.close()
        return

    n_ok   = 0
    n_fail = 0

    for i, ticker in enumerate(tickers, start=1):
        try:
            row = fetch_scores(ticker)

            if row is None:
                n_fail += 1
                registrar_log(conn, ticker, "fail", "Sin datos o error HTTP")
            else:
                insertar(conn, row)
                n_ok += 1
                registrar_log(conn, ticker, "success", "OK")

        except Exception as e:
            n_fail += 1
            log.error(f"[{ticker}] excepción inesperada: {e}")
            registrar_log(conn, ticker, "fail", str(e))
            conn.rollback()

        if i % LOG_CADA == 0:
            log.info(f"Progreso: {i}/{total} — OK={n_ok} FAIL={n_fail}")

        time.sleep(SLEEP_ENTRE)

    conn.close()
    caffeinate.terminate()

    print(f"\n{'='*65}")
    print(f"  RESUMEN FINAL")
    print(f"  TOTAL : {total}")
    print(f"  OK    : {n_ok}")
    print(f"  FAIL  : {n_fail}")
    print(f"  Log   : {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — TOTAL={total} OK={n_ok} FAIL={n_fail}")


if __name__ == "__main__":
    main()
