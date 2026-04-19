#!/usr/bin/env python3
"""
ingest_keymetrics_hist.py

Lee los tickers de seleccion.universo y descarga el histórico anual
de key metrics desde FMP para cada empresa (hasta 5 registros FY).

Se guardan TODOS los registros históricos, no solo el más reciente.
Usado principalmente para calcular regresiones de ROIC y deuda.

Endpoint:
  https://financialmodelingprep.com/stable/key-metrics?symbol={ticker}&apikey={API_KEY}

Ejecución: python micro/seleccion/ingest_keymetrics_hist.py
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
LOG_FILE = os.path.join(LOG_DIR, f"ingest_keymetrics_hist_{date.today().isoformat()}.log")

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
TABLE          = "keymetrics_hist"
BASE_URL       = "https://financialmodelingprep.com/stable/key-metrics"
SLEEP_ENTRE    = 0.25
SLEEP_429      = 60
MAX_REINTENTOS = 3
LOG_CADA       = 100

# ── Mapeo FMP → columna DB ─────────────────────────────────────────────────────
CAMPO_MAP = {
    "returnOnInvestedCapital": "roic",
    "returnOnEquity":          "roe",
    "operatingReturnOnAssets": "operating_return_assets",
    "incomeQuality":           "income_quality",
    "evToEBITDA":              "ev_to_ebitda",
    "netDebtToEBITDA":         "net_debt_to_ebitda",
    "freeCashFlowYield":       "fcf_yield",
    "earningsYield":           "earnings_yield",
    "currentRatio":            "current_ratio",
    "workingCapital":          "working_capital",
    "marketCap":               "market_cap",
    "enterpriseValue":         "enterprise_value",
    "investedCapital":         "invested_capital",
}

COLUMNAS_DB = list(CAMPO_MAP.values())

# ── SQL ────────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ingest.keymetrics_hist (
    ticker                  VARCHAR(10)     NOT NULL,
    fecha_reporte           DATE            NOT NULL,
    fiscal_year             VARCHAR(10),
    periodo                 VARCHAR(10),

    roic                    NUMERIC(16,4),
    roe                     NUMERIC(16,4),
    operating_return_assets NUMERIC(16,4),
    income_quality          NUMERIC(16,4),
    ev_to_ebitda            NUMERIC(16,4),
    net_debt_to_ebitda      NUMERIC(16,4),
    fcf_yield               NUMERIC(16,4),
    earnings_yield          NUMERIC(16,4),

    current_ratio           NUMERIC(16,4),
    working_capital         NUMERIC(20,2),

    market_cap              NUMERIC(20,2),
    enterprise_value        NUMERIC(20,2),
    invested_capital        NUMERIC(20,2),

    fecha_consulta          DATE            NOT NULL,
    run_id                  VARCHAR(40),
    creado_en               TIMESTAMPTZ     DEFAULT NOW(),

    PRIMARY KEY (ticker, fecha_reporte)
)
"""

SQL_TICKERS = """
    SELECT u.ticker
    FROM seleccion.universo u
    LEFT JOIN ingest.keymetrics_hist h
        ON  h.ticker = u.ticker
        AND EXTRACT(YEAR FROM h.fecha_reporte) =
            EXTRACT(YEAR FROM CURRENT_DATE) - 1
    WHERE u.snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.universo)
      AND h.ticker IS NULL
    ORDER BY u.ticker
"""

SQL_CONTEO_UNIVERSO = """
    SELECT COUNT(*)
    FROM seleccion.universo
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.universo)
"""

INSERT_SQL = f"""
    INSERT INTO ingest.keymetrics_hist (
        ticker, fecha_reporte, fiscal_year, periodo,
        {", ".join(COLUMNAS_DB)},
        fecha_consulta, run_id
    )
    VALUES (
        %(ticker)s, %(fecha_reporte)s, %(fiscal_year)s, %(periodo)s,
        {", ".join(f"%({c})s" for c in COLUMNAS_DB)},
        %(fecha_consulta)s, %(run_id)s
    )
    ON CONFLICT (ticker, fecha_reporte) DO UPDATE SET
        fiscal_year             = EXCLUDED.fiscal_year,
        periodo                 = EXCLUDED.periodo,
        {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNAS_DB)},
        fecha_consulta          = EXCLUDED.fecha_consulta,
        run_id                  = EXCLUDED.run_id
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


def crear_tabla(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def obtener_tickers(conn) -> tuple[list[str], int]:
    """
    Devuelve (tickers_sin_datos, total_universo).
    Solo incluye tickers que no tienen ningún registro del año fiscal anterior.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_CONTEO_UNIVERSO)
        total_universo = cur.fetchone()[0]
        cur.execute(SQL_TICKERS)
        tickers = [row[0] for row in cur.fetchall()]
    return tickers, total_universo


# ── API ────────────────────────────────────────────────────────────────────────
def fetch_keymetrics_hist(ticker: str) -> list[dict] | None:
    """
    Llama al endpoint FMP key-metrics.
    Devuelve TODOS los registros históricos (lista completa), no solo el primero.
    Reintenta hasta MAX_REINTENTOS veces si recibe 429.
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

            if not isinstance(data, list) or not data:
                log.warning(f"[{ticker}] respuesta vacía o formato inesperado")
                return None

            filas = []
            hoy   = date.today()

            for item in data:
                fecha_raw = item.get("date")
                if not fecha_raw:
                    continue

                def to_float(val):
                    if val is None:
                        return None
                    try:
                        return float(val)
                    except (TypeError, ValueError):
                        return None

                fila = {
                    "ticker":         ticker,
                    "fecha_reporte":  fecha_raw,
                    "fiscal_year":    item.get("fiscalYear"),
                    "periodo":        item.get("period"),
                    "fecha_consulta": hoy,
                    "run_id":         RUN_ID,
                }
                for fmp_field, db_col in CAMPO_MAP.items():
                    fila[db_col] = to_float(item.get(fmp_field))

                filas.append(fila)

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

    print(f"\n{'='*65}")
    print(f"  INGEST KEY METRICS HIST — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id  : {RUN_ID}")
    print(f"  destino : {SCHEMA}.{TABLE}")
    print(f"  fecha   : {date.today().isoformat()}  (dinámica)")
    print(f"{'='*65}\n")

    conn = get_conn()

    crear_tabla(conn)
    log.info("Tabla ingest.keymetrics_hist verificada")

    anio_objetivo = date.today().year - 1
    tickers, total_universo = obtener_tickers(conn)
    total      = len(tickers)
    ya_tienen  = total_universo - total

    print(f"  Año fiscal objetivo    : {anio_objetivo}")
    print(f"  Tickers sin datos      : {total}")
    print(f"  Ya tienen datos        : {ya_tienen}")
    print()

    log.info(f"Año fiscal objetivo: {anio_objetivo} — sin datos: {total}, ya tienen: {ya_tienen}")

    if total == 0:
        print("  Todos los tickers ya tienen datos del año fiscal anterior. Nada que hacer.")
        log.info("Nada que procesar — universo completo para el año objetivo.")
        caffeinate.terminate()
        conn.close()
        return

    n_ok         = 0
    n_fail       = 0
    n_registros  = 0   # total de filas históricas insertadas

    for i, ticker in enumerate(tickers, start=1):
        try:
            filas = fetch_keymetrics_hist(ticker)

            if filas is None:
                n_fail += 1
                registrar_log(conn, ticker, "fail", "Sin datos o error HTTP")
            else:
                n_insertadas  = insertar(conn, filas)
                n_ok         += 1
                n_registros  += n_insertadas
                registrar_log(conn, ticker, "success", f"{n_insertadas} registros históricos")

        except Exception as e:
            n_fail += 1
            log.error(f"[{ticker}] excepción inesperada: {e}")
            registrar_log(conn, ticker, "fail", str(e))
            conn.rollback()

        if i % LOG_CADA == 0:
            log.info(
                f"Progreso: {i}/{total} — "
                f"OK={n_ok} FAIL={n_fail} REGISTROS={n_registros}"
            )

        time.sleep(SLEEP_ENTRE)

    conn.close()
    caffeinate.terminate()

    print(f"\n{'='*65}")
    print(f"  RESUMEN FINAL")
    print(f"  TOTAL tickers  : {total}")
    print(f"  OK             : {n_ok}")
    print(f"  FAIL           : {n_fail}")
    print(f"  REGISTROS hist : {n_registros}  (~{n_registros // max(n_ok, 1):.1f} por ticker)")
    print(f"  Log            : {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — TOTAL={total} OK={n_ok} FAIL={n_fail} REGISTROS={n_registros}")


if __name__ == "__main__":
    main()
