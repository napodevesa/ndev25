#!/usr/bin/env python3
"""
ingest_ratios_ttm.py

Descarga ratios TTM desde FMP para todos los tickers de
universos.stock_opciones_2026 e inserta en ingest.ratios_ttm.

Endpoint: https://financialmodelingprep.com/stable/ratios-ttm?symbol={ticker}&apikey={API_KEY}

Ejecución: python micro/ingest/ingest_ratios_ttm.py
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
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output_ingest")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"ingest_ratios_ttm_{date.today().isoformat()}.log")

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
TABLE          = "ratios_ttm"
BASE_URL       = "https://financialmodelingprep.com/stable/ratios-ttm"
SLEEP_ENTRE    = 0.25    # segundos entre requests normales
SLEEP_429      = 60      # segundos de espera si recibimos 429
MAX_REINTENTOS = 3       # intentos por ticker antes de marcar FAIL
LOG_CADA       = 100     # loguear progreso cada N tickers

# ── Mapeo FMP → columna DB ─────────────────────────────────────────────────────
CAMPO_MAP = {
    "grossProfitMarginTTM":                        "gross_profit_margin",
    "ebitMarginTTM":                               "ebit_margin",
    "ebitdaMarginTTM":                             "ebitda_margin",
    "operatingProfitMarginTTM":                    "operating_profit_margin",
    "netProfitMarginTTM":                          "net_profit_margin",
    "operatingCashFlowSalesRatioTTM":              "operating_cash_flow_sales_ratio",
    "freeCashFlowOperatingCashFlowRatioTTM":       "free_cash_flow_operating_cash_flow_ratio",
    "interestCoverageRatioTTM":                    "interest_coverage_ratio",
    "debtServiceCoverageRatioTTM":                 "debt_service_coverage_ratio",
    "currentRatioTTM":                             "current_ratio",
    "quickRatioTTM":                               "quick_ratio",
    "debtToEquityRatioTTM":                        "debt_to_equity_ratio",
    "debtToAssetsRatioTTM":                        "debt_to_assets_ratio",
    "longTermDebtToCapitalRatioTTM":               "long_term_debt_to_capital_ratio",
    "priceToEarningsRatioTTM":                     "price_to_earnings_ratio",
    "priceToBookRatioTTM":                         "price_to_book_ratio",
    "priceToSalesRatioTTM":                        "price_to_sales_ratio",
    "priceToFreeCashFlowRatioTTM":                 "price_to_free_cash_flow_ratio",
    "priceToOperatingCashFlowRatioTTM":            "price_to_operating_cash_flow_ratio",
    "enterpriseValueMultipleTTM":                  "enterprise_value_multiple",
    "dividendYieldTTM":                            "dividend_yield",
    "freeCashFlowPerShareTTM":                     "free_cash_flow_per_share",
}

COLUMNAS_DB = list(CAMPO_MAP.values())

# ── SQL ────────────────────────────────────────────────────────────────────────
SQL_TICKERS = """
    SELECT ticker
    FROM universos.stock_opciones_2026
    ORDER BY ticker
"""

INSERT_SQL = f"""
    INSERT INTO {SCHEMA}.{TABLE} (
        ticker,
        fecha_consulta,
        {", ".join(COLUMNAS_DB)},
        run_id
    )
    VALUES (
        %(ticker)s,
        %(fecha_consulta)s,
        {", ".join(f"%({c})s" for c in COLUMNAS_DB)},
        %(run_id)s
    )
    ON CONFLICT (ticker, fecha_consulta) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNAS_DB)},
        run_id        = EXCLUDED.run_id,
        actualizado_en = NOW()
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


# ── API ────────────────────────────────────────────────────────────────────────
def fetch_ratios_ttm(ticker: str) -> dict | None:
    """
    Llama al endpoint FMP stable/ratios-ttm.
    Reintenta hasta MAX_REINTENTOS veces si recibe 429.
    Devuelve el dict mapeado a columnas DB, o None si falla.
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

            # El endpoint stable devuelve un objeto o una lista con un elemento
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

            row = {
                "ticker":         ticker,
                "fecha_consulta": date.today(),
                "run_id":         RUN_ID,
            }
            for fmp_field, db_col in CAMPO_MAP.items():
                val = raw.get(fmp_field)
                # Convertir a float; dejar None si es None o no numérico
                if val is not None:
                    try:
                        val = float(val)
                    except (TypeError, ValueError):
                        val = None
                row[db_col] = val

            return row

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
    print(f"  INGEST RATIOS TTM — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id  : {RUN_ID}")
    print(f"  destino : {SCHEMA}.{TABLE}")
    print(f"  fecha   : {date.today().isoformat()}  (dinámica)")
    print(f"{'='*65}\n")

    conn = get_conn()
    tickers = obtener_tickers(conn)
    total = len(tickers)
    log.info(f"Tickers a procesar: {total}")

    n_ok   = 0
    n_fail = 0

    for i, ticker in enumerate(tickers, start=1):
        try:
            row = fetch_ratios_ttm(ticker)

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
