#!/usr/bin/env python3
"""
calcular_scores.py

Lee de ingest.ratios_ttm e ingest.keymetrics, calcula los scores
Quality (60%) y Value (40%) para todo el universo y escribe en
seleccion.scores.

No hace llamadas a APIs externas — cálculo 100% local con pandas.

snapshot_date = primer día del mes actual (ej: 2026-04-01)

Ejecución: python micro/seleccion/calcular_scores.py
"""

import os
import logging
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
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
LOG_FILE = os.path.join(LOG_DIR, f"calcular_scores_{date.today().isoformat()}.log")

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
SCHEMA = "seleccion"
TABLE  = "scores"

# Ponderaciones Quality
W_QUALITY = {
    "p_roic":             0.25,
    "p_operating_margin": 0.20,
    "p_fcf_quality":      0.20,
    "p_interest_coverage":0.20,
    "p_income_quality":   0.15,
}

# Ponderaciones Value
W_VALUE = {
    "p_price_to_fcf":      0.35,
    "p_ev_to_ebitda":      0.30,
    "p_price_to_earnings": 0.20,
    "p_price_to_book":     0.15,
}

# ── SQL ────────────────────────────────────────────────────────────────────────
SQL_DATOS = """
SELECT
    r.ticker,

    -- Clasificación
    u.sector,
    u.industry,
    u.market_cap_tier,

    -- Quality inputs
    k.roic,
    r.operating_profit_margin,
    r.free_cash_flow_operating_cash_flow_ratio,
    r.interest_coverage_ratio,
    k.income_quality,

    -- Value inputs
    r.price_to_free_cash_flow_ratio,
    k.ev_to_ebitda,
    r.price_to_earnings_ratio,
    r.price_to_book_ratio,

    -- Filtros absolutos (valores reales, no percentiles)
    k.roic                          AS roic_value,
    k.net_debt_to_ebitda,
    r.debt_to_equity_ratio          AS debt_to_equity,
    r.free_cash_flow_per_share      AS fcf_per_share

FROM ingest.ratios_ttm r
JOIN ingest.keymetrics k
    ON  k.ticker         = r.ticker
    AND k.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.keymetrics)
JOIN universos.stock_opciones_2026 u
    ON  u.ticker = r.ticker
WHERE r.fecha_consulta = (SELECT MAX(fecha_consulta) FROM ingest.ratios_ttm)
"""

INSERT_SQL = """
    INSERT INTO seleccion.scores (
        ticker, snapshot_date,
        sector, industry, market_cap_tier,

        p_roic, p_operating_margin, p_fcf_quality,
        p_interest_coverage, p_income_quality,
        quality_score,

        p_price_to_fcf, p_ev_to_ebitda,
        p_price_to_earnings, p_price_to_book,
        value_score,

        multifactor_score, multifactor_rank, multifactor_percentile,

        roic_value, net_debt_to_ebitda, debt_to_equity, fcf_per_share,

        run_id
    )
    VALUES (
        %(ticker)s, %(snapshot_date)s,
        %(sector)s, %(industry)s, %(market_cap_tier)s,

        %(p_roic)s, %(p_operating_margin)s, %(p_fcf_quality)s,
        %(p_interest_coverage)s, %(p_income_quality)s,
        %(quality_score)s,

        %(p_price_to_fcf)s, %(p_ev_to_ebitda)s,
        %(p_price_to_earnings)s, %(p_price_to_book)s,
        %(value_score)s,

        %(multifactor_score)s, %(multifactor_rank)s, %(multifactor_percentile)s,

        %(roic_value)s, %(net_debt_to_ebitda)s, %(debt_to_equity)s, %(fcf_per_share)s,

        %(run_id)s
    )
    ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
        sector                = EXCLUDED.sector,
        industry              = EXCLUDED.industry,
        market_cap_tier       = EXCLUDED.market_cap_tier,
        p_roic                = EXCLUDED.p_roic,
        p_operating_margin    = EXCLUDED.p_operating_margin,
        p_fcf_quality         = EXCLUDED.p_fcf_quality,
        p_interest_coverage   = EXCLUDED.p_interest_coverage,
        p_income_quality      = EXCLUDED.p_income_quality,
        quality_score         = EXCLUDED.quality_score,
        p_price_to_fcf        = EXCLUDED.p_price_to_fcf,
        p_ev_to_ebitda        = EXCLUDED.p_ev_to_ebitda,
        p_price_to_earnings   = EXCLUDED.p_price_to_earnings,
        p_price_to_book       = EXCLUDED.p_price_to_book,
        value_score           = EXCLUDED.value_score,
        multifactor_score     = EXCLUDED.multifactor_score,
        multifactor_rank      = EXCLUDED.multifactor_rank,
        multifactor_percentile= EXCLUDED.multifactor_percentile,
        roic_value            = EXCLUDED.roic_value,
        net_debt_to_ebitda    = EXCLUDED.net_debt_to_ebitda,
        debt_to_equity        = EXCLUDED.debt_to_equity,
        fcf_per_share         = EXCLUDED.fcf_per_share,
        run_id                = EXCLUDED.run_id
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


def leer_datos(conn) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_DATOS)
        rows = cur.fetchall()
    df = pd.DataFrame(rows)
    log.info(f"Filas leídas de ingest: {len(df)}")
    return df


# ── CÁLCULO DE PERCENTILES ─────────────────────────────────────────────────────
def percentil_mayor_mejor(serie: pd.Series) -> pd.Series:
    """
    Percentil 0-1 donde mayor valor = percentil más alto.
    Clip en p1/p99 para manejar outliers extremos.
    NaN → 0 antes de rankear (penaliza ausencia de dato).
    """
    s = serie.copy().astype(float)
    # Clip outliers
    p1  = s.quantile(0.01)
    p99 = s.quantile(0.99)
    s   = s.clip(lower=p1, upper=p99)
    # NaN → 0 (peor percentil posible)
    s   = s.fillna(0)
    return s.rank(pct=True)


def percentil_menor_mejor(serie: pd.Series) -> pd.Series:
    """
    Percentil 0-1 donde menor valor = percentil más alto (se invierte).
    Valores negativos se reemplazan con NaN antes de rankear
    (un P/E negativo no significa que la empresa sea barata).
    NaN → se excluye del ranking; resultado final → 0.
    """
    s = serie.copy().astype(float)
    # Negativos no son "baratos" — se neutralizan
    s = s.where(s > 0, other=np.nan)
    # Clip outliers solo sobre los positivos
    p1  = s.quantile(0.01)
    p99 = s.quantile(0.99)
    s   = s.clip(lower=p1, upper=p99)
    # Rankear: menor valor → rank más alto → invertir
    rank = s.rank(pct=True)
    invertido = 1 - rank
    # Donde era NaN (negativo o ausente) → 0
    invertido = invertido.where(s.notna(), other=0)
    return invertido


# ── PIPELINE DE SCORING ────────────────────────────────────────────────────────
def calcular(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Calculando percentiles Quality...")

    # ── QUALITY (mayor es mejor)
    df["p_roic"]             = percentil_mayor_mejor(df["roic"])
    df["p_operating_margin"] = percentil_mayor_mejor(df["operating_profit_margin"])
    df["p_fcf_quality"]      = percentil_mayor_mejor(df["free_cash_flow_operating_cash_flow_ratio"])
    df["p_interest_coverage"]= percentil_mayor_mejor(df["interest_coverage_ratio"])
    df["p_income_quality"]   = percentil_mayor_mejor(df["income_quality"])

    df["quality_score"] = (
        df["p_roic"]              * W_QUALITY["p_roic"]             +
        df["p_operating_margin"]  * W_QUALITY["p_operating_margin"] +
        df["p_fcf_quality"]       * W_QUALITY["p_fcf_quality"]      +
        df["p_interest_coverage"] * W_QUALITY["p_interest_coverage"]+
        df["p_income_quality"]    * W_QUALITY["p_income_quality"]
    ) * 100

    log.info("Calculando percentiles Value...")

    # ── VALUE (menor ratio es mejor → invertir)
    df["p_price_to_fcf"]      = percentil_menor_mejor(df["price_to_free_cash_flow_ratio"])
    df["p_ev_to_ebitda"]      = percentil_menor_mejor(df["ev_to_ebitda"])
    df["p_price_to_earnings"] = percentil_menor_mejor(df["price_to_earnings_ratio"])
    df["p_price_to_book"]     = percentil_menor_mejor(df["price_to_book_ratio"])

    df["value_score"] = (
        df["p_price_to_fcf"]      * W_VALUE["p_price_to_fcf"]      +
        df["p_ev_to_ebitda"]      * W_VALUE["p_ev_to_ebitda"]       +
        df["p_price_to_earnings"] * W_VALUE["p_price_to_earnings"]  +
        df["p_price_to_book"]     * W_VALUE["p_price_to_book"]
    ) * 100

    # ── SCORE FINAL
    df["multifactor_score"] = (
        df["quality_score"] * 0.60 +
        df["value_score"]   * 0.40
    )

    # Rank (1 = mejor) y percentil
    df["multifactor_rank"]       = df["multifactor_score"].rank(ascending=False, method="min").astype(int)
    df["multifactor_percentile"] = df["multifactor_score"].rank(pct=True) * 100

    # Redondear a 2 decimales para columnas NUMERIC(6,2)
    cols_round2 = [
        "p_roic", "p_operating_margin", "p_fcf_quality",
        "p_interest_coverage", "p_income_quality", "quality_score",
        "p_price_to_fcf", "p_ev_to_ebitda", "p_price_to_earnings",
        "p_price_to_book", "value_score",
        "multifactor_score", "multifactor_percentile",
    ]
    for col in cols_round2:
        df[col] = df[col].round(2)

    # Convertir columnas de percentil de 0-1 a 0-100 para las p_* individuales
    cols_pct = [
        "p_roic", "p_operating_margin", "p_fcf_quality",
        "p_interest_coverage", "p_income_quality",
        "p_price_to_fcf", "p_ev_to_ebitda",
        "p_price_to_earnings", "p_price_to_book",
    ]
    for col in cols_pct:
        df[col] = (df[col] * 100).round(2)

    log.info("Scores calculados.")
    return df


# ── INSERT ─────────────────────────────────────────────────────────────────────
def insertar(conn, df: pd.DataFrame) -> int:
    filas = []
    for _, row in df.iterrows():
        def safe(val):
            """None si NaN o inf — psycopg2 no acepta float NaN."""
            if pd.isna(val) or (isinstance(val, float) and not np.isfinite(val)):
                return None
            return val

        filas.append({
            "ticker":               row["ticker"],
            "snapshot_date":        SNAPSHOT_DATE,
            "sector":               row.get("sector"),
            "industry":             row.get("industry"),
            "market_cap_tier":      row.get("market_cap_tier"),
            "p_roic":               safe(row["p_roic"]),
            "p_operating_margin":   safe(row["p_operating_margin"]),
            "p_fcf_quality":        safe(row["p_fcf_quality"]),
            "p_interest_coverage":  safe(row["p_interest_coverage"]),
            "p_income_quality":     safe(row["p_income_quality"]),
            "quality_score":        safe(row["quality_score"]),
            "p_price_to_fcf":       safe(row["p_price_to_fcf"]),
            "p_ev_to_ebitda":       safe(row["p_ev_to_ebitda"]),
            "p_price_to_earnings":  safe(row["p_price_to_earnings"]),
            "p_price_to_book":      safe(row["p_price_to_book"]),
            "value_score":          safe(row["value_score"]),
            "multifactor_score":    safe(row["multifactor_score"]),
            "multifactor_rank":     int(row["multifactor_rank"]),
            "multifactor_percentile": safe(row["multifactor_percentile"]),
            "roic_value":           safe(row.get("roic_value")),
            "net_debt_to_ebitda":   safe(row.get("net_debt_to_ebitda")),
            "debt_to_equity":       safe(row.get("debt_to_equity")),
            "fcf_per_share":        safe(row.get("fcf_per_share")),
            "run_id":               RUN_ID,
        })

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_SQL, filas, page_size=500)
    conn.commit()
    return len(filas)


def registrar_log(conn, status: str, message: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_SQL, (SCHEMA, TABLE, "BULK", status, message))
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log: {e}")
        conn.rollback()


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*65}")
    print(f"  CALCULAR SCORES — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id        : {RUN_ID}")
    print(f"  snapshot_date : {SNAPSHOT_DATE}  (primer día del mes)")
    print(f"  destino       : {SCHEMA}.{TABLE}")
    print(f"{'='*65}\n")

    conn = get_conn()

    # ── Paso 1: leer datos
    print("  [1/4] Leyendo ingest.ratios_ttm + ingest.keymetrics...")
    df = leer_datos(conn)

    if df.empty:
        log.error("Sin datos en ingest. Ejecutar ingest_ratios_ttm.py e ingest_keymetrics.py primero.")
        conn.close()
        return

    print(f"         {len(df)} empresas leídas\n")

    # ── Paso 2: calcular scores
    print("  [2/4] Calculando percentiles y scores...")
    df = calcular(df)
    print(f"         Quality avg : {df['quality_score'].mean():.1f}")
    print(f"         Value avg   : {df['value_score'].mean():.1f}")
    print(f"         Multi avg   : {df['multifactor_score'].mean():.1f}\n")

    # ── Paso 3: insertar
    print("  [3/4] Insertando en seleccion.scores...")
    try:
        n = insertar(conn, df)
        registrar_log(conn, "success", f"{n} filas insertadas — snapshot {SNAPSHOT_DATE}")
        print(f"         {n} filas escritas\n")
    except Exception as e:
        log.error(f"Error al insertar: {e}")
        registrar_log(conn, "fail", str(e))
        conn.rollback()
        conn.close()
        return

    # ── Paso 4: mostrar resultados
    print("  [4/4] Resultados\n")

    top10 = (
        df[["ticker", "sector", "quality_score", "value_score", "multifactor_score", "multifactor_rank"]]
        .sort_values("multifactor_rank")
        .head(10)
    )

    print(f"  {'─'*65}")
    print(f"  TOP 10 por multifactor_score:")
    print(f"  {'─'*65}")
    print(f"  {'RK':>4}  {'TICKER':<8}  {'SECTOR':<20}  {'QUALITY':>7}  {'VALUE':>6}  {'MULTI':>6}")
    print(f"  {'─'*65}")
    for _, row in top10.iterrows():
        sector_str = str(row.get("sector") or "—")[:20]
        print(
            f"  {int(row['multifactor_rank']):>4}  "
            f"{row['ticker']:<8}  "
            f"{sector_str:<20}  "
            f"{row['quality_score']:>7.1f}  "
            f"{row['value_score']:>6.1f}  "
            f"{row['multifactor_score']:>6.1f}"
        )
    print(f"  {'─'*65}\n")

    print(f"  Distribución del universo ({len(df)} empresas):")
    print(f"    Quality    — avg={df['quality_score'].mean():.1f}  "
          f"p25={df['quality_score'].quantile(0.25):.1f}  "
          f"p75={df['quality_score'].quantile(0.75):.1f}")
    print(f"    Value      — avg={df['value_score'].mean():.1f}  "
          f"p25={df['value_score'].quantile(0.25):.1f}  "
          f"p75={df['value_score'].quantile(0.75):.1f}")
    print(f"    Multifactor— avg={df['multifactor_score'].mean():.1f}  "
          f"p25={df['multifactor_score'].quantile(0.25):.1f}  "
          f"p75={df['multifactor_score'].quantile(0.75):.1f}")

    conn.close()

    print(f"\n{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Log: {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — {len(df)} empresas procesadas, snapshot={SNAPSHOT_DATE}")


if __name__ == "__main__":
    main()
