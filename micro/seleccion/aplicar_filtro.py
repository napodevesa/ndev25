#!/usr/bin/env python3
"""
aplicar_filtro.py

Lee de seleccion.scores y aplica los filtros absolutos de calidad
para poblar seleccion.universo (~700 empresas sanas).

Filtros:
  ROIC > 4%           — empresa genera retorno real sobre capital
  Net Debt/EBITDA < 3 — deuda manejable
  D/E < 0.8           — balance sólido
  FCF/share > 0       — genera cash real

Es SQL puro — sin API calls, sin caffeinate.

Ejecución: python micro/seleccion/aplicar_filtro.py
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
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5433))

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID        = datetime.now().strftime("%Y%m%d_%H%M")
SNAPSHOT_DATE = date(date.today().year, date.today().month, 1)

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"aplicar_filtro_{date.today().isoformat()}.log")

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
TABLE  = "universo"

# ── SQL ────────────────────────────────────────────────────────────────────────
INSERT_UNIVERSO = """
    INSERT INTO seleccion.universo (
        ticker, snapshot_date, sector, industry, market_cap_tier,
        quality_score, value_score, multifactor_score,
        multifactor_rank, multifactor_percentile,
        roic_value, net_debt_to_ebitda, debt_to_equity, fcf_per_share,
        run_id
    )
    SELECT
        ticker, snapshot_date, sector, industry, market_cap_tier,
        quality_score, value_score, multifactor_score,
        multifactor_rank, multifactor_percentile,
        roic_value, net_debt_to_ebitda, debt_to_equity, fcf_per_share,
        %(run_id)s
    FROM seleccion.scores
    WHERE snapshot_date       = (SELECT MAX(snapshot_date) FROM seleccion.scores)
      AND roic_value          > 0.04
      AND net_debt_to_ebitda  < 3.0
      AND debt_to_equity      < 0.8
      AND fcf_per_share       > 0
    ON CONFLICT (ticker, snapshot_date) DO UPDATE SET
        quality_score          = EXCLUDED.quality_score,
        value_score            = EXCLUDED.value_score,
        multifactor_score      = EXCLUDED.multifactor_score,
        multifactor_rank       = EXCLUDED.multifactor_rank,
        multifactor_percentile = EXCLUDED.multifactor_percentile,
        run_id                 = EXCLUDED.run_id
"""

SQL_RESUMEN = """
    SELECT
        ticker, sector, industry, market_cap_tier,
        quality_score, value_score, multifactor_score, multifactor_rank
    FROM seleccion.universo
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.universo)
    ORDER BY multifactor_rank
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
    print(f"  APLICAR FILTRO — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id        : {RUN_ID}")
    print(f"  snapshot_date : {SNAPSHOT_DATE}  (primer día del mes)")
    print(f"  destino       : {SCHEMA}.{TABLE}")
    print(f"{'='*65}\n")

    conn = get_conn()

    # ── Paso 1: insertar universo filtrado
    print("  [1/3] Aplicando filtros absolutos sobre seleccion.scores...")
    print("        ROIC > 4%  |  Net Debt/EBITDA < 3  |  D/E < 0.8  |  FCF/share > 0\n")

    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_UNIVERSO, {"run_id": RUN_ID})
            n = cur.rowcount
        conn.commit()
    except Exception as e:
        log.error(f"Error al insertar universo: {e}")
        registrar_log(conn, "fail", str(e))
        conn.rollback()
        conn.close()
        return

    log.info(f"Universo insertado: {n} empresas")
    registrar_log(conn, "success", f"{n} empresas en universo de trabajo")
    print(f"         {n} empresas pasaron el filtro\n")

    # ── Paso 2: leer para mostrar resumen
    print("  [2/3] Generando resumen...\n")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_RESUMEN)
        rows = cur.fetchall()

    if not rows:
        log.warning("No se encontraron filas en seleccion.universo para el resumen.")
        conn.close()
        return

    # Agrupar manualmente sin pandas
    total = len(rows)

    # Distribución por market_cap_tier
    mktcap_conteo: dict[str, int] = {}
    for r in rows:
        tier = r["market_cap_tier"] or "—"
        mktcap_conteo[tier] = mktcap_conteo.get(tier, 0) + 1

    # Distribución por sector (top 5)
    sector_conteo: dict[str, int] = {}
    for r in rows:
        sec = r["sector"] or "—"
        sector_conteo[sec] = sector_conteo.get(sec, 0) + 1
    top5_sectores = sorted(sector_conteo.items(), key=lambda x: x[1], reverse=True)[:5]

    # Promedios de scores
    avg_quality = sum(float(r["quality_score"] or 0) for r in rows) / total
    avg_value   = sum(float(r["value_score"]   or 0) for r in rows) / total
    avg_multi   = sum(float(r["multifactor_score"] or 0) for r in rows) / total

    # Top 5 por multifactor_score (ya ordenado por multifactor_rank)
    top5 = rows[:5]

    # ── Paso 3: imprimir resumen
    print("  [3/3] Resumen del universo de trabajo\n")

    print(f"  {'─'*65}")
    print(f"  Total empresas en universo : {total}")
    print(f"  {'─'*65}\n")

    print(f"  Por market_cap_tier:")
    for tier, cnt in sorted(mktcap_conteo.items(), key=lambda x: x[1], reverse=True):
        pct = cnt / total * 100
        print(f"    {tier:<10} {cnt:>4}  ({pct:.1f}%)")

    print(f"\n  Por sector (top 5):")
    for sec, cnt in top5_sectores:
        pct = cnt / total * 100
        print(f"    {sec:<28} {cnt:>4}  ({pct:.1f}%)")

    print(f"\n  Scores promedio del universo filtrado:")
    print(f"    Quality     : {avg_quality:.1f}")
    print(f"    Value       : {avg_value:.1f}")
    print(f"    Multifactor : {avg_multi:.1f}")

    print(f"\n  Top 5 por multifactor_score:")
    print(f"  {'─'*65}")
    print(f"  {'RK':>4}  {'TICKER':<8}  {'SECTOR':<24}  {'QUALITY':>7}  {'VALUE':>6}  {'MULTI':>6}")
    print(f"  {'─'*65}")
    for r in top5:
        sector_str = str(r["sector"] or "—")[:24]
        print(
            f"  {int(r['multifactor_rank']):>4}  "
            f"{r['ticker']:<8}  "
            f"{sector_str:<24}  "
            f"{float(r['quality_score'] or 0):>7.1f}  "
            f"{float(r['value_score'] or 0):>6.1f}  "
            f"{float(r['multifactor_score'] or 0):>6.1f}"
        )
    print(f"  {'─'*65}")

    conn.close()

    print(f"\n{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Log: {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — universo={total}, snapshot={SNAPSHOT_DATE}")


if __name__ == "__main__":
    main()
