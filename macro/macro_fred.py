#!/usr/bin/env python3
"""
macro_fred.py

Descarga 15 series macroeconómicas de FRED, calcula semáforos,
inserta en macro.macro_raw y guarda el diagnóstico en macro.macro_diagnostico.

Ejecución: python macro/macro_fred.py
"""

import os
import subprocess
import requests
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv

# ── ENV ────────────────────────────────────────────────────────────────────────
load_dotenv("/Users/ndev/Desktop/ndev25/.env")

POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5433")
FRED_API_KEY      = os.getenv("FRED_API_KEY")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, FRED_API_KEY]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
RUN_ID   = datetime.now().strftime("%Y%m%d_%H%M")

# ── Conexión ───────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT,
    )

# ── Series a descargar ─────────────────────────────────────────────────────────
SERIES = {
    "Tasa de Desempleo":            ("UNRATE",          "%",     "Mercado laboral"),
    "Tasa Fed Funds (objetivo sup)":("DFEDTARU",        "%",     "Política monetaria"),
    "IPC (índice nivel)":           ("CPIAUCSL",        "índice","Base cálculo inflación"),
    "IPC Subyacente (core)":        ("CPILFESL",        "índice","Sin alimentos ni energía"),
    "Curva Rendimiento 10Y-2Y":     ("T10Y2Y",          "pp",    "Spread de tasas"),
    "Bono 2Y":                      ("DGS2",            "%",     "Parte corta curva"),
    "Bono 10Y":                     ("DGS10",           "%",     "Parte larga curva"),
    "PIB Real (var. trimestral)":   ("A191RL1Q225SBEA", "%",     "Crecimiento económico"),
    "Nóminas no agrícolas (NFP)":   ("PAYEMS",          "miles", "Empleo total"),
    "Ventas minoristas":            ("RSAFS",           "M USD", "Consumo"),
    "Expectativas inflación 5Y":    ("T5YIE",           "%",     "Credibilidad Fed"),
    "Cond. financieras (NFCI)":     ("NFCI",            "índice","Crédito+liquidez+riesgo"),
    "VIX (rezago 1 día)":           ("VIXCLS",          "puntos","Volatilidad implícita"),
    "Permisos de construcción":     ("PERMIT",          "miles", "Indicador líder"),
    "Crédito al consumo":           ("TOTALSL",         "M USD", "Estrés financiero"),
}

SERIE_ID_MAP = {
    "CPIAUCSL": "IPC_ANUAL",
    "CPILFESL": "CORE_ANUAL",
}

# ── Semáforos ──────────────────────────────────────────────────────────────────
def interpretar(nombre, valor):
    try:
        v = float(valor)
    except (ValueError, TypeError):
        return None, None

    reglas = {
        "Tasa de Desempleo": [
            (4.0,  "verde",    "Pleno empleo"),
            (5.5,  "amarillo", "Mercado laboral enfriando"),
            (float("inf"), "rojo", "Desempleo elevado"),
        ],
        "Tasa Fed Funds (objetivo sup)": [
            (2.5,  "verde",    "Política acomodaticia"),
            (4.5,  "amarillo", "Política restrictiva moderada"),
            (float("inf"), "rojo", "Política muy restrictiva"),
        ],
        "IPC Anual (%)": [
            (2.5,  "verde",    "Cerca del objetivo 2%"),
            (4.0,  "amarillo", "Inflación elevada"),
            (float("inf"), "rojo", "Inflación muy alta"),
        ],
        "IPC Subyacente Anual (%)": [
            (2.5,  "verde",    "Core bajo control"),
            (4.0,  "amarillo", "Core persistente"),
            (float("inf"), "rojo", "Core muy elevado"),
        ],
        "Curva Rendimiento 10Y-2Y": [
            (-0.01, "rojo",    "Curva invertida — señal recesión"),
            (0.5,   "amarillo","Curva plana o normalizando"),
            (float("inf"), "verde", "Curva normal"),
        ],
        "PIB Real (var. trimestral)": [
            (0.0,  "rojo",    "Contracción"),
            (1.5,  "amarillo","Crecimiento débil"),
            (float("inf"), "verde", "Crecimiento sano"),
        ],
        "VIX (rezago 1 día)": [
            (20.0, "verde",    "Baja volatilidad"),
            (30.0, "amarillo", "Volatilidad elevada"),
            (float("inf"), "rojo", "Pánico / stress sistémico"),
        ],
        "Expectativas inflación 5Y": [
            (2.5,  "verde",    "Bien ancladas"),
            (3.0,  "amarillo", "Expectativas subiendo"),
            (float("inf"), "rojo", "Desancladas"),
        ],
        "Cond. financieras (NFCI)": [
            (0.0,  "verde",    "Condiciones laxas"),
            (0.5,  "amarillo", "Condiciones tensionando"),
            (float("inf"), "rojo", "Condiciones muy restrictivas"),
        ],
        "Bono 2Y": [
            (3.0,  "verde",    "Tasas cortas bajas"),
            (4.5,  "amarillo", "Tasas cortas elevadas"),
            (float("inf"), "rojo", "Tasas cortas muy altas"),
        ],
        "Bono 10Y": [
            (3.5,  "verde",    "Tasas largas moderadas"),
            (4.5,  "amarillo", "Tasas largas presionando"),
            (float("inf"), "rojo", "Tasas largas muy altas"),
        ],
        "Nóminas no agrícolas (NFP)": [
            (100,   "rojo",    "Creación de empleo muy débil"),
            (150,   "amarillo","Empleo desacelerando"),
            (float("inf"), "verde", "Creación de empleo sana"),
        ],
        "Ventas minoristas": [
            (650000, "rojo",    "Consumo deprimido"),
            (700000, "amarillo","Consumo moderado"),
            (float("inf"), "verde", "Consumo sólido"),
        ],
        "Permisos de construcción": [
            (1000,  "rojo",    "Construcción muy débil"),
            (1400,  "amarillo","Construcción moderada"),
            (float("inf"), "verde", "Construcción activa"),
        ],
        "Crédito al consumo": [
            (4800000, "verde",    "Crédito en niveles normales"),
            (5200000, "amarillo", "Crédito elevado, posible estrés"),
            (float("inf"), "rojo", "Sobreendeudamiento"),
        ],
    }

    for umbral, semaforo, nota in reglas.get(nombre, []):
        if v <= umbral:
            return semaforo, nota
    return None, None

# ── Descarga FRED ──────────────────────────────────────────────────────────────
def get_observations(series_id, limit=1):
    params = {
        "series_id":  series_id,
        "api_key":    FRED_API_KEY,
        "file_type":  "json",
        "sort_order": "desc",
        "limit":      limit,
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=10)
        r.raise_for_status()
        return [o for o in r.json().get("observations", []) if o["value"] != "."]
    except Exception as e:
        print(f"  ⚠ Error en {series_id}: {e}")
        return []


def calcular_variacion_anual(series_id):
    params = {
        "series_id":  series_id,
        "api_key":    FRED_API_KEY,
        "file_type":  "json",
        "sort_order": "desc",
        "limit":      14,
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=10)
        r.raise_for_status()
        obs = [o for o in r.json().get("observations", []) if o["value"] != "."]
        if len(obs) >= 13:
            v_actual   = float(obs[0]["value"])
            v_anterior = float(obs[12]["value"])
            return obs[0]["date"], round((v_actual / v_anterior - 1) * 100, 2)
        elif len(obs) >= 2:
            v_actual   = float(obs[0]["value"])
            v_anterior = float(obs[-1]["value"])
            print(f"  ℹ {series_id}: variación aproximada ({len(obs)} obs.)")
            return obs[0]["date"], round((v_actual / v_anterior - 1) * 100, 2)
    except Exception as e:
        print(f"  ⚠ Error variación anual {series_id}: {e}")
    return None, None

# ── INSERT macro_raw ───────────────────────────────────────────────────────────
def insertar_en_db(conn, filas: list[dict]):
    sql = """
        INSERT INTO macro.macro_raw
            (serie_id, fecha_dato, valor, valor_texto, semaforo, nota_semaforo, run_id)
        VALUES
            (%(serie_id)s, %(fecha_dato)s, %(valor)s, %(valor_texto)s,
             %(semaforo)s, %(nota_semaforo)s, %(run_id)s)
        ON CONFLICT (serie_id, fecha_dato) DO NOTHING
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, filas)
    conn.commit()
    print(f"  ✓ {len(filas)} filas procesadas en macro_raw (run_id: {RUN_ID})")

# ── Guardar diagnóstico ────────────────────────────────────────────────────────
def guardar_diagnostico(conn, run_id: str):
    sql_select = "SELECT * FROM macro.v_diagnostico"
    sql_insert = """
        INSERT INTO macro.macro_diagnostico (
            run_id,
            estado_macro, confianza, score_riesgo,
            n_verdes, n_amarillos, n_rojos, regla_disparada,
            s_desempleo, s_fed, s_ipc, s_core,
            s_curva, s_pib, s_vix, s_expectativas, s_nfci,
            desempleo, fed_funds, ipc_anual, core_anual,
            curva_10y2y, pib_trim, vix
        ) VALUES (
            %(run_id)s,
            %(estado_macro)s, %(confianza)s, %(score_riesgo)s,
            %(n_verdes)s, %(n_amarillos)s, %(n_rojos)s, %(regla_disparada)s,
            %(s_desempleo)s, %(s_fed)s, %(s_ipc)s, %(s_core)s,
            %(s_curva)s, %(s_pib)s, %(s_vix)s, %(s_expectativas)s, %(s_nfci)s,
            %(desempleo)s, %(fed_funds)s, %(ipc_anual)s, %(core_anual)s,
            %(curva_10y2y)s, %(pib_trim)s, %(vix)s
        )
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql_select)
        fila = cur.fetchone()
        if not fila:
            print("  ⚠ v_diagnostico no devolvió resultados")
            return
        fila = dict(fila)
        fila["run_id"] = run_id

    with conn.cursor() as cur:
        cur.execute(sql_insert, fila)
    conn.commit()

    print(f"  ✓ Diagnóstico guardado: {fila['estado_macro']} "
          f"| score {fila['score_riesgo']} "
          f"| confianza {fila['confianza']} "
          f"| {fila['n_verdes']}V {fila['n_amarillos']}A {fila['n_rojos']}R")

# ── Log infraestructura ────────────────────────────────────────────────────────
def registrar_log(conn, status: str, message: str):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO infraestructura.update_logs "
                "(schema_name, table_name, ticker, status, message) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("macro", "macro_raw", "BULK", status, message),
            )
        conn.commit()
    except Exception as e:
        print(f"  ⚠ No se pudo registrar log: {e}")
        conn.rollback()

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    caffeinate = subprocess.Popen(["caffeinate"])

    print(f"\n{'='*65}")
    print(f"  MACRO USA — {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  run: {RUN_ID}")
    print(f"{'='*65}\n")

    resultados = []
    filas_db   = []

    for nombre, (s_id, unidad, _) in SERIES.items():

        if s_id in ("CPIAUCSL", "CPILFESL"):
            fecha, valor   = calcular_variacion_anual(s_id)
            nombre_display = nombre.replace("(índice nivel)", "Anual (%)")
            serie_id_db    = SERIE_ID_MAP[s_id]
            unidad_display = "%"
        else:
            obs = get_observations(s_id)
            fecha, valor   = (obs[0]["date"], obs[0]["value"]) if obs else (None, None)
            nombre_display = nombre
            serie_id_db    = s_id
            unidad_display = unidad

        semaforo, nota = interpretar(nombre_display, valor)

        resultados.append({
            "Indicador": nombre_display,
            "Fecha":     fecha or "—",
            "Valor":     valor or "N/D",
            "Unidad":    unidad_display,
            "Semáforo":  semaforo or "—",
            "Nota":      nota or "Sin dato",
        })

        if fecha and valor is not None:
            try:
                valor_num = float(valor)
            except (ValueError, TypeError):
                valor_num = None

            filas_db.append({
                "serie_id":     serie_id_db,
                "fecha_dato":   fecha,
                "valor":        valor_num,
                "valor_texto":  str(valor),
                "semaforo":     semaforo,
                "nota_semaforo":nota,
                "run_id":       RUN_ID,
            })

    # Resumen en pantalla
    df = pd.DataFrame(resultados)
    pd.set_option("display.max_colwidth", 40)
    pd.set_option("display.width", 120)
    print(df[["Indicador","Fecha","Valor","Unidad","Semáforo","Nota"]].to_string(index=False))

    print(f"\n{'─'*65}")
    print(f"  verde:    {sum(1 for r in resultados if r['Semáforo'] == 'verde')}")
    print(f"  amarillo: {sum(1 for r in resultados if r['Semáforo'] == 'amarillo')}")
    print(f"  rojo:     {sum(1 for r in resultados if r['Semáforo'] == 'rojo')}")
    print(f"{'─'*65}\n")

    # Insertar en DB
    print("  Insertando en macro_raw...")
    conn = get_conn()
    try:
        insertar_en_db(conn, filas_db)
        guardar_diagnostico(conn, RUN_ID)
        registrar_log(conn, "success",
                      f"{len(filas_db)} series — run_id={RUN_ID}")
    except Exception as e:
        print(f"  ✗ Error de DB: {e}")
        registrar_log(conn, "fail", str(e))
        conn.close()
        caffeinate.terminate()
        raise
    finally:
        conn.close()

    caffeinate.terminate()

    print(f"\n{'='*65}")
    print(f"  Pipeline completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
