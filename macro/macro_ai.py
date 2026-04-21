#!/usr/bin/env python3
"""
macro_ai.py

Lee el diagnóstico macro más reciente sin nota AI y genera
un análisis cualitativo con Claude. Guarda en macro.macro_notas_ai.

Ejecución: python macro/macro_ai.py
"""

import os
import json
import subprocess
import requests
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
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, ANTHROPIC_API_KEY]):
    raise EnvironmentError("Faltan variables: POSTGRES_*, ANTHROPIC_API_KEY")

MODELO   = "claude-sonnet-4-20250514"
PROMPT_V = "v1"
RUN_ID   = datetime.now().strftime("%Y%m%d_%H%M")

# ── Conexión ───────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT,
    )

# ── Leer diagnóstico pendiente ─────────────────────────────────────────────────
def leer_diagnostico_pendiente(conn) -> dict | None:
    sql = """
        SELECT d.*
        FROM macro.macro_diagnostico d
        LEFT JOIN macro.macro_notas_ai n ON n.diagnostico_id = d.id
        WHERE n.id IS NULL
        ORDER BY d.calculado_en DESC
        LIMIT 1
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return dict(row) if row else None

# ── Construir prompt ───────────────────────────────────────────────────────────
def construir_prompt(d: dict) -> str:
    return f"""Sos un economista senior especializado en macroeconomía de USA.
Te paso los indicadores macroeconómicos actuales y el diagnóstico ya calculado por un motor de reglas determinístico.
Tu tarea es agregar contexto cualitativo — explicar el "por qué" detrás de los números.

DIAGNÓSTICO DEL MOTOR DE REGLAS:
- Estado macro:    {d['estado_macro']}
- Score de riesgo: {d['score_riesgo']} / 100
- Confianza:       {d['confianza']}
- Verdes:  {d['n_verdes']} | Amarillos: {d['n_amarillos']} | Rojos: {d['n_rojos']}
- Regla disparada: {d['regla_disparada']}

INDICADORES CLAVE:
- Desempleo:        {d['desempleo']}%    → {d['s_desempleo']}
- Fed Funds:        {d['fed_funds']}%    → {d['s_fed']}
- IPC anual:        {d['ipc_anual']}%    → {d['s_ipc']}
- IPC core anual:   {d['core_anual']}%   → {d['s_core']}
- Curva 10Y-2Y:     {d['curva_10y2y']} pp → {d['s_curva']}
- PIB trimestral:   {d['pib_trim']}%    → {d['s_pib']}
- VIX:              {d['vix']}          → {d['s_vix']}

Respondé ÚNICAMENTE con un JSON válido, sin texto antes ni después, sin bloques de código.
El JSON debe tener exactamente esta estructura:

{{
  "resumen": "2-3 oraciones explicando qué está pasando y por qué el motor clasificó como {d['estado_macro']}",
  "riesgos": "los 2-3 riesgos principales en este momento, separados por punto y coma",
  "outlook": "visión concreta para los próximos 3-6 meses",
  "score_sentimiento": <número 0-100, donde 0=muy negativo y 100=muy positivo>,
  "score_recesion": <número 0-100, probabilidad estimada de recesión en 12 meses>,
  "score_inflacion": <número 0-100, riesgo de reaceleración inflacionaria>
}}"""

# ── Llamar a Claude ────────────────────────────────────────────────────────────
def llamar_claude(prompt: str) -> tuple[dict, int]:
    headers = {
        "x-api-key":         ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }
    body = {
        "model":      MODELO,
        "max_tokens": 1000,
        "messages":   [{"role": "user", "content": prompt}],
    }
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers, json=body, timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    texto         = data["content"][0]["text"].strip()
    tokens_usados = data["usage"]["input_tokens"] + data["usage"]["output_tokens"]

    try:
        parsed = json.loads(texto)
    except json.JSONDecodeError:
        texto_limpio = texto.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(texto_limpio)

    return parsed, tokens_usados

# ── Guardar nota ───────────────────────────────────────────────────────────────
def guardar_nota(conn, diagnostico: dict, nota: dict, tokens: int):
    sql = """
        INSERT INTO macro.macro_notas_ai (
            run_id, diagnostico_id, modelo_ai, estado_macro,
            resumen, riesgos, outlook, nota_completa,
            score_sentimiento, score_recesion, score_inflacion,
            tokens_usados, prompt_version
        ) VALUES (
            %(run_id)s, %(diagnostico_id)s, %(modelo_ai)s, %(estado_macro)s,
            %(resumen)s, %(riesgos)s, %(outlook)s, %(nota_completa)s,
            %(score_sentimiento)s, %(score_recesion)s, %(score_inflacion)s,
            %(tokens_usados)s, %(prompt_version)s
        )
    """
    params = {
        "run_id":            diagnostico["run_id"],
        "diagnostico_id":    diagnostico["id"],
        "modelo_ai":         MODELO,
        "estado_macro":      diagnostico["estado_macro"],
        "resumen":           nota.get("resumen"),
        "riesgos":           nota.get("riesgos"),
        "outlook":           nota.get("outlook"),
        "nota_completa":     json.dumps(nota, ensure_ascii=False),
        "score_sentimiento": nota.get("score_sentimiento"),
        "score_recesion":    nota.get("score_recesion"),
        "score_inflacion":   nota.get("score_inflacion"),
        "tokens_usados":     tokens,
        "prompt_version":    PROMPT_V,
    }
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()

# ── Log infraestructura ────────────────────────────────────────────────────────
def registrar_log(conn, status: str, message: str):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO infraestructura.update_logs "
                "(schema_name, table_name, ticker, status, message) "
                "VALUES (%s, %s, %s, %s, %s)",
                ("macro", "macro_notas_ai", "BULK", status, message),
            )
        conn.commit()
    except Exception as e:
        print(f"  ⚠ No se pudo registrar log: {e}")
        conn.rollback()

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    caffeinate = subprocess.Popen(["caffeinate"])

    print(f"\n{'='*65}")
    print(f"  MACRO AI — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id : {RUN_ID}")
    print(f"{'='*65}\n")

    conn = get_conn()

    print("  Buscando diagnóstico sin nota AI...")
    diagnostico = leer_diagnostico_pendiente(conn)

    if not diagnostico:
        print("  ✓ Todos los diagnósticos ya tienen nota AI. Nada que hacer.")
        registrar_log(conn, "skip", "Sin diagnósticos pendientes")
        conn.close()
        caffeinate.terminate()
        return

    print(f"  → Diagnóstico encontrado: {diagnostico['estado_macro']} "
          f"| run_id: {diagnostico['run_id']} "
          f"| score: {diagnostico['score_riesgo']}")

    print("  Llamando a Claude API...")
    prompt = construir_prompt(diagnostico)

    try:
        nota, tokens = llamar_claude(prompt)
    except Exception as e:
        print(f"  ✗ Error en Claude API: {e}")
        registrar_log(conn, "fail", str(e))
        conn.close()
        caffeinate.terminate()
        raise

    print(f"  ✓ Respuesta recibida ({tokens} tokens)")

    print(f"\n  {'─'*55}")
    print(f"  Estado:     {diagnostico['estado_macro']}")
    print(f"  Resumen:    {nota.get('resumen', '—')}")
    print(f"  Riesgos:    {nota.get('riesgos', '—')}")
    print(f"  Outlook:    {nota.get('outlook', '—')}")
    print(f"  Sentimiento:{nota.get('score_sentimiento')} / 100")
    print(f"  Recesión:   {nota.get('score_recesion')} / 100")
    print(f"  Inflación:  {nota.get('score_inflacion')} / 100")
    print(f"  {'─'*55}\n")

    print("  Guardando en macro_notas_ai...")
    guardar_nota(conn, diagnostico, nota, tokens)
    print(f"  ✓ Nota guardada (run_id: {diagnostico['run_id']})")

    registrar_log(conn, "success",
                  f"estado={diagnostico['estado_macro']} tokens={tokens} run_id={RUN_ID}")
    conn.close()
    caffeinate.terminate()

    print(f"\n{'='*65}")
    print(f"  Pipeline AI completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
