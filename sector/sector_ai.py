#!/usr/bin/env python3
"""
sector_ai.py

Lee el diagnóstico sectorial más reciente y genera un análisis
cualitativo de rotación sectorial con Claude.
Guarda en sector.sector_notas_ai.

Ejecución: python sector/sector_ai.py
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

# ── Verificar nota existente ───────────────────────────────────────────────────
def ya_tiene_nota(conn, run_id: str) -> bool:
    sql = "SELECT 1 FROM sector.sector_notas_ai WHERE run_id = %s LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        return cur.fetchone() is not None

# ── Leer diagnóstico ───────────────────────────────────────────────────────────
def leer_diagnostico(conn) -> dict | None:
    sql = "SELECT * FROM sector.v_sector_diagnostico"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return dict(row) if row else None

# ── Leer top industrias ────────────────────────────────────────────────────────
def leer_top_industrias(conn) -> list[dict]:
    sql = """
        SELECT ticker, industria, sector_gics, estado, alineacion_macro,
               score_momentum, score_volumen, score_total,
               ret_1m, ret_3m, ret_6m, rsi_rs_semanal,
               vol_ratio, rank_total
        FROM sector.v_sector_ranking
        WHERE tipo = 'industria'
        ORDER BY rank_total
        LIMIT 10
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]

# ── Leer top sectores SPDR ─────────────────────────────────────────────────────
def leer_top_sectores(conn) -> list[dict]:
    sql = """
        SELECT ticker, industria, estado, alineacion_macro,
               score_total, ret_3m, rsi_rs_semanal, rank_total
        FROM sector.v_sector_ranking
        WHERE tipo = 'sector'
        ORDER BY rank_total
        LIMIT 5
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]

# ── Leer nota macro vigente ────────────────────────────────────────────────────
def leer_nota_macro(conn) -> str | None:
    sql = """
        SELECT resumen, outlook
        FROM macro.macro_notas_ai
        ORDER BY generado_en DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if row:
            return f"Resumen macro: {row[0]} | Outlook: {row[1]}"
        return None

# ── Construir prompt ───────────────────────────────────────────────────────────
def construir_prompt(diag: dict, top_industrias: list, top_sectores: list,
                     nota_macro: str) -> str:
    industrias_txt = "\n".join([
        f"  {r['ticker']:6} | {r['industria']:<30} | estado: {r['estado']:<15} "
        f"| alineacion: {r['alineacion_macro']:<10} | score: {r['score_total']:.0f} "
        f"| ret_3m: {r['ret_3m']:.1f}% | RSI_sem: {r['rsi_rs_semanal']:.0f}"
        for r in top_industrias
    ])
    sectores_txt = "\n".join([
        f"  {r['ticker']:6} | score: {r['score_total']:.0f} | ret_3m: {r['ret_3m']:.1f}% "
        f"| estado: {r['estado']}"
        for r in top_sectores
    ])
    return f"""Sos un portfolio manager especializado en rotación sectorial de mercados USA.
Te paso el análisis cuantitativo del momento sectorial. Tu tarea es agregar interpretación
cualitativa — explicar qué está rotando, por qué, y qué oportunidades/riesgos ves.

CONTEXTO MACRO:
- Estado macro: {diag['estado_macro']}
- {nota_macro or 'Sin nota macro disponible'}

RESUMEN DEL UNIVERSO SECTORIAL:
- Líderes fuertes (LEADING_STRONG): {diag['n_leading_strong']}
- Líderes débiles (LEADING_WEAK):   {diag['n_leading_weak']}
- Neutrales:                         {diag['n_neutral']}
- Rezagados (LAGGING):               {diag['n_lagging']}
- Alineados con macro:               {diag['n_aligned']}
- Score promedio del universo:       {diag['score_universo']}
- Señal de rotación:                 {diag['señal_rotacion']}
- Top alineados con macro:           {diag['top_tickers_aligned']}
- Top global:                        {diag['top_tickers_global']}

TOP 10 INDUSTRIAS (por score):
{industrias_txt}

TOP 5 SECTORES SPDR:
{sectores_txt}

Respondé ÚNICAMENTE con un JSON válido, sin texto antes ni después, sin bloques de código.
Estructura exacta:

{{
  "resumen": "2-3 oraciones: qué está rotando ahora y por qué dado el contexto macro",
  "oportunidades": "las 2-3 industrias con mejor setup riesgo/retorno y por qué",
  "riesgos": "sectores o industrias a evitar y por qué",
  "score_rotacion": <0-100: qué tan clara y fuerte es la rotación actual>,
  "score_riesgo": <0-100: nivel de riesgo sectorial general>
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
    data  = r.json()
    texto = data["content"][0]["text"].strip()
    tokens = data["usage"]["input_tokens"] + data["usage"]["output_tokens"]
    try:
        parsed = json.loads(texto)
    except json.JSONDecodeError:
        parsed = json.loads(texto.replace("```json","").replace("```","").strip())
    return parsed, tokens

# ── Guardar nota ───────────────────────────────────────────────────────────────
def guardar_nota(conn, diag: dict, nota: dict, top_industrias: list, tokens: int):
    top_tickers = ", ".join([r["ticker"] for r in top_industrias[:5]])
    sql = """
        INSERT INTO sector.sector_notas_ai (
            run_id, estado_macro, top_tickers,
            resumen, oportunidades, riesgos, nota_completa,
            score_rotacion, score_riesgo,
            tokens_usados, prompt_version
        ) VALUES (
            %(run_id)s, %(estado_macro)s, %(top_tickers)s,
            %(resumen)s, %(oportunidades)s, %(riesgos)s, %(nota_completa)s,
            %(score_rotacion)s, %(score_riesgo)s,
            %(tokens_usados)s, %(prompt_version)s
        )
    """
    params = {
        "run_id":         diag["run_id"],
        "estado_macro":   diag["estado_macro"],
        "top_tickers":    top_tickers,
        "resumen":        nota.get("resumen"),
        "oportunidades":  nota.get("oportunidades"),
        "riesgos":        nota.get("riesgos"),
        "nota_completa":  json.dumps(nota, ensure_ascii=False),
        "score_rotacion": nota.get("score_rotacion"),
        "score_riesgo":   nota.get("score_riesgo"),
        "tokens_usados":  tokens,
        "prompt_version": PROMPT_V,
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
                ("sector", "sector_notas_ai", "BULK", status, message),
            )
        conn.commit()
    except Exception as e:
        print(f"  ⚠ No se pudo registrar log: {e}")
        conn.rollback()

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    caffeinate = subprocess.Popen(["caffeinate"])

    print(f"\n{'='*65}")
    print(f"  SECTOR AI — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id : {RUN_ID}")
    print(f"{'='*65}\n")

    conn = get_conn()

    print("  Leyendo diagnóstico sectorial...")
    diag = leer_diagnostico(conn)
    if not diag:
        print("  ✗ Sin datos en v_sector_diagnostico")
        registrar_log(conn, "fail", "Sin datos en v_sector_diagnostico")
        conn.close()
        caffeinate.terminate()
        return

    print(f"  → run_id:         {diag['run_id']}")
    print(f"  → estado_macro:   {diag['estado_macro']}")
    print(f"  → señal:          {diag['señal_rotacion']}")
    print(f"  → top alineados:  {diag['top_tickers_aligned']}")

    if ya_tiene_nota(conn, diag["run_id"]):
        print(f"\n  ✓ Este run ya tiene nota AI. Nada que hacer.")
        registrar_log(conn, "skip", f"Nota ya existe para run_id={diag['run_id']}")
        conn.close()
        caffeinate.terminate()
        return

    top_industrias = leer_top_industrias(conn)
    top_sectores   = leer_top_sectores(conn)
    nota_macro     = leer_nota_macro(conn)

    print("\n  Llamando a Claude API...")
    prompt = construir_prompt(diag, top_industrias, top_sectores, nota_macro)

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
    print(f"  Estado macro:    {diag['estado_macro']}")
    print(f"  Señal rotación:  {diag['señal_rotacion']}")
    print(f"  Resumen:         {nota.get('resumen', '—')}")
    print(f"  Oportunidades:   {nota.get('oportunidades', '—')}")
    print(f"  Riesgos:         {nota.get('riesgos', '—')}")
    print(f"  Score rotación:  {nota.get('score_rotacion')} / 100")
    print(f"  Score riesgo:    {nota.get('score_riesgo')} / 100")
    print(f"  {'─'*55}\n")

    print("  Guardando en sector_notas_ai...")
    guardar_nota(conn, diag, nota, top_industrias, tokens)
    print(f"  ✓ Nota guardada (run_id: {diag['run_id']})")

    registrar_log(conn, "success",
                  f"estado={diag['estado_macro']} tokens={tokens} run_id={RUN_ID}")
    conn.close()
    caffeinate.terminate()

    print(f"\n{'='*65}")
    print(f"  Pipeline AI completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
