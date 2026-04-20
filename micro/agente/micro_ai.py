#!/usr/bin/env python3
"""
micro_ai.py

Lee agente.decision y agente.top para generar una nota cualitativa
con Claude sobre las empresas seleccionadas por el motor determinístico.

Guarda el resultado en agente.notas_ai.

Ejecución: python micro/agente/micro_ai.py
"""

import os
import json
import logging
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
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, ANTHROPIC_API_KEY]):
    raise EnvironmentError("Faltan variables de entorno. Verificar .env")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M")

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR  = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"micro_ai_{date.today().isoformat()}.log")

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
MODELO    = "claude-sonnet-4-20250514"
PROMPT_V  = "v1"
SCHEMA    = "agente"
TABLE     = "notas_ai"

# ── CREATE TABLE ───────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agente.notas_ai (
    snapshot_date       DATE            NOT NULL,
    run_id              VARCHAR(40)     NOT NULL,
    estado_macro        VARCHAR(20),
    sector_top          TEXT,
    n_activas           INT,
    n_stock             INT,
    n_csp               INT,
    resumen             TEXT,
    oportunidades_stock TEXT,
    oportunidades_csp   TEXT,
    alertas             TEXT,
    nota_completa       TEXT,
    score_conviction    SMALLINT,
    tokens_usados       INT,
    prompt_version      VARCHAR(10),
    generado_en         TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (snapshot_date)
)
"""

# ── SQL ────────────────────────────────────────────────────────────────────────
SQL_SNAPSHOT_DATE = """
    SELECT MAX(snapshot_date) FROM agente.decision
"""

SQL_MACRO = """
    SELECT estado_macro, score_riesgo, confianza
    FROM macro.macro_diagnostico
    ORDER BY calculado_en DESC LIMIT 1
"""

SQL_RESUMEN_DECISIONES = """
    SELECT
        COUNT(*)                                                          AS n_total,
        COUNT(*) FILTER (WHERE instrumento = 'stock')                    AS n_stock,
        COUNT(*) FILTER (WHERE instrumento = 'cash_secured_put')         AS n_csp,
        ROUND(AVG(target_position_size), 2)                              AS size_promedio,
        ROUND(AVG(target_position_size)
              FILTER (WHERE instrumento = 'stock'), 2)                   AS size_stock,
        ROUND(AVG(target_position_size)
              FILTER (WHERE instrumento = 'cash_secured_put'), 2)        AS size_csp,
        COUNT(*) FILTER (WHERE contexto = 'structural_quality')          AS n_sq,
        COUNT(*) FILTER (WHERE contexto = 'solid_but_expensive')         AS n_sbe,
        COUNT(*) FILTER (WHERE contexto = 'improving')                   AS n_imp,
        MODE() WITHIN GROUP (ORDER BY flag_timing)                       AS timing_dominante
    FROM agente.decision
    WHERE trade_status = 'active'
      AND snapshot_date = %(snapshot_date)s
"""

SQL_TOP = """
    SELECT t.ticker, t.instrumento, t.flag_timing,
           t.score_conviccion, t.rank_conviccion,
           t.sector_alineado, t.target_position_size,
           e.quality_score, e.value_score,
           e.altman_z_score, e.piotroski_score,
           e.rsi_14_semanal, e.precio_vs_ma200,
           e.volume_ratio_20d, e.roic_signo,
           e.roic_confiable, e.deuda_signo,
           e.momentum_3m, e.momentum_6m,
           e.sector, e.market_cap_tier
    FROM agente.top t
    JOIN seleccion.enriquecimiento e
        ON  e.ticker = t.ticker
        AND e.snapshot_date = t.snapshot_date
    WHERE t.snapshot_date = %(snapshot_date)s
    ORDER BY t.rank_conviccion
    LIMIT 10
"""

SQL_YA_TIENE_NOTA = """
    SELECT 1 FROM agente.notas_ai WHERE snapshot_date = %s LIMIT 1
"""

SQL_NOTA_SECTOR = """
    SELECT resumen, oportunidades
    FROM sector.sector_notas_ai
    ORDER BY generado_en DESC LIMIT 1
"""

INSERT_NOTA = """
    INSERT INTO agente.notas_ai (
        snapshot_date, run_id, estado_macro, sector_top,
        n_activas, n_stock, n_csp,
        resumen, oportunidades_stock, oportunidades_csp,
        alertas, nota_completa,
        score_conviction, tokens_usados, prompt_version
    ) VALUES (
        %(snapshot_date)s, %(run_id)s, %(estado_macro)s, %(sector_top)s,
        %(n_activas)s, %(n_stock)s, %(n_csp)s,
        %(resumen)s, %(oportunidades_stock)s, %(oportunidades_csp)s,
        %(alertas)s, %(nota_completa)s,
        %(score_conviction)s, %(tokens_usados)s, %(prompt_version)s
    )
    ON CONFLICT (snapshot_date) DO UPDATE SET
        run_id              = EXCLUDED.run_id,
        estado_macro        = EXCLUDED.estado_macro,
        sector_top          = EXCLUDED.sector_top,
        n_activas           = EXCLUDED.n_activas,
        n_stock             = EXCLUDED.n_stock,
        n_csp               = EXCLUDED.n_csp,
        resumen             = EXCLUDED.resumen,
        oportunidades_stock = EXCLUDED.oportunidades_stock,
        oportunidades_csp   = EXCLUDED.oportunidades_csp,
        alertas             = EXCLUDED.alertas,
        nota_completa       = EXCLUDED.nota_completa,
        score_conviction    = EXCLUDED.score_conviction,
        tokens_usados       = EXCLUDED.tokens_usados,
        prompt_version      = EXCLUDED.prompt_version,
        generado_en         = NOW()
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


def registrar_log(conn, status: str, message: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(LOG_SQL, (SCHEMA, TABLE, "BULK", status, message))
        conn.commit()
    except Exception as e:
        log.warning(f"No se pudo registrar log: {e}")
        conn.rollback()


# ── Lecturas DB ────────────────────────────────────────────────────────────────
def obtener_snapshot_date(conn) -> date | None:
    with conn.cursor() as cur:
        cur.execute(SQL_SNAPSHOT_DATE)
        row = cur.fetchone()
        return row[0] if row else None


def ya_tiene_nota(conn, snapshot_date: date) -> bool:
    with conn.cursor() as cur:
        cur.execute(SQL_YA_TIENE_NOTA, (snapshot_date,))
        return cur.fetchone() is not None


def leer_macro(conn) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_MACRO)
        row = cur.fetchone()
        return dict(row) if row else {}


def leer_resumen_decisiones(conn, snapshot_date: date) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_RESUMEN_DECISIONES, {"snapshot_date": snapshot_date})
        row = cur.fetchone()
        return dict(row) if row else {}


def leer_top(conn, snapshot_date: date) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SQL_TOP, {"snapshot_date": snapshot_date})
        return [dict(r) for r in cur.fetchall()]


def leer_nota_sector(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute(SQL_NOTA_SECTOR)
        row = cur.fetchone()
        if row:
            return f"Resumen sector: {row[0]} | Oportunidades: {row[1]}"
        return None


# ── Prompt ────────────────────────────────────────────────────────────────────
def _signo(v) -> str:
    if v is None:
        return "—"
    try:
        iv = int(v)
        return "↑" if iv == 1 else ("↓" if iv == -1 else "→")
    except (TypeError, ValueError):
        return "—"


def _fmt(v, decimals: int = 1, suffix: str = "") -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return "—"


def construir_prompt(
    macro: dict,
    resumen: dict,
    top: list[dict],
    nota_sector: str | None,
    snapshot_date: date,
) -> str:

    top_txt = "\n".join([
        f"  #{int(r['rank_conviccion']) if r.get('rank_conviccion') else '?':>2}  "
        f"{r.get('ticker','?'):<6} | {str(r.get('instrumento','')):<18} "
        f"| {str(r.get('sector',''))[:22]:<22} | {str(r.get('market_cap_tier','')):<10} "
        f"| score {_fmt(r.get('score_conviccion'),1)} "
        f"| timing: {r.get('flag_timing','—'):<25} "
        f"| alineado: {r.get('sector_alineado','—'):<8} "
        f"| quality: {_fmt(r.get('quality_score'),1):>5} "
        f"| value: {_fmt(r.get('value_score'),1):>5} "
        f"| Altman: {_fmt(r.get('altman_z_score'),2):>5} "
        f"| Piotroski: {int(r['piotroski_score']) if r.get('piotroski_score') is not None else '—':>2} "
        f"| RSI: {_fmt(r.get('rsi_14_semanal'),1):>5} "
        f"| MA200: {_fmt(r.get('precio_vs_ma200'),2,'%'):>7} "
        f"| vol: {_fmt(r.get('volume_ratio_20d'),2,'x'):>5} "
        f"| ROIC: {_signo(r.get('roic_signo'))} {'✓' if r.get('roic_confiable') else ' '} "
        f"| deuda: {_signo(r.get('deuda_signo'))} "
        f"| mom3m: {_fmt(r.get('momentum_3m'),1,'%'):>6} "
        f"| mom6m: {_fmt(r.get('momentum_6m'),1,'%'):>6} "
        f"| size: {_fmt(r.get('target_position_size'),2)}"
        for r in top
    ])

    return f"""Sos un portfolio manager cuantitativo especializado en renta variable USA.
Te paso el output del motor determinístico de selección de empresas.
Tu tarea es agregar interpretación cualitativa: explicar la tesis detrás de las selecciones,
identificar el contexto macro-sectorial que las respalda, y señalar riesgos concretos.

FECHA DE ANÁLISIS: {snapshot_date.strftime('%d/%m/%Y')}

CONTEXTO MACRO:
- Estado: {macro.get('estado_macro', '—')} | Score riesgo: {macro.get('score_riesgo', '—')} | Confianza: {macro.get('confianza', '—')}

CONTEXTO SECTORIAL:
- {nota_sector or 'Sin nota sectorial disponible'}

RESUMEN DE SEÑALES ACTIVAS:
- Total activas:        {resumen.get('n_total', 0)}
- Stock (directo):      {resumen.get('n_stock', 0)}  (size prom: {_fmt(resumen.get('size_stock'), 2)})
- Cash-secured put:     {resumen.get('n_csp', 0)}    (size prom: {_fmt(resumen.get('size_csp'), 2)})
- Timing dominante:     {resumen.get('timing_dominante', '—')}
- Contexto structural_quality:  {resumen.get('n_sq', 0)}
- Contexto solid_but_expensive: {resumen.get('n_sbe', 0)}
- Contexto improving:           {resumen.get('n_imp', 0)}

TOP {len(top)} EMPRESAS (por convicción):
{top_txt}

Columnas: rank | ticker | instrumento | sector | cap_tier | score | timing | alineado_macro | quality | value | Altman | Piotroski | RSI | vs_MA200 | vol_ratio | ROIC_tendencia | deuda_tendencia | mom_3m | mom_6m | size

Respondé ÚNICAMENTE con un JSON válido, sin texto antes ni después, sin bloques de código.
Estructura exacta:

{{
  "resumen": "2-3 oraciones sobre qué tipo de empresas seleccionó el sistema y por qué dado el contexto macro-sectorial",
  "oportunidades_stock": "las 2-3 empresas stock con mejor setup y la tesis que las une",
  "oportunidades_csp": "las 2-3 empresas CSP con mejor setup y por qué el put vendido tiene sentido aquí",
  "alertas": "2-3 alertas concretas: macro, sectorial, o de timing que podrían invalidar las señales",
  "score_conviction": <0-100: nivel de convicción general del sistema en este snapshot>
}}"""


# ── Claude API ────────────────────────────────────────────────────────────────
def llamar_claude(prompt: str) -> tuple[dict, int]:
    headers = {
        "x-api-key":         ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }
    body = {
        "model":      MODELO,
        "max_tokens": 1200,
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


# ── Guardar ───────────────────────────────────────────────────────────────────
def guardar_nota(conn, snapshot_date: date, macro: dict, resumen: dict,
                 top: list[dict], nota: dict, tokens: int) -> None:
    sector_top = ", ".join(sorted({r.get("sector", "") for r in top[:5] if r.get("sector")}))

    params = {
        "snapshot_date":      snapshot_date,
        "run_id":             RUN_ID,
        "estado_macro":       macro.get("estado_macro"),
        "sector_top":         sector_top or None,
        "n_activas":          resumen.get("n_total"),
        "n_stock":            resumen.get("n_stock"),
        "n_csp":              resumen.get("n_csp"),
        "resumen":            nota.get("resumen"),
        "oportunidades_stock": nota.get("oportunidades_stock"),
        "oportunidades_csp":  nota.get("oportunidades_csp"),
        "alertas":            nota.get("alertas"),
        "nota_completa":      json.dumps(nota, ensure_ascii=False),
        "score_conviction":   nota.get("score_conviction"),
        "tokens_usados":      tokens,
        "prompt_version":     PROMPT_V,
    }
    with conn.cursor() as cur:
        cur.execute(INSERT_NOTA, params)
    conn.commit()


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*65}")
    print(f"  MICRO AI — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id : {RUN_ID}")
    print(f"{'='*65}\n")

    conn = get_conn()
    crear_tabla(conn)
    log.info("Tabla agente.notas_ai verificada")

    # 1. Obtener snapshot_date desde agente.decision
    snapshot_date = obtener_snapshot_date(conn)
    if not snapshot_date:
        log.error("Sin datos en agente.decision. Ejecutar agente_decision.py primero.")
        conn.close()
        return

    print(f"  snapshot_date : {snapshot_date.isoformat()}")

    # 2. Verificar si ya tiene nota
    if ya_tiene_nota(conn, snapshot_date):
        print(f"  Este snapshot ya tiene nota AI. Nada que hacer.")
        log.info(f"snapshot {snapshot_date} ya tiene nota. Saliendo.")
        conn.close()
        return

    # 3. Leer datos
    print("  Leyendo datos del sistema...")
    macro   = leer_macro(conn)
    resumen = leer_resumen_decisiones(conn, snapshot_date)
    top     = leer_top(conn, snapshot_date)
    nota_sector = leer_nota_sector(conn)

    print(f"  → Estado macro   : {macro.get('estado_macro', '—')}")
    print(f"  → Señales activas: {resumen.get('n_total', 0)} "
          f"(stock={resumen.get('n_stock', 0)}, csp={resumen.get('n_csp', 0)})")
    print(f"  → Timing dominante: {resumen.get('timing_dominante', '—')}")
    print(f"  → Top empresas cargadas: {len(top)}")

    if not top:
        log.error("Sin datos en agente.top. Ejecutar agente_decision.py primero.")
        conn.close()
        return

    # 4. Llamar a Claude
    print("\n  Llamando a Claude API...")
    prompt = construir_prompt(macro, resumen, top, nota_sector, snapshot_date)

    try:
        nota, tokens = llamar_claude(prompt)
    except Exception as e:
        log.error(f"Error en Claude API: {e}")
        registrar_log(conn, "fail", str(e))
        conn.close()
        raise

    print(f"  Respuesta recibida ({tokens} tokens)")

    # 5. Mostrar en pantalla
    print(f"\n  {'─'*55}")
    print(f"  Estado macro        : {macro.get('estado_macro', '—')}")
    print(f"  Señales activas     : {resumen.get('n_total', 0)}")
    print(f"  Resumen             : {nota.get('resumen', '—')}")
    print(f"  Oportunidades stock : {nota.get('oportunidades_stock', '—')}")
    print(f"  Oportunidades CSP   : {nota.get('oportunidades_csp', '—')}")
    print(f"  Alertas             : {nota.get('alertas', '—')}")
    print(f"  Score conviction    : {nota.get('score_conviction')} / 100")
    print(f"  {'─'*55}\n")

    # 6. Guardar
    print("  Guardando en agente.notas_ai...")
    guardar_nota(conn, snapshot_date, macro, resumen, top, nota, tokens)
    registrar_log(conn, "success",
                  f"nota AI generada — {tokens} tokens — snapshot {snapshot_date}")
    print(f"  Nota guardada (snapshot: {snapshot_date})")

    conn.close()

    print(f"\n{'='*65}")
    print(f"  Pipeline AI completo — {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Log : {LOG_FILE}")
    print(f"{'='*65}\n")

    log.info(f"Fin — snapshot={snapshot_date} tokens={tokens}")


if __name__ == "__main__":
    main()
