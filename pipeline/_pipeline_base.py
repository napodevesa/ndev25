"""
_pipeline_base.py

Lógica compartida para run_semanal.py, run_mensual.py y run_anual.py.
No se ejecuta directamente.
"""

import os
import sys
import time
import subprocess
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv("/Users/ndev/Desktop/ndev25/.env")

POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5433))

BASE_DIR = "/Users/ndev/Desktop/ndev25"

# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def _log_step(conn, run_id: str, pipeline: str, step: str,
              status: str, inicio: datetime, fin: datetime, mensaje: str) -> int:
    duracion = round((fin - inicio).total_seconds(), 2)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO infraestructura.pipeline_log
                (pipeline, step, status, inicio, fin, duracion_seg, mensaje, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (pipeline, step, status, inicio, fin, duracion, mensaje, run_id),
        )
        log_id = cur.fetchone()[0]
    conn.commit()
    return log_id


# ── Ejecución de un step ──────────────────────────────────────────────────────
def run_step(conn, run_id: str, pipeline: str, script_rel: str) -> bool:
    """
    Ejecuta BASE_DIR/script_rel con el Python del entorno actual.
    Devuelve True si OK, False si falla.
    Registra en infraestructura.pipeline_log.
    """
    script_path = os.path.join(BASE_DIR, script_rel)
    step_name   = os.path.basename(script_rel)
    inicio      = datetime.now()

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        cwd=BASE_DIR,
    )

    fin    = datetime.now()
    ok     = result.returncode == 0
    status = "ok" if ok else "fail"

    if ok:
        mensaje = result.stdout[-500:] if result.stdout else "ok"
    else:
        stderr  = (result.stderr or "")[-800:]
        stdout  = (result.stdout or "")[-200:]
        mensaje = f"returncode={result.returncode}\nSTDERR: {stderr}\nSTDOUT: {stdout}"

    log_id   = _log_step(conn, run_id, pipeline, step_name,
                         status, inicio, fin, mensaje)
    duracion = round((fin - inicio).total_seconds(), 2)

    if ok:
        print(f"  [✓] {step_name:<45} {duracion:.1f}s")
    else:
        print(f"  [✗] {step_name:<45} FALLÓ — ver pipeline_log id={log_id}")

    return ok


# ── Ejecutar lista de steps ───────────────────────────────────────────────────
def run_pipeline(pipeline_name: str, steps: list[str], run_id: str) -> tuple[int, int]:
    """
    Ejecuta la lista de scripts en orden.
    Detiene al primer fallo.
    Devuelve (n_ok, n_fail).
    """
    conn  = get_conn()
    n_ok  = 0
    n_fail = 0

    for script in steps:
        ok = run_step(conn, run_id, pipeline_name, script)
        if ok:
            n_ok += 1
        else:
            n_fail += 1
            # Marcar steps restantes como skip
            skipped = steps[steps.index(script) + 1:]
            for sk in skipped:
                now = datetime.now()
                _log_step(conn, run_id, pipeline_name,
                          os.path.basename(sk), "skip", now, now,
                          f"Omitido por fallo en {os.path.basename(script)}")
                print(f"  [–] {os.path.basename(sk):<45} SKIP")
            break

    conn.close()
    return n_ok, n_fail


# ── Resumen final ─────────────────────────────────────────────────────────────
def print_resumen(pipeline_name: str, run_id: str,
                  n_ok: int, n_fail: int, n_total: int,
                  t_inicio: datetime) -> None:
    elapsed = datetime.now() - t_inicio
    mins    = int(elapsed.total_seconds()) // 60
    segs    = int(elapsed.total_seconds()) % 60

    print(f"\n{'='*65}")
    if n_fail == 0:
        print(f"  Pipeline {pipeline_name} completado en {mins:02d}:{segs:02d}")
    else:
        print(f"  Pipeline {pipeline_name} DETENIDO en {mins:02d}:{segs:02d}")
    print(f"  Steps OK  : {n_ok}/{n_total}")
    print(f"  Steps FAIL: {n_fail}")
    print(f"  Ver detalles: SELECT * FROM infraestructura.pipeline_log WHERE run_id = '{run_id}'")
    print(f"{'='*65}\n")
