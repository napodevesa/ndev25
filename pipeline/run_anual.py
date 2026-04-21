#!/usr/bin/env python3
"""
run_anual.py

Pipeline anual: ingesta histórica de keymetrics + pipeline mensual completo.
Ejecutar una vez al año (ej. enero) para actualizar series históricas largas.

Ejecución: python pipeline/run_anual.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime
from _pipeline_base import run_pipeline, print_resumen

# Importar lógica del mensual para reutilizar sus listas de steps
import importlib.util

def _load_mensual():
    spec = importlib.util.spec_from_file_location(
        "run_mensual",
        os.path.join(os.path.dirname(__file__), "run_mensual.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

PIPELINE = "anual"
RUN_ID   = datetime.now().strftime("%Y%m%d_%H%M")

STEPS_ANUAL = [
    "micro/seleccion/ingest_keymetrics_hist.py",
]


def main():
    t_inicio = datetime.now()

    mensual_mod  = _load_mensual()
    STEPS_MENSUAL = mensual_mod.STEPS_MENSUAL
    STEPS_SEMANAL = mensual_mod.STEPS_SEMANAL
    ALL_STEPS = STEPS_ANUAL + STEPS_MENSUAL + STEPS_SEMANAL

    print(f"\n{'='*65}")
    print(f"  PIPELINE ANUAL — {t_inicio.strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id : {RUN_ID}")
    print(f"  steps  : {len(ALL_STEPS)} "
          f"({len(STEPS_ANUAL)} anual + {len(STEPS_MENSUAL)} mensual + {len(STEPS_SEMANAL)} semanal)")
    print(f"{'='*65}\n")

    # Fase 1: Anual
    print(f"  ── Fase 1: Anual ({len(STEPS_ANUAL)} step) ──────────────────────────────\n")
    n_ok_a, n_fail_a = run_pipeline(PIPELINE, STEPS_ANUAL, RUN_ID)

    if n_fail_a > 0:
        print(f"\n  Pipeline anual detenido. Mensual y semanal NO ejecutados.")
        print_resumen(PIPELINE, RUN_ID, n_ok_a, n_fail_a, len(ALL_STEPS), t_inicio)
        return

    # Fase 2: Mensual
    print(f"\n  ── Fase 2: Mensual ({len(STEPS_MENSUAL)} steps) ─────────────────────────────\n")
    n_ok_m, n_fail_m = run_pipeline("mensual", STEPS_MENSUAL, RUN_ID)

    if n_fail_m > 0:
        print(f"\n  Pipeline mensual detenido. Semanal NO ejecutado.")
        print_resumen(PIPELINE, RUN_ID,
                      n_ok_a + n_ok_m, n_fail_a + n_fail_m,
                      len(ALL_STEPS), t_inicio)
        return

    # Fase 3: Semanal
    print(f"\n  ── Fase 3: Semanal ({len(STEPS_SEMANAL)} steps) ─────────────────────────────\n")
    n_ok_s, n_fail_s = run_pipeline("semanal", STEPS_SEMANAL, RUN_ID)

    n_ok   = n_ok_a + n_ok_m + n_ok_s
    n_fail = n_fail_a + n_fail_m + n_fail_s
    print_resumen(PIPELINE, RUN_ID, n_ok, n_fail, len(ALL_STEPS), t_inicio)


if __name__ == "__main__":
    main()
