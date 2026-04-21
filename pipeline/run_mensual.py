#!/usr/bin/env python3
"""
run_mensual.py

Pipeline mensual (post earnings): ingesta fundamentals + scores completos
+ pipeline semanal completo al final.
Ejecutar en los primeros días del mes, después de que reporting season concluya.

Ejecución: python pipeline/run_mensual.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime
from _pipeline_base import run_pipeline, print_resumen

PIPELINE = "mensual"
RUN_ID   = datetime.now().strftime("%Y%m%d_%H%M")

# Pasos exclusivos del mensual
STEPS_MENSUAL = [
    "micro/ingest/ingest_ratios_ttm.py",
    "micro/ingest/ingest_keymetrics.py",
    "micro/ingest/ingest_precios.py",
    "micro/seleccion/calcular_scores.py",
    "micro/seleccion/aplicar_filtro.py",
    "micro/seleccion/ingest_scores.py",
    "micro/seleccion/enriquecer.py",
    "micro/agente/agente_decision.py",
    "micro/agente/micro_ai.py",
    "agente_opciones/ingest_contratos.py",
    "agente_opciones/motor_opciones.py",
]

# Pasos del semanal que se agregan al final
STEPS_SEMANAL = [
    "macro/macro_fred.py",
    "macro/macro_ai.py",
    "sector/sector_precios.py",
    "sector/sector_ai.py",
    "sector/sector_diagnostico_tecnico.py",
    "micro/ingest/ingest_precios.py",
    "micro/seleccion/enriquecer.py",
    "micro/agente/agente_decision.py",
    "micro/agente/micro_ai.py",
    "agente_opciones/ingest_contratos.py",
    "agente_opciones/motor_opciones.py",
]

ALL_STEPS = STEPS_MENSUAL + STEPS_SEMANAL


def main():
    t_inicio = datetime.now()

    print(f"\n{'='*65}")
    print(f"  PIPELINE MENSUAL — {t_inicio.strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id : {RUN_ID}")
    print(f"  steps  : {len(ALL_STEPS)} ({len(STEPS_MENSUAL)} mensual + {len(STEPS_SEMANAL)} semanal)")
    print(f"{'='*65}\n")

    print(f"  ── Fase 1: Mensual ({len(STEPS_MENSUAL)} steps) ─────────────────────────────\n")
    n_ok_m, n_fail_m = run_pipeline(PIPELINE, STEPS_MENSUAL, RUN_ID)

    if n_fail_m > 0:
        print(f"\n  Pipeline mensual detenido por fallo. Semanal NO ejecutado.")
        print_resumen(PIPELINE, RUN_ID, n_ok_m, n_fail_m, len(ALL_STEPS), t_inicio)
        return

    print(f"\n  ── Fase 2: Semanal ({len(STEPS_SEMANAL)} steps) ─────────────────────────────\n")
    n_ok_s, n_fail_s = run_pipeline("semanal", STEPS_SEMANAL, RUN_ID)

    n_ok   = n_ok_m + n_ok_s
    n_fail = n_fail_m + n_fail_s
    print_resumen(PIPELINE, RUN_ID, n_ok, n_fail, len(ALL_STEPS), t_inicio)


if __name__ == "__main__":
    main()
