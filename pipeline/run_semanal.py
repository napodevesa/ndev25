#!/usr/bin/env python3
"""
run_semanal.py

Pipeline semanal: macro → sector → micro (técnicos) → agente → opciones.
Ejecutar después de mercados del viernes/lunes.

Ejecución: python pipeline/run_semanal.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime
from _pipeline_base import run_pipeline, print_resumen

PIPELINE  = "semanal"
RUN_ID    = datetime.now().strftime("%Y%m%d_%H%M")

STEPS = [
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


def main():
    t_inicio = datetime.now()

    print(f"\n{'='*65}")
    print(f"  PIPELINE SEMANAL — {t_inicio.strftime('%Y-%m-%d %H:%M')}")
    print(f"  run_id : {RUN_ID}")
    print(f"  steps  : {len(STEPS)}")
    print(f"{'='*65}\n")

    n_ok, n_fail = run_pipeline(PIPELINE, STEPS, RUN_ID)
    print_resumen(PIPELINE, RUN_ID, n_ok, n_fail, len(STEPS), t_inicio)


if __name__ == "__main__":
    main()
