import subprocess
import sys
import time
from datetime import datetime

BASE = "/Users/ndev/Desktop/ndev25"
PYTHON = sys.executable

PIPELINE = [
    ("MACRO",      "macro/macro_fred.py"),
    ("MACRO AI",   "macro/macro_ai.py"),
    ("SECTOR",     "sector/sector_precios.py"),
    ("SECTOR DIAG","sector/sector_diagnostico_tecnico.py"),
    ("SECTOR AI",  "sector/sector_ai.py"),
    ("ETF SIGNAL", "etf/etf_signal.py"),
    ("PRECIOS",    "micro/ingest/ingest_precios.py"),
    ("RATIOS TTM", "micro/ingest/ingest_ratios_ttm.py"),
    ("KEYMETRICS", "micro/ingest/ingest_keymetrics.py"),
    ("SCORES",     "micro/seleccion/ingest_scores.py"),
    ("CALC SCORES","micro/seleccion/calcular_scores.py"),
    ("FILTRO",     "micro/seleccion/aplicar_filtro.py"),
    ("ENRIQUECER", "micro/seleccion/enriquecer.py"),
    ("AGENTE",     "micro/agente/agente_decision.py"),
    ("AGENTE AI",  "micro/agente/micro_ai.py"),
    ("CONTRATOS",  "agente_opciones/ingest_contratos.py"),
    ("MOTOR OPC",  "agente_opciones/motor_opciones.py"),
    ("ESTRATEGIAS","micro/estrategias/calcular_estrategias.py"),
]

def correr_script(nombre, path):
    inicio = time.time()
    print(f"\n{'='*60}")
    print(f"  [{nombre}] {path}")
    print(f"{'='*60}")

    result = subprocess.run(
        [PYTHON, f"{BASE}/{path}"],
        cwd=BASE
    )

    elapsed = time.time() - inicio

    if result.returncode != 0:
        print(f"\n❌ FALLÓ [{nombre}] — {elapsed:.0f}s")
        return False

    print(f"\n✅ OK [{nombre}] — {elapsed:.0f}s")
    return True

def main():
    inicio_total = time.time()
    print(f"\n{'='*60}")
    print(f"  PIPELINE SEMANAL — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    completados = 0
    fallidos = []

    for nombre, path in PIPELINE:
        ok = correr_script(nombre, path)
        if ok:
            completados += 1
        else:
            fallidos.append(nombre)
            print(f"\n⚠️  Parando pipeline — falló {nombre}")
            break

    elapsed_total = time.time() - inicio_total

    print(f"\n{'='*60}")
    print(f"  RESUMEN FINAL")
    print(f"  Completados: {completados}/{len(PIPELINE)}")
    print(f"  Tiempo total: {elapsed_total/60:.1f} minutos")
    if fallidos:
        print(f"  Fallidos: {', '.join(fallidos)}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
