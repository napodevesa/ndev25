# Pipeline ndev25

## Ejecución semanal

1. Correr el pipeline completo:
   python run/run_semanal.py

2. Verificar que todo esté ok en pgAdmin local

3. Sincronizar a Neon:
   python run/run_neon_sync.py

4. Verificar el site en Streamlit Cloud

## Frecuencia recomendada
- Pipeline: domingos a la noche
- Sync Neon: después del pipeline

## Tiempo estimado
- Pipeline completo: ~2.5 horas
- Sync Neon: ~10 minutos
