# Pipeline ndev25

## Ejecución semanal

1. Correr el pipeline completo:
   python run/run_semanal.py

2. Verificar que todo esté ok en pgAdmin local (ver queries abajo)

3. Sincronizar a Neon:
   python run/run_neon_sync.py

4. Verificar el site en Streamlit Cloud

## Frecuencia recomendada
- Pipeline: domingos a la noche
- Sync Neon: después del pipeline

## Tiempo estimado
- Pipeline completo: ~2.5 horas
- Sync Neon: ~10 minutos

## Verificación post-pipeline en pgAdmin

Conectate a localhost:5433 y ejecutá estas queries:

### 1 — Fechas de actualización
```sql
SELECT 'ratios_ttm' as tabla, MAX(fecha_consulta) FROM ingest.ratios_ttm
UNION ALL
SELECT 'keymetrics', MAX(fecha_consulta) FROM ingest.keymetrics
UNION ALL
SELECT 'enriquecimiento', MAX(snapshot_date) FROM seleccion.enriquecimiento
UNION ALL
SELECT 'agente_decision', MAX(snapshot_date) FROM agente.decision
UNION ALL
SELECT 'estrategias_div', MAX(snapshot_date) FROM estrategias.dividendos
UNION ALL
SELECT 'etf_signal', MAX(snapshot_date) FROM etf.signal;
```

### 2 — Conteos esperados
```sql
SELECT 'enriquecimiento' as tabla, COUNT(*) FROM seleccion.enriquecimiento
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM seleccion.enriquecimiento)
UNION ALL
SELECT 'agente_decision', COUNT(*) FROM agente.decision
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM agente.decision)
UNION ALL
SELECT 'dividendos', COUNT(*) FROM estrategias.dividendos
UNION ALL
SELECT 'buy_hold', COUNT(*) FROM estrategias.buy_hold
UNION ALL
SELECT 'cash_flow', COUNT(*) FROM estrategias.cash_flow
UNION ALL
SELECT 'the_wheel', COUNT(*) FROM estrategias.the_wheel
UNION ALL
SELECT 'crecimiento', COUNT(*) FROM estrategias.crecimiento;
```

### Valores esperados
| Tabla          | Count esperado |
|----------------|---------------|
| enriquecimiento | ~748          |
| agente_decision | ~200          |
| dividendos      | 15            |
| buy_hold        | 15            |
| cash_flow       | 15            |
| the_wheel       | 10            |
| crecimiento     | 15            |
