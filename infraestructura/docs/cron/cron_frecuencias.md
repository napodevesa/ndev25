### cron\_frecuencias.md

# 🗓️ Planificación de Tareas CRON para NDEV25

Este documento define la planificación de ejecución automática de scripts con `cron` para el proyecto **NDEV25**, en función del calendario de actualizaciones definido para la infraestructura de datos.

---

## 📋 Tabla de Frecuencias Cron

| **Script**                                   | **Frecuencia** | **Cuándo correr**             | **Hora sugerida** | **Motivo**                       |
| -------------------------------------------- | -------------- | ----------------------------- | ----------------- | -------------------------------- |
| `carga_datos_ratios_ttm.ipynb`               | Mensual        | 1er lunes de cada mes         | 03:00 AM          | Refrescar trailing twelve months |
| `actualizar_ratios_historicos_anual.ipynb`   | Trimestral     | 5 de enero, abril, julio, oct | 04:00 AM          | Nuevos balances anuales          |
| `actualizar_ratios_historicos_quarter.ipynb` | Mensual        | 2do lunes de cada mes         | 03:30 AM          | Earnings trimestrales            |
| `actualizar_income_statement_anual.ipynb`    | Trimestral     | 6 de enero, abril, julio, oct | 04:30 AM          | Actualización contable anual     |
| `actualizar_income_statement_quarter.ipynb`  | Mensual        | 2do martes de cada mes        | 04:00 AM          | Reportes trimestrales            |

---

## ⚙️ Comandos crontab sugeridos (formato Linux/macOS)

```cron
# Formato: minuto hora día_mes mes día_semana comando

# 1. Ratios TTM (1er lunes del mes)
0 3 * * 1 test $(date +\%d) -le 7 && papermill /Users/ndev/Desktop/ndev25/api_raw/carga_datos/python/carga_datos_ratios_ttm.ipynb /Users/ndev/cron_logs/ratios_ttm_output_$(date +\%Y\%m\%d).ipynb >> /Users/ndev/cron_logs/ratios_ttm.log 2>&1

# 2. Ratios Históricos Anuales (5 de enero, abril, julio, octubre)
0 4 5 1,4,7,10 * papermill /ruta/actualizar_ratios_historicos_anual.ipynb /Users/ndev/cron_logs/ratios_hist_anual_output_$(date +\%Y\%m\%d).ipynb >> /Users/ndev/cron_logs/ratios_hist_anual.log 2>&1

# 3. Ratios Históricos Quarter (2do lunes)
30 3 * * 1 [ $(date +\%d) -ge 8 -a $(date +\%d) -le 14 ] && papermill /ruta/actualizar_ratios_historicos_quarter.ipynb /Users/ndev/cron_logs/ratios_hist_quarter_output_$(date +\%Y\%m\%d).ipynb >> /Users/ndev/cron_logs/ratios_hist_quarter.log 2>&1

# 4. Income Statement Anual (6 de enero, abril, julio, octubre)
30 4 6 1,4,7,10 * papermill /ruta/actualizar_income_statement_anual.ipynb /Users/ndev/cron_logs/income_anual_output_$(date +\%Y\%m\%d).ipynb >> /Users/ndev/cron_logs/income_anual.log 2>&1

# 5. Income Statement Quarter (2do martes)
0 4 * * 2 [ $(date +\%d) -ge 8 -a $(date +\%d) -le 14 ] && papermill /ruta/actualizar_income_statement_quarter.ipynb /Users/ndev/cron_logs/income_quarter_output_$(date +\%Y\%m\%d).ipynb >> /Users/ndev/cron_logs/income_quarter.log 2>&1
```

---

## 📅 Notas importantes

* Se recomienda mantener un directorio `~/cron_logs/` para registrar salidas.
* Todos los scripts deben:

  * Ser ejecutables con `papermill` desde consola.
  * Leer variables de entorno desde `.env` o estar correctamente configurados.
* Se pueden testear manualmente ejecutando los comandos para verificar logs y salidas.

---

---

## 📝 Seguimiento Manual de Ejecuciones

| **Script**                  | **Fecha de ejecución** | **Ejecutado por** | **Notas**                         |
|----------------------------|------------------------|-------------------|-----------------------------------|
| `ratios_ttm`               | 2025-07-13             | ndev              | Corrido manualmente con papermill |
| `ratios_historicos_quarter`| 2025-07-20             | ndev              | Testeo inicial exitoso            |


------

✅ **Última revisión:** 2025-07-12

**Autor:** NDEV25 Infraestructura
