# 🗓️ Calendario de Actualizaciones `api_raw` – NDEV25

**Versión inicial:** 2025-07-11

---

## 🚀 Propósito

Este calendario define **cuándo ejecutar cada script de `api_raw`** para mantener la infraestructura actualizada de forma **ordenada, trazable y eficiente** en NDEV25.

---

## 📅 Frecuencias

| **Script**                      | **Frecuencia**  | **Cuándo correr**                   | **Motivo**                                |
|---------------------------------|-----------------|-------------------------------------|------------------------------------------|
| `ratios_ttm`                    | Mensual         | Primer semana de cada mes           | Refrescar trailing twelve months         |
| `ratios_historicos_anual`       | Trimestral      | Enero, Abril, Julio, Octubre        | Por cierres fiscales anuales             |
| `ratios_historicos_quarter`     | Mensual         | Segunda semana de cada mes          | Por reportes trimestrales escalonados    |
| `income_statement_anual`        | Trimestral      | Enero, Abril, Julio, Octubre        | Por cierres fiscales anuales             |
| `income_statement_quarter`      | Mensual         | Segunda semana de cada mes          | Para capturar earnings trimestrales      |

---

## 🛠️ Recomendaciones operativas

✅ **Revisar `update_logs` tras cada ejecución** para confirmar correctos logs de success/fail.  
✅ **Realizar backups antes de cada ciclo trimestral (enero, abril, julio, octubre).**  
✅ Usar `tickers_test_api_raw` si deseas testear antes de la ejecución completa.  
✅ Registrar en cuaderno / NDEV25 tareas cada ejecución completada.

---

## 🚩 Próximos pasos

✅ Tras confirmar consistencia de `api_raw`, avanzar a `#procesados`.  
✅ Preparar `crontab` si deseas automatización.  
✅ Mantener este archivo actualizado en caso de cambiar frecuencias.

---

**NDEV25 – Calendario de Actualizaciones `api_raw` – 2025**

