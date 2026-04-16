
## 📄 Uso de scripts `api_raw` en NDEV30

Este documento explica **cuándo y cómo utilizar cada tipo de script** dentro del esquema `api_raw` para mantener tu pipeline ordenado, eficiente y auditable.

---

## 🟩 1️⃣ **Script de carga masiva inicial**

**¿Cuál es?**

* Ejemplo: `cargar_datos_income_statement_anual.py`
* Scripts similares para `cargar_datos_ratios_historicos_anual`, `cargar_datos_ratios_historicos_quarter`, `cargar_datos_income_statement_quarter`.

**¿Qué hace?**
✅ Descarga **toda la historia disponible** de la API (desde 2010 en adelante).
✅ Inserta todos los registros en la tabla destino usando `ON CONFLICT` para evitar duplicados.
✅ Llena completamente tu base de datos `api_raw` para análisis y backtesting.

**¿Cuándo usarlo?**
✅ **Solo una vez, al inicio de NDEV30**, para poblar la base de datos desde cero.
✅ En caso de:

* Migración de servidor.
* Reconstrucción completa de la base.
* Verificación de integridad histórica.

**⚠️ Consideraciones:**
❌ Consume muchas llamadas a la API.
❌ Vuelve a descargar todos los registros aunque ya estén en la base.
❌ No realiza auditoría en `update_logs`.

---

## 🟩 2️⃣ **Script incremental con logging**

**¿Cuál es?**

* Ejemplo: `actualizar_income_statement_anual.py`
* Scripts similares para `actualizar_ratios_historicos_anual`, `actualizar_ratios_historicos_quarter`, `actualizar_income_statement_quarter`.

**¿Qué hace?**
✅ Consulta **solo los datos nuevos** desde la API comparando con `MAX(calendarYear)` o `MAX(date)` de tu tabla.
✅ Inserta únicamente nuevos registros, evitando llamadas innecesarias a la API.
✅ Registra auditoría en `infraestructura.update_logs` para cada ticker (`success`, `fail`).
✅ Facilita monitoreo y automatización con `cron` o `launchd`.

**¿Cuándo usarlo?**
✅ Para **actualizaciones diarias/semanales** de `api_raw`.
✅ En producción estable para mantener tu base siempre actualizada.
✅ Para auditoría y control del pipeline de ETL.

**⚠️ Consideraciones:**
✅ Requiere haber hecho previamente la carga masiva inicial.
✅ Permite identificar tickers que fallan o tienen inconsistencias en la API.

---

## ✅ Resumen visual

| Característica                 | Carga Masiva Inicial             | Script Incremental con Logging   |
| ------------------------------ | -------------------------------- | -------------------------------- |
| **Descarga**                   | Toda la historia disponible      | Solo datos nuevos                |
| **Uso**                        | 1 vez al inicio o en migraciones | Diario/semanal en producción     |
| **Auditoría en `update_logs`** | ❌ No                             | ✅ Sí                             |
| **Eficiencia**                 | Baja (consume API)               | Alta (consume solo lo necesario) |
| **Ideal para**                 | Poblado inicial                  | Mantenimiento continuo           |

---

Con esta organización, **tu infraestructura `api_raw` queda lista para ser escalable, auditada y eficiente dentro de NDEV30.**

---

**Proyecto NDEV30 – Organización de Scripts `api_raw` – 2025**

---
