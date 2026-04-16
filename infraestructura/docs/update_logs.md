

## 📄 `infraestructura/docs/update_logs.md`

````markdown
# Auditoría de Procesos con `update_logs` en NDEV30

## 📌 Propósito

La tabla `infraestructura.update_logs` registra de forma automática el estado de ejecución de los **pipelines de actualización incremental de NDEV30**, permitiendo:

✅ Monitorear tickers procesados exitosamente o con errores.  
✅ Detectar fallos o interrupciones en procesos.  
✅ Mantener **trazabilidad y control histórico** sobre los procesos de carga de datos.

---

## 🗂️ Estructura de la tabla

La tabla se creó con:

```sql
CREATE TABLE infraestructura.update_logs (
    id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    ticker TEXT,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT CHECK (status IN ('success', 'fail')) NOT NULL,
    message TEXT
);
````

**Columnas:**

| Columna          | Descripción                                         |
| ---------------- | --------------------------------------------------- |
| `id`             | Identificador incremental                           |
| `schema_name`    | Esquema del proceso (`api_raw`, `procesados`, etc.) |
| `table_name`     | Tabla asociada al proceso                           |
| `ticker`         | Ticker procesado                                    |
| `execution_time` | Fecha y hora de registro (auto)                     |
| `status`         | `success` o `fail`                                  |
| `message`        | Mensaje descriptivo del resultado                   |

---

## 🚀 Uso en pipelines

Los **scripts de actualización incremental (`actualizar_*.py`)** utilizan esta tabla tras procesar cada ticker:

* Si el ticker se procesa correctamente y se insertan datos:

  * `status`: `success`
  * `message`: `"Carga completa"`
* Si no hay datos nuevos:

  * `status`: `success`
  * `message`: `"Sin datos nuevos"`
* Si ocurre un error:

  * `status`: `fail`
  * `message`: Detalle del error.

---

## 🛠️ Consultas útiles

**Ver últimos registros:**

```sql
SELECT * FROM infraestructura.update_logs ORDER BY id DESC LIMIT 20;
```

**Filtrar fallos recientes:**

```sql
SELECT * FROM infraestructura.update_logs WHERE status = 'fail' ORDER BY execution_time DESC;
```

**Ver resumen por tabla:**

```sql
SELECT table_name, status, COUNT(*) 
FROM infraestructura.update_logs 
GROUP BY table_name, status 
ORDER BY table_name, status;
```

---

## 🔍 Buenas prácticas

✅ Ejecutar los scripts de actualización incremental con logging activo tras cada carga.
✅ Revisar periódicamente los logs para detectar tickers con errores recurrentes.
✅ Incorporar visualizaciones de esta tabla en Metabase/Dash en la siguiente fase.
✅ Planificar alertas automáticas si se detectan `fail` en volúmenes elevados o tickers críticos.

---

## 🗓️ Próximos pasos

✅ Integrar el uso de `update_logs` en:

* `actualizar_ratios_historicos_anual.py`
* `actualizar_ratios_historicos_quarter.py`
* `actualizar_income_statement_anual.py`
* `actualizar_income_statement_quarter.py`

✅ Configurar ejecución automática con `cron` para auditorías automáticas diarias/semanales.
✅ Combinar el monitoreo de `update_logs` con `estado_api_raw` para control integral de infraestructura.

---

**NDEV30 – Módulo de Auditoría de Procesos con `update_logs` – 2025**

```

---
