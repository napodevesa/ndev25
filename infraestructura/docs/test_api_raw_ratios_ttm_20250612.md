Claro, aquí tienes el contenido completo del archivo `test_api_raw_ratios_ttm_20250612.md` listo para copiar y pegar en tu directorio `infraestructura/docs/`:

---

````markdown
# ✅ Test de Carga Inicial `ratios_ttm` – 2025-06-12

## Descripción

Verificaciones realizadas tras la ejecución inicial del script de carga de la tabla `api_raw.ratios_ttm`.  
Objetivo: confirmar correcto poblamiento, ausencia de duplicados y trazabilidad mediante logs.

---

## 📌 Consultas de verificación

### 1. Últimos datos cargados

```sql
SELECT * FROM api_raw.ratios_ttm 
ORDER BY fecha_de_consulta DESC 
LIMIT 20;
````

✅ *Esperado:* Registros recientes con `fecha_de_consulta = '2025-06-12'`
✅ *Validar:* Cantidad y variedad de tickers, presencia de datos reales (no nulos).

---

### 2. Confirmación en `update_logs`

```sql
SELECT * FROM infraestructura.update_logs 
WHERE table_name = 'ratios_ttm' 
ORDER BY execution_time DESC 
LIMIT 20;
```

✅ *Esperado:* Logs con `status = 'success'`
✅ *Validar:* Cada ticker procesado debería figurar, con mensaje adecuado (`Carga completa` o `Sin datos nuevos`).

---

### 3. Revisión de duplicados

```sql
SELECT ticker, fecha_de_consulta, COUNT(*)
FROM api_raw.ratios_ttm
GROUP BY ticker, fecha_de_consulta
HAVING COUNT(*) > 1;
```

✅ *Esperado:* **Cero resultados**
✅ *Significado:* No hay duplicaciones por clave primaria (`ticker`, `fecha_de_consulta`).

---

## 📁 Ubicación


```
infraestructura/docs/test_api_raw_ratios_ttm_20250612.md
```

---

## 🧩 Observaciones

* Esta prueba fue parte del proceso de poblamiento inicial del módulo `ratios_ttm` de `api_raw`.
* En el futuro, al agregar actualizaciones incrementales, repetir parte de estas validaciones con `created_at`.

---

**NDEV25 — Auditoría Inicial de ratios\_ttm — 2025-06-12**

```

---
```
